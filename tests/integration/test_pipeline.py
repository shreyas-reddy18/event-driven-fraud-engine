from __future__ import annotations

import base64
import json

import boto3
import pytest
from boto3.dynamodb.conditions import Key
from moto import mock_aws

from tests.conftest import REGION, STREAM_NAME, TABLE_NAME, make_fraud_table
from src.ingestion.handler import handler as ingest
from src.processor.handler import handler as process

SNS_TOPIC_NAME = "fraud-alerts"
DLQ_NAME = "fraud-processor-dlq"
ALERT_QUEUE_NAME = "test-fraud-alerts-subscriber"


@pytest.fixture
def pipeline():
    with mock_aws():
        make_fraud_table()

        boto3.client("kinesis", region_name=REGION).create_stream(
            StreamName=STREAM_NAME, ShardCount=1
        )

        sns = boto3.client("sns", region_name=REGION)
        topic_arn = sns.create_topic(Name=SNS_TOPIC_NAME)["TopicArn"]

        sqs = boto3.client("sqs", region_name=REGION)
        sqs.create_queue(QueueName=DLQ_NAME)
        alert_queue_url = sqs.create_queue(QueueName=ALERT_QUEUE_NAME)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(
            QueueUrl=alert_queue_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        yield {
            "kinesis": boto3.client("kinesis", region_name=REGION),
            "ddb": boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME),
            "sqs": sqs,
            "alert_queue_url": alert_queue_url,
        }


def api_event(tx: dict) -> dict:
    return {"body": json.dumps(tx)}


def drain_kinesis(kinesis_client) -> list[dict]:
    shard_id = kinesis_client.describe_stream(StreamName=STREAM_NAME)["StreamDescription"][
        "Shards"
    ][0]["ShardId"]
    it = kinesis_client.get_shard_iterator(
        StreamName=STREAM_NAME, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]
    return kinesis_client.get_records(ShardIterator=it)["Records"]


def kinesis_trigger_event(records: list[dict]) -> dict:
    return {
        "Records": [
            {
                "kinesis": {
                    "data": base64.b64encode(r["Data"]).decode(),
                    "sequenceNumber": r["SequenceNumber"],
                    "partitionKey": "acc-test",
                    "approximateArrivalTimestamp": 1649946482.0,
                },
                "eventSource": "aws:kinesis",
                "awsRegion": REGION,
                "eventSourceARN": f"arn:aws:kinesis:{REGION}:123456789012:stream/{STREAM_NAME}",
            }
            for r in records
        ]
    }


class TestPipeline:
    def test_approved_transaction_written_to_dynamodb(self, pipeline):
        tx = {
            "transaction_id": "tx-approved-001",
            "account_id": "acc-pipeline",
            "amount": 100.0,
            "merchant_id": "merch-1",
            "currency": "USD",
            "country_code": "US",
        }
        assert ingest(api_event(tx), None)["statusCode"] == 202

        records = drain_kinesis(pipeline["kinesis"])
        assert len(records) == 1

        process(kinesis_trigger_event(records), None)

        item = pipeline["ddb"].get_item(Key={"transaction_id": "tx-approved-001"}).get("Item")
        assert item is not None
        assert item["verdict"] == "APPROVED"
        assert item["account_id"] == "acc-pipeline"

    def test_blocked_transaction_triggers_sns_alert(self, pipeline):
        tx = {
            "transaction_id": "tx-blocked-001",
            "account_id": "acc-blocked",
            "amount": 99999.0,  # exceeds AMOUNT_HARD_LIMIT=50000
            "merchant_id": "merch-2",
            "currency": "USD",
            "country_code": "US",
        }
        ingest(api_event(tx), None)
        process(kinesis_trigger_event(drain_kinesis(pipeline["kinesis"])), None)

        item = pipeline["ddb"].get_item(Key={"transaction_id": "tx-blocked-001"}).get("Item")
        assert item["verdict"] == "BLOCKED"

        messages = pipeline["sqs"].receive_message(
            QueueUrl=pipeline["alert_queue_url"], MaxNumberOfMessages=1
        ).get("Messages", [])
        assert len(messages) == 1
        alert = json.loads(json.loads(messages[0]["Body"])["Message"])
        assert alert["verdict"] == "BLOCKED"
        assert alert["transaction_id"] == "tx-blocked-001"

    def test_duplicate_transaction_processed_once(self, pipeline):
        tx = {
            "transaction_id": "tx-dup-001",
            "account_id": "acc-dup",
            "amount": 50.0,
            "merchant_id": "merch-3",
            "currency": "USD",
            "country_code": "US",
        }
        ingest(api_event(tx), None)
        records = drain_kinesis(pipeline["kinesis"])
        event = kinesis_trigger_event(records)

        process(event, None)
        process(event, None)  # duplicate — idempotency guard should skip

        response = pipeline["ddb"].query(
            IndexName="account_id-timestamp-index",
            KeyConditionExpression=Key("account_id").eq("acc-dup"),
        )
        assert response["Count"] == 1

    def test_ingestion_serialization_roundtrip(self, pipeline):
        tx = {
            "transaction_id": "tx-serial-001",
            "account_id": "acc-serial",
            "amount": 250.0,
            "merchant_id": "merch-serial",
            "currency": "GBP",
            "country_code": "GB",
        }
        ingest(api_event(tx), None)
        process(kinesis_trigger_event(drain_kinesis(pipeline["kinesis"])), None)

        item = pipeline["ddb"].get_item(Key={"transaction_id": "tx-serial-001"}).get("Item")
        assert item["currency"] == "GBP"
        assert item["country_code"] == "GB"
