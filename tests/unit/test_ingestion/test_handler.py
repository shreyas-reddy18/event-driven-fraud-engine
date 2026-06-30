from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_aws

from tests.conftest import REGION, STREAM_NAME
from src.ingestion.handler import handler

VALID_TX = {
    "transaction_id": "tx-001",
    "account_id": "acc-123",
    "amount": 99.99,
    "merchant_id": "merch-1",
    "currency": "USD",
    "country_code": "US",
}


def api_event(body) -> dict:
    return {"body": json.dumps(body) if isinstance(body, dict) else body}


@pytest.fixture
def kinesis():
    with mock_aws():
        client = boto3.client("kinesis", region_name=REGION)
        client.create_stream(StreamName=STREAM_NAME, ShardCount=1)
        yield client


def _kinesis_records(client) -> list[dict]:
    shard_id = client.describe_stream(StreamName=STREAM_NAME)["StreamDescription"]["Shards"][0][
        "ShardId"
    ]
    it = client.get_shard_iterator(
        StreamName=STREAM_NAME, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]
    return client.get_records(ShardIterator=it)["Records"]


class TestIngestionHandler:
    def test_valid_transaction_returns_202(self, kinesis):
        assert handler(api_event(VALID_TX), None)["statusCode"] == 202

    def test_valid_transaction_publishes_to_kinesis(self, kinesis):
        handler(api_event(VALID_TX), None)
        assert len(_kinesis_records(kinesis)) == 1

    def test_response_body_contains_transaction_id(self, kinesis):
        body = json.loads(handler(api_event(VALID_TX), None)["body"])
        assert body["transaction_id"] == "tx-001"
        assert body["status"] == "accepted"

    def test_invalid_json_returns_400(self, kinesis):
        assert handler({"body": "not-json{{"}, None)["statusCode"] == 400

    def test_missing_required_field_returns_400(self, kinesis):
        tx = {k: v for k, v in VALID_TX.items() if k != "account_id"}
        assert handler(api_event(tx), None)["statusCode"] == 400

    def test_none_body_returns_400(self, kinesis):
        assert handler({"body": None}, None)["statusCode"] == 400

    def test_zero_amount_returns_400(self, kinesis):
        assert handler(api_event({**VALID_TX, "amount": 0}), None)["statusCode"] == 400

    def test_negative_amount_returns_400(self, kinesis):
        assert handler(api_event({**VALID_TX, "amount": -10.0}), None)["statusCode"] == 400

    def test_invalid_json_does_not_publish_to_kinesis(self, kinesis):
        handler({"body": "bad"}, None)
        assert len(_kinesis_records(kinesis)) == 0
