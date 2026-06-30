from __future__ import annotations

import os

# Must be set before any src.* imports — Config reads from os.environ at class definition time.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("KINESIS_STREAM_NAME", "fraud-transactions")
os.environ.setdefault("DYNAMODB_TABLE_NAME", "fraud-transactions")
os.environ.setdefault("SNS_ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:fraud-alerts")
os.environ.setdefault("DLQ_URL", "https://sqs.us-east-1.amazonaws.com/123456789012/fraud-processor-dlq")

import boto3

REGION = "us-east-1"
TABLE_NAME = "fraud-transactions"
STREAM_NAME = "fraud-transactions"


def make_fraud_table() -> None:
    boto3.client("dynamodb", region_name=REGION).create_table(
        TableName=TABLE_NAME,
        AttributeDefinitions=[
            {"AttributeName": "transaction_id", "AttributeType": "S"},
            {"AttributeName": "account_id", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "transaction_id", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "account_id-timestamp-index",
                "KeySchema": [
                    {"AttributeName": "account_id", "KeyType": "HASH"},
                    {"AttributeName": "timestamp", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
