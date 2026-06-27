from __future__ import annotations

from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from src.common.config import config
from src.common.logging import get_logger
from src.common.models import FraudVerdict, Transaction, Verdict

logger = get_logger(__name__)

_dynamodb = boto3.resource("dynamodb")
_table = _dynamodb.Table(config.dynamodb_table_name)

# 90-day TTL
_TTL_SECONDS = 90 * 24 * 3600


def put_transaction(tx: Transaction, verdict: FraudVerdict) -> None:
    ttl = int(tx.timestamp.timestamp()) + _TTL_SECONDS
    _table.put_item(
        Item={
            "transaction_id": tx.transaction_id,
            "account_id": tx.account_id,
            "amount": str(tx.amount),
            "merchant_id": tx.merchant_id,
            "currency": tx.currency,
            "country_code": tx.country_code,
            "timestamp": tx.timestamp.isoformat(),
            "verdict": verdict.verdict.value,
            "triggered_rules": [r.model_dump() for r in verdict.triggered_rules],
            "evaluated_at": verdict.evaluated_at.isoformat(),
            "ttl": ttl,
        }
    )


def put_transaction_idempotent(tx: Transaction, verdict: FraudVerdict) -> bool:
    """
    Writes the transaction only if transaction_id does not already exist.
    Returns True on success, False if the item already existed (duplicate).
    Uses a conditional expression so the check and write are atomic.
    """
    ttl = int(tx.timestamp.timestamp()) + _TTL_SECONDS
    try:
        _table.put_item(
            Item={
                "transaction_id": tx.transaction_id,
                "account_id": tx.account_id,
                "amount": str(tx.amount),
                "merchant_id": tx.merchant_id,
                "currency": tx.currency,
                "country_code": tx.country_code,
                "timestamp": tx.timestamp.isoformat(),
                "verdict": verdict.verdict.value,
                "triggered_rules": [r.model_dump() for r in verdict.triggered_rules],
                "evaluated_at": verdict.evaluated_at.isoformat(),
                "ttl": ttl,
            },
            ConditionExpression="attribute_not_exists(transaction_id)",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning(
                "Duplicate transaction skipped",
                transaction_id=tx.transaction_id,
            )
            return False
        raise


def get_recent_timestamps(account_id: str, since_epoch: float) -> list[float]:
    """Returns Unix timestamps of transactions for account_id after since_epoch."""
    response = _table.query(
        IndexName="account_id-timestamp-index",
        KeyConditionExpression=(
            Key("account_id").eq(account_id)
            & Key("timestamp").gte(
                datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()
            )
        ),
        ProjectionExpression="#ts",
        ExpressionAttributeNames={"#ts": "timestamp"},
    )
    return [
        datetime.fromisoformat(item["timestamp"]).timestamp()
        for item in response.get("Items", [])
    ]


def get_last_transaction(account_id: str) -> dict | None:
    """Returns the most recent transaction record for the account, or None."""
    response = _table.query(
        IndexName="account_id-timestamp-index",
        KeyConditionExpression=Key("account_id").eq(account_id),
        ScanIndexForward=False,
        Limit=1,
    )
    items = response.get("Items", [])
    return items[0] if items else None
