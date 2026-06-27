from __future__ import annotations

import base64
import json
from typing import Any

import boto3
from pydantic import ValidationError

from src.common.config import config
from src.common.logging import get_logger
from src.common.models import Transaction

logger = get_logger(__name__)

_kinesis = boto3.client("kinesis")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    try:
        body = json.loads(event.get("body") or "{}")
        tx = Transaction(**body)
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.error("Invalid transaction payload", error=str(exc))
        return {"statusCode": 400, "body": json.dumps({"error": str(exc)})}

    _kinesis.put_record(
        StreamName=config.kinesis_stream_name,
        Data=tx.model_dump_json().encode(),
        PartitionKey=tx.account_id,
    )

    logger.info("Transaction published to Kinesis", transaction_id=tx.transaction_id)
    return {
        "statusCode": 202,
        "body": json.dumps({"transaction_id": tx.transaction_id, "status": "accepted"}),
    }
