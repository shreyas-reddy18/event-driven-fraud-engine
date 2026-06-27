from __future__ import annotations

import json

import boto3

from src.common.config import config
from src.common.logging import get_logger
from src.common.models import FraudVerdict

logger = get_logger(__name__)

_sns = boto3.client("sns")


def publish_fraud_alert(verdict: FraudVerdict) -> None:
    message = json.dumps(verdict.model_dump(mode="json"), default=str)
    _sns.publish(
        TopicArn=config.sns_alert_topic_arn,
        Message=message,
        Subject=f"Fraud Alert: {verdict.verdict.value} — {verdict.transaction_id}",
        MessageAttributes={
            "verdict": {"DataType": "String", "StringValue": verdict.verdict.value},
            "account_id": {"DataType": "String", "StringValue": verdict.account_id},
        },
    )
    logger.info(
        "Fraud alert published",
        transaction_id=verdict.transaction_id,
        verdict=verdict.verdict.value,
    )
