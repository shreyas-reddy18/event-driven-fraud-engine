from __future__ import annotations

import base64
import json
from typing import Any

import boto3
from pydantic import ValidationError

from src.alerting.sns import publish_fraud_alert
from src.common.config import config
from src.common.logging import get_logger
from src.common.models import FraudVerdict, Transaction, Verdict
from src.detection.rules import RulesEngine
from src.storage.dynamodb import get_last_transaction, get_recent_timestamps, put_transaction_idempotent

logger = get_logger(__name__)

_sqs = boto3.client("sqs")

# Country centroids (lat, lon) for geographic impossibility checks.
# Extend as needed; missing codes are skipped gracefully by the rules engine.
_COUNTRY_COORDS: dict[str, tuple[float, float]] = {
    "US": (37.09, -95.71),
    "GB": (55.37, -3.43),
    "DE": (51.16, 10.45),
    "FR": (46.22, 2.21),
    "JP": (36.20, 138.25),
    "CN": (35.86, 104.19),
    "AU": (-25.27, 133.77),
    "BR": (-14.23, -51.92),
    "IN": (20.59, 78.96),
    "ZA": (-30.55, 22.93),
}

_engine = RulesEngine(country_coords=_COUNTRY_COORDS)


def _send_to_dlq(raw_record: dict[str, Any], error: str) -> None:
    _sqs.send_message(
        QueueUrl=config.dlq_url,
        MessageBody=json.dumps({"record": raw_record, "error": error}),
    )
    logger.error("Record sent to DLQ", error=error)


def _process_record(record: dict[str, Any]) -> None:
    raw_data = base64.b64decode(record["kinesis"]["data"]).decode()

    try:
        tx = Transaction(**json.loads(raw_data))
    except (json.JSONDecodeError, ValidationError) as exc:
        # Malformed payload — not retryable; park in DLQ immediately.
        _send_to_dlq(record, str(exc))
        return

    # --- Idempotency guard ---------------------------------------------------
    # Kinesis guarantees at-least-once delivery; the conditional DynamoDB write
    # ensures we never score or alert on the same transaction_id twice.
    # -------------------------------------------------------------------------
    window_start = tx.timestamp.timestamp() - config.velocity_window_seconds
    recent_timestamps = get_recent_timestamps(tx.account_id, window_start)
    last_tx = get_last_transaction(tx.account_id)
    prev_country = last_tx["country_code"] if last_tx else None
    prev_ts = (
        __import__("datetime").datetime.fromisoformat(last_tx["timestamp"]).timestamp()
        if last_tx
        else None
    )

    rule_results = _engine.evaluate(
        tx=tx,
        recent_tx_timestamps=recent_timestamps,
        prev_country_code=prev_country,
        prev_timestamp=prev_ts,
    )

    triggered = [r for r in rule_results if r.triggered]
    if triggered:
        verdict_value = Verdict.BLOCKED
    else:
        verdict_value = Verdict.APPROVED

    verdict = FraudVerdict(
        transaction_id=tx.transaction_id,
        account_id=tx.account_id,
        verdict=verdict_value,
        triggered_rules=rule_results,
    )

    written = put_transaction_idempotent(tx, verdict)
    if not written:
        # Duplicate — already processed; skip alerting.
        return

    logger.info(
        "Transaction processed",
        transaction_id=tx.transaction_id,
        verdict=verdict_value.value,
        triggered_rules=[r.rule_name for r in triggered],
    )

    if verdict_value == Verdict.BLOCKED:
        publish_fraud_alert(verdict)


def handler(event: dict[str, Any], context: Any) -> None:
    failed_ids: list[str] = []

    for record in event.get("Records", []):
        try:
            _process_record(record)
        except Exception as exc:
            seq = record["kinesis"]["sequenceNumber"]
            logger.error(
                "Unhandled error processing record",
                sequence_number=seq,
                error=str(exc),
            )
            # Report the failure back to Lambda so it retries only the failed
            # shard position; successfully processed records are not re-read.
            failed_ids.append({"itemIdentifier": seq})

    if failed_ids:
        # Partial batch response — Lambda retries only the reported failures.
        return {"batchItemFailures": failed_ids}  # type: ignore[return-value]
