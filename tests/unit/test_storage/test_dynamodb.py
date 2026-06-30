from __future__ import annotations

from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from tests.conftest import REGION, make_fraud_table
from src.common.models import FraudVerdict, RuleResult, Transaction, Verdict
from src.storage.dynamodb import get_last_transaction, get_recent_timestamps, put_transaction_idempotent

BASE_TS = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_tx(**kwargs) -> Transaction:
    return Transaction(
        **{
            "transaction_id": "tx-001",
            "account_id": "acc-123",
            "amount": 100.0,
            "merchant_id": "merch-1",
            "currency": "USD",
            "country_code": "US",
            "timestamp": BASE_TS,
            **kwargs,
        }
    )


def make_verdict(tx: Transaction, verdict: Verdict = Verdict.APPROVED) -> FraudVerdict:
    return FraudVerdict(
        transaction_id=tx.transaction_id,
        account_id=tx.account_id,
        verdict=verdict,
        triggered_rules=[RuleResult(rule_name="amount_hard_limit", triggered=False)],
    )


@pytest.fixture
def ddb():
    with mock_aws():
        make_fraud_table()
        yield boto3.resource("dynamodb", region_name=REGION).Table("fraud-transactions")


class TestPutTransactionIdempotent:
    def test_first_write_returns_true(self, ddb):
        tx = make_tx()
        assert put_transaction_idempotent(tx, make_verdict(tx)) is True

    def test_duplicate_returns_false(self, ddb):
        tx = make_tx()
        put_transaction_idempotent(tx, make_verdict(tx))
        assert put_transaction_idempotent(tx, make_verdict(tx)) is False

    def test_different_ids_both_succeed(self, ddb):
        tx1 = make_tx(transaction_id="tx-001")
        tx2 = make_tx(transaction_id="tx-002")
        assert put_transaction_idempotent(tx1, make_verdict(tx1)) is True
        assert put_transaction_idempotent(tx2, make_verdict(tx2)) is True

    def test_item_attributes_written(self, ddb):
        tx = make_tx()
        put_transaction_idempotent(tx, make_verdict(tx))
        item = ddb.get_item(Key={"transaction_id": "tx-001"}).get("Item")
        assert item is not None
        assert item["account_id"] == "acc-123"
        assert item["verdict"] == "APPROVED"
        assert "ttl" in item


class TestGetRecentTimestamps:
    def test_no_records(self, ddb):
        assert get_recent_timestamps("acc-123", BASE_TS.timestamp() - 60) == []

    def test_record_within_window(self, ddb):
        tx = make_tx()
        put_transaction_idempotent(tx, make_verdict(tx))
        result = get_recent_timestamps("acc-123", BASE_TS.timestamp() - 60)
        assert len(result) == 1
        assert abs(result[0] - BASE_TS.timestamp()) < 1

    def test_record_outside_window_excluded(self, ddb):
        tx = make_tx()
        put_transaction_idempotent(tx, make_verdict(tx))
        # window starts 30 minutes after the tx timestamp
        result = get_recent_timestamps("acc-123", BASE_TS.timestamp() + 1800)
        assert result == []

    def test_other_account_excluded(self, ddb):
        tx = make_tx(account_id="acc-other")
        put_transaction_idempotent(tx, make_verdict(tx))
        assert get_recent_timestamps("acc-123", BASE_TS.timestamp() - 60) == []

    def test_multiple_records_within_window(self, ddb):
        for i, tx_id in enumerate(["tx-001", "tx-002", "tx-003"]):
            tx = make_tx(
                transaction_id=tx_id,
                timestamp=datetime(2024, 1, 1, 12, 0, i, tzinfo=timezone.utc),
            )
            put_transaction_idempotent(tx, make_verdict(tx))
        result = get_recent_timestamps("acc-123", BASE_TS.timestamp() - 60)
        assert len(result) == 3


class TestGetLastTransaction:
    def test_no_records_returns_none(self, ddb):
        assert get_last_transaction("acc-123") is None

    def test_returns_transaction(self, ddb):
        tx = make_tx()
        put_transaction_idempotent(tx, make_verdict(tx))
        result = get_last_transaction("acc-123")
        assert result is not None
        assert result["transaction_id"] == "tx-001"

    def test_returns_most_recent(self, ddb):
        tx1 = make_tx(
            transaction_id="tx-001",
            timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        tx2 = make_tx(
            transaction_id="tx-002",
            timestamp=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
        )
        put_transaction_idempotent(tx1, make_verdict(tx1))
        put_transaction_idempotent(tx2, make_verdict(tx2))
        result = get_last_transaction("acc-123")
        assert result["transaction_id"] == "tx-002"

    def test_other_account_not_returned(self, ddb):
        tx = make_tx(account_id="acc-other")
        put_transaction_idempotent(tx, make_verdict(tx))
        assert get_last_transaction("acc-123") is None
