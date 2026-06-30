from __future__ import annotations

from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from tests.conftest import REGION, make_fraud_table
from src.common.models import Transaction
from src.detection.rules import (
    RulesEngine,
    blacklist_cache,
    check_amount_limit,
    check_blacklist,
    check_geography,
    check_velocity,
)

COUNTRY_COORDS = {
    "US": (37.09, -95.71),
    "GB": (55.37, -3.43),
    "JP": (36.20, 138.25),
}

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


@pytest.fixture(autouse=True)
def freeze_cache():
    """Prevent any DynamoDB calls in unit tests; cache state is set directly."""
    blacklist_cache._merchants = set()
    blacklist_cache._accounts = set()
    blacklist_cache._loaded_at = float("inf")
    yield
    blacklist_cache._loaded_at = 0.0


@pytest.fixture
def blacklist_ddb():
    with mock_aws():
        make_fraud_table()
        ddb = boto3.client("dynamodb", region_name=REGION)
        for tx_id, entity_id, entity_type in [
            ("bl-001", "merch-evil", "merchant"),
            ("bl-002", "acc-evil", "account"),
        ]:
            ddb.put_item(
                TableName="fraud-transactions",
                Item={
                    "transaction_id": {"S": tx_id},
                    "entity_id": {"S": entity_id},
                    "entity_type": {"S": entity_type},
                    "blacklisted": {"BOOL": True},
                },
            )
        blacklist_cache._loaded_at = 0.0  # force refresh on next call
        yield
        blacklist_cache._loaded_at = 0.0


class TestCheckBlacklist:
    def test_merchant_blocked(self):
        blacklist_cache._merchants = {"merch-bad"}
        result = check_blacklist(make_tx(merchant_id="merch-bad"))
        assert result.triggered
        assert result.rule_name == "blacklist_merchant"

    def test_account_blocked(self):
        blacklist_cache._accounts = {"acc-bad"}
        result = check_blacklist(make_tx(account_id="acc-bad"))
        assert result.triggered
        assert result.rule_name == "blacklist_account"

    def test_not_blocked(self):
        assert not check_blacklist(make_tx()).triggered

    def test_merchant_checked_before_account(self):
        blacklist_cache._merchants = {"merch-bad"}
        blacklist_cache._accounts = {"acc-bad"}
        result = check_blacklist(make_tx(merchant_id="merch-bad", account_id="acc-bad"))
        assert result.rule_name == "blacklist_merchant"


class TestBlacklistCacheRefresh:
    def test_loads_blocked_merchant(self, blacklist_ddb):
        assert blacklist_cache.is_merchant_blocked("merch-evil")

    def test_loads_blocked_account(self, blacklist_ddb):
        assert blacklist_cache.is_account_blocked("acc-evil")

    def test_clean_entity_not_blocked(self, blacklist_ddb):
        assert not blacklist_cache.is_merchant_blocked("merch-clean")


class TestCheckAmountLimit:
    def test_under_limit(self):
        assert not check_amount_limit(make_tx(amount=100.0)).triggered

    def test_at_limit_not_triggered(self):
        assert not check_amount_limit(make_tx(amount=50000.0)).triggered

    def test_over_limit(self):
        result = check_amount_limit(make_tx(amount=50001.0))
        assert result.triggered
        assert result.rule_name == "amount_hard_limit"


class TestCheckVelocity:
    def _recent(self, n: int) -> list[float]:
        return [BASE_TS.timestamp() - i for i in range(n)]

    def test_under_limit(self):
        assert not check_velocity(make_tx(), self._recent(9)).triggered

    def test_at_limit_triggers(self):
        # config.velocity_max_tx == 10; len >= 10 fires the rule
        assert check_velocity(make_tx(), self._recent(10)).triggered

    def test_over_limit(self):
        result = check_velocity(make_tx(), self._recent(11))
        assert result.triggered
        assert result.rule_name == "velocity"

    def test_no_history(self):
        assert not check_velocity(make_tx(), []).triggered

    def test_outdated_timestamps_excluded(self):
        # all timestamps are 120 s before BASE_TS; window is 60 s
        old = [BASE_TS.timestamp() - 120 - i for i in range(15)]
        assert not check_velocity(make_tx(), old).triggered


class TestCheckGeography:
    def test_no_previous_tx(self):
        assert not check_geography(make_tx(), None, None, COUNTRY_COORDS).triggered

    def test_same_country(self):
        assert not check_geography(
            make_tx(), "US", BASE_TS.timestamp() - 60, COUNTRY_COORDS
        ).triggered

    def test_impossible_speed(self):
        # US → JP in 1 second
        tx = make_tx(
            country_code="JP",
            timestamp=datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
        )
        result = check_geography(tx, "US", BASE_TS.timestamp(), COUNTRY_COORDS)
        assert result.triggered
        assert result.rule_name == "geography"

    def test_possible_speed(self):
        # US → GB in 10 hours; ~6 800 km / 10 h ≈ 680 km/h < 900 limit
        tx = make_tx(
            country_code="GB",
            timestamp=datetime(2024, 1, 1, 22, 0, 0, tzinfo=timezone.utc),
        )
        assert not check_geography(tx, "US", BASE_TS.timestamp(), COUNTRY_COORDS).triggered

    def test_unknown_country_skipped(self):
        assert not check_geography(
            make_tx(country_code="ZZ"), "US", BASE_TS.timestamp(), COUNTRY_COORDS
        ).triggered

    def test_timestamp_predates_previous(self):
        # tx at 11:00, prev at 12:00 → elapsed < 0 → triggered
        tx = make_tx(
            country_code="GB",
            timestamp=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
        )
        assert check_geography(tx, "US", BASE_TS.timestamp(), COUNTRY_COORDS).triggered


class TestRulesEngine:
    def test_evaluate_returns_four_results(self):
        engine = RulesEngine(country_coords=COUNTRY_COORDS)
        results = engine.evaluate(make_tx(), [], None, None)
        assert len(results) == 4

    def test_all_clean_on_normal_tx(self):
        engine = RulesEngine(country_coords=COUNTRY_COORDS)
        results = engine.evaluate(make_tx(), [], None, None)
        assert all(not r.triggered for r in results)

    def test_amount_rule_fires(self):
        engine = RulesEngine(country_coords=COUNTRY_COORDS)
        results = engine.evaluate(make_tx(amount=99999.0), [], None, None)
        assert any(r.rule_name == "amount_hard_limit" and r.triggered for r in results)
