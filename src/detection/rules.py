from __future__ import annotations

import time
from math import asin, cos, radians, sin, sqrt
from typing import TYPE_CHECKING

import boto3

from src.common.config import config
from src.common.logging import get_logger
from src.common.models import RuleResult, Transaction

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# In-memory blacklist cache
# ---------------------------------------------------------------------------

class _BlacklistCache:
    """
    Two-level lookup: DynamoDB is the source of truth; this cache holds a
    local copy with a TTL so each Lambda instance only queries DynamoDB once
    per TTL window rather than once per transaction.
    """

    def __init__(self) -> None:
        self._merchants: set[str] = set()
        self._accounts: set[str] = set()
        self._loaded_at: float = 0.0
        self._client: DynamoDBClient = boto3.client("dynamodb")

    def _is_stale(self) -> bool:
        return (time.monotonic() - self._loaded_at) > config.blacklist_cache_ttl

    def _refresh(self) -> None:
        logger.info("Refreshing blacklist cache from DynamoDB")
        merchants: set[str] = set()
        accounts: set[str] = set()
        paginator = self._client.get_paginator("scan")
        for page in paginator.paginate(
            TableName=config.dynamodb_table_name,
            FilterExpression="entity_type IN (:m, :a) AND blacklisted = :t",
            ExpressionAttributeValues={
                ":m": {"S": "merchant"},
                ":a": {"S": "account"},
                ":t": {"BOOL": True},
            },
            ProjectionExpression="entity_id, entity_type",
        ):
            for item in page["Items"]:
                if item["entity_type"]["S"] == "merchant":
                    merchants.add(item["entity_id"]["S"])
                else:
                    accounts.add(item["entity_id"]["S"])
        self._merchants = merchants
        self._accounts = accounts
        self._loaded_at = time.monotonic()
        logger.info(
            "Blacklist cache refreshed",
            merchant_count=len(merchants),
            account_count=len(accounts),
        )

    def is_merchant_blocked(self, merchant_id: str) -> bool:
        if self._is_stale():
            self._refresh()
        return merchant_id in self._merchants

    def is_account_blocked(self, account_id: str) -> bool:
        if self._is_stale():
            self._refresh()
        return account_id in self._accounts


blacklist_cache = _BlacklistCache()


# ---------------------------------------------------------------------------
# Individual rules
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * asin(sqrt(a))


def check_blacklist(tx: Transaction) -> RuleResult:
    if blacklist_cache.is_merchant_blocked(tx.merchant_id):
        return RuleResult(
            rule_name="blacklist_merchant",
            triggered=True,
            detail=f"Merchant {tx.merchant_id} is blacklisted",
        )
    if blacklist_cache.is_account_blocked(tx.account_id):
        return RuleResult(
            rule_name="blacklist_account",
            triggered=True,
            detail=f"Account {tx.account_id} is blacklisted",
        )
    return RuleResult(rule_name="blacklist", triggered=False)


def check_amount_limit(tx: Transaction) -> RuleResult:
    if tx.amount > config.amount_hard_limit:
        return RuleResult(
            rule_name="amount_hard_limit",
            triggered=True,
            detail=f"Amount {tx.amount} exceeds hard limit {config.amount_hard_limit}",
        )
    return RuleResult(rule_name="amount_hard_limit", triggered=False)


def check_velocity(tx: Transaction, recent_tx_timestamps: list[float]) -> RuleResult:
    """
    recent_tx_timestamps: Unix timestamps of prior transactions for this account
    within the velocity window, fetched by the caller from DynamoDB before
    invoking the rules engine.
    """
    window_start = tx.timestamp.timestamp() - config.velocity_window_seconds
    in_window = [t for t in recent_tx_timestamps if t >= window_start]
    if len(in_window) >= config.velocity_max_tx:
        return RuleResult(
            rule_name="velocity",
            triggered=True,
            detail=(
                f"{len(in_window)} transactions in {config.velocity_window_seconds}s "
                f"(limit {config.velocity_max_tx})"
            ),
        )
    return RuleResult(rule_name="velocity", triggered=False)


def check_geography(
    tx: Transaction,
    prev_country_code: str | None,
    prev_timestamp: float | None,
    country_coords: dict[str, tuple[float, float]],
) -> RuleResult:
    """
    Flags geographic impossibility: distance / time_delta > impossible_travel_kmh.
    country_coords maps ISO-3166-1 alpha-2 codes to (lat, lon) centroids.
    """
    if prev_country_code is None or prev_timestamp is None:
        return RuleResult(rule_name="geography", triggered=False)
    if prev_country_code == tx.country_code:
        return RuleResult(rule_name="geography", triggered=False)

    coords_prev = country_coords.get(prev_country_code)
    coords_curr = country_coords.get(tx.country_code)
    if not coords_prev or not coords_curr:
        return RuleResult(rule_name="geography", triggered=False)

    distance_km = _haversine_km(*coords_prev, *coords_curr)
    elapsed_hours = (tx.timestamp.timestamp() - prev_timestamp) / 3600
    if elapsed_hours <= 0:
        return RuleResult(
            rule_name="geography",
            triggered=True,
            detail="Transaction timestamp predates previous transaction",
        )

    speed_kmh = distance_km / elapsed_hours
    if speed_kmh > config.impossible_travel_kmh:
        return RuleResult(
            rule_name="geography",
            triggered=True,
            detail=(
                f"Implied travel speed {speed_kmh:.0f} km/h between "
                f"{prev_country_code} and {tx.country_code} "
                f"exceeds limit {config.impossible_travel_kmh} km/h"
            ),
        )
    return RuleResult(rule_name="geography", triggered=False)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class RulesEngine:
    def __init__(self, country_coords: dict[str, tuple[float, float]]) -> None:
        self._country_coords = country_coords

    def evaluate(
        self,
        tx: Transaction,
        recent_tx_timestamps: list[float],
        prev_country_code: str | None,
        prev_timestamp: float | None,
    ) -> list[RuleResult]:
        return [
            check_blacklist(tx),
            check_amount_limit(tx),
            check_velocity(tx, recent_tx_timestamps),
            check_geography(tx, prev_country_code, prev_timestamp, self._country_coords),
        ]
