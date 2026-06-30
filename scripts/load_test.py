#!/usr/bin/env python3
"""
Load testing script for the Fraud Detection Pipeline.

Exercises all four transaction profiles against the API Gateway endpoint:

  Profile A  — Valid transactions              → expected APPROVED
  Profile B  — Amount exceeds hard limit       → expected BLOCKED
  Profile C  — Velocity burst (15 tx / 2s)    → expected BLOCKED (later records)
  Profile D  — Geographic anomaly (US → IN)   → expected BLOCKED (second record)

Detection is asynchronous (Kinesis → Lambda); the HTTP layer always returns
202 Accepted for well-formed payloads. "Expected BLOCKED" counts in the report
reflect what the rules engine will decide once records are consumed — they are
not derived from HTTP responses.

Usage:
    python scripts/load_test.py --url <api-url> [--count 100] [--concurrency 20]
    python scripts/load_test.py --url <api-url> --dry-run
"""

from __future__ import annotations

import argparse
import json
import random
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from statistics import mean
from typing import Any


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------


@dataclass
class RequestResult:
    profile: str
    transaction_id: str
    status_code: int
    latency_ms: float
    error: str | None = None

    @property
    def accepted(self) -> bool:
        return self.status_code == 202

    @property
    def client_error(self) -> bool:
        return 400 <= self.status_code < 500

    @property
    def server_error(self) -> bool:
        return self.status_code >= 500

    @property
    def network_error(self) -> bool:
        return self.status_code == 0


# ---------------------------------------------------------------------------
# HTTP transport
# ---------------------------------------------------------------------------


def _post(url: str, payload: dict[str, Any], profile: str) -> RequestResult:
    tx_id = payload["transaction_id"]
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            latency_ms = (time.perf_counter() - t0) * 1000
            return RequestResult(
                profile=profile,
                transaction_id=tx_id,
                status_code=resp.status,
                latency_ms=latency_ms,
            )
    except urllib.error.HTTPError as exc:
        latency_ms = (time.perf_counter() - t0) * 1000
        return RequestResult(
            profile=profile,
            transaction_id=tx_id,
            status_code=exc.code,
            latency_ms=latency_ms,
            error=exc.reason,
        )
    except Exception as exc:
        latency_ms = (time.perf_counter() - t0) * 1000
        return RequestResult(
            profile=profile,
            transaction_id=tx_id,
            status_code=0,
            latency_ms=latency_ms,
            error=str(exc),
        )


# ---------------------------------------------------------------------------
# Payload generators
# ---------------------------------------------------------------------------

_MERCHANTS = ["merch_retail_001", "merch_gas_042", "merch_grocery_107", "merch_online_288"]
_COUNTRIES = ["US", "GB", "DE", "FR", "AU"]
_CURRENCIES = ["USD", "EUR", "GBP", "AUD"]


def _txn_id() -> str:
    return f"txn_{uuid.uuid4().hex[:16]}"


def _acct_id(suffix: str = "") -> str:
    return f"acct_{uuid.uuid4().hex[:12]}{suffix}"


def gen_profile_a(n: int) -> list[tuple[dict[str, Any], str]]:
    """Standard transactions that should return APPROVED."""
    return [
        (
            {
                "transaction_id": _txn_id(),
                "account_id": _acct_id(),
                "amount": round(random.uniform(1.0, 4999.0), 2),
                "merchant_id": random.choice(_MERCHANTS),
                "currency": random.choice(_CURRENCIES),
                "country_code": random.choice(_COUNTRIES),
            },
            "A_VALID",
        )
        for _ in range(n)
    ]


def gen_profile_b() -> tuple[dict[str, Any], str]:
    """Single transaction with amount well above the 50,000 hard limit → BLOCKED."""
    return (
        {
            "transaction_id": _txn_id(),
            "account_id": _acct_id(),
            "amount": 99_999.99,
            "merchant_id": random.choice(_MERCHANTS),
            "currency": "USD",
            "country_code": "US",
        },
        "B_AMOUNT",
    )


def gen_profile_c() -> list[tuple[dict[str, Any], str]]:
    """
    15 transactions from the same account_id in rapid succession.
    The velocity rule fires once the count within the window exceeds
    VELOCITY_MAX_TX (default 10), so records 11-15 will be BLOCKED.
    All 15 are submitted together so the thread pool fires them within
    the same 2-second burst window.
    """
    shared_account = _acct_id("_velocity")
    return [
        (
            {
                "transaction_id": _txn_id(),
                "account_id": shared_account,
                "amount": round(random.uniform(10.0, 300.0), 2),
                "merchant_id": random.choice(_MERCHANTS),
                "currency": "USD",
                "country_code": "US",
            },
            "C_VELOCITY",
        )
        for _ in range(15)
    ]


def gen_profile_d() -> tuple[tuple[dict[str, Any], str], tuple[dict[str, Any], str]]:
    """
    Two consecutive transactions for the same account_id:
      First  → US  (anchor — APPROVED)
      Second → IN  (~13,000 km away within seconds → BLOCKED by geography rule)
    Sent sequentially so the first record reaches Kinesis before the second,
    preserving the causal ordering the geography rule depends on.
    """
    shared_account = _acct_id("_geo")
    first = (
        {
            "transaction_id": _txn_id(),
            "account_id": shared_account,
            "amount": round(random.uniform(100.0, 800.0), 2),
            "merchant_id": random.choice(_MERCHANTS),
            "currency": "USD",
            "country_code": "US",
        },
        "D_GEO_ANCHOR",
    )
    second = (
        {
            "transaction_id": _txn_id(),
            "account_id": shared_account,
            "amount": round(random.uniform(100.0, 800.0), 2),
            "merchant_id": random.choice(_MERCHANTS),
            "currency": "INR",
            "country_code": "IN",
        },
        "D_GEO_FLAGGED",
    )
    return first, second


# ---------------------------------------------------------------------------
# Metrics helpers
# ---------------------------------------------------------------------------


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = max(0, int(len(s) * 0.95) - 1)
    return s[idx]


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------

_W = 56


def _bar(label: str, value: int, total: int, width: int = 20) -> str:
    filled = int((value / total) * width) if total else 0
    bar = "█" * filled + "░" * (width - filled)
    pct = f"{value / total * 100:.1f}%" if total else "0.0%"
    return f"  {label:<22} {bar}  {value:>5} ({pct})"


def print_report(results: list[RequestResult], elapsed: float) -> None:
    total = len(results)
    if total == 0:
        print("No results to report.")
        return

    accepted = sum(1 for r in results if r.accepted)
    client_errors = sum(1 for r in results if r.client_error)
    server_errors = sum(1 for r in results if r.server_error)
    net_errors = sum(1 for r in results if r.network_error)

    by_profile: dict[str, int] = {}
    for r in results:
        by_profile[r.profile] = by_profile.get(r.profile, 0) + 1

    # Profiles that the rules engine will block (inferred, not from HTTP)
    expected_blocked = (
        by_profile.get("B_AMOUNT", 0)
        + by_profile.get("C_VELOCITY", 0)  # all 15 queued; engine blocks 11-15
        + by_profile.get("D_GEO_FLAGGED", 0)
    )

    latencies = [r.latency_ms for r in results]

    sep = "─" * _W
    print(f"\n{sep}")
    print(f"  FRAUD PIPELINE  ·  LOAD TEST REPORT")
    print(sep)
    print(f"  Wall time          {elapsed:.2f}s")
    print(f"  Throughput         {total / elapsed:.1f} req/s")
    print(f"  Total requests     {total}")
    print(sep)
    print("  HTTP STATUS")
    print(_bar("202 Accepted", accepted, total))
    print(_bar("4xx Client error", client_errors, total))
    print(_bar("5xx Server error", server_errors, total))
    print(_bar("Network / timeout", net_errors, total))
    print(sep)
    print("  PROFILE BREAKDOWN")
    for profile in sorted(by_profile):
        tag = ""
        if profile == "A_VALID":
            tag = " (→ APPROVED)"
        elif profile == "B_AMOUNT":
            tag = " (→ BLOCKED)"
        elif profile == "C_VELOCITY":
            tag = " (→ BLOCKED >10th)"
        elif profile == "D_GEO_ANCHOR":
            tag = " (→ APPROVED)"
        elif profile == "D_GEO_FLAGGED":
            tag = " (→ BLOCKED)"
        print(f"  {profile:<22} {by_profile[profile]:>5} requests{tag}")
    print(sep)
    print("  RULE ENGINE  (async — inferred from profiles sent)")
    print(f"  Expected BLOCKED   {expected_blocked:>5} transactions")
    print(f"    Profile B — amount limit      {by_profile.get('B_AMOUNT', 0):>3}")
    print(f"    Profile C — velocity burst    {by_profile.get('C_VELOCITY', 0):>3}")
    print(f"    Profile D — geo anomaly       {by_profile.get('D_GEO_FLAGGED', 0):>3}")
    print(sep)
    print("  LATENCY  (round-trip, ms)")
    print(f"  Min     {min(latencies):>10.1f} ms")
    print(f"  Avg     {mean(latencies):>10.1f} ms")
    print(f"  p95     {_p95(latencies):>10.1f} ms")
    print(f"  Max     {max(latencies):>10.1f} ms")
    if net_errors:
        errors = [r for r in results if r.error]
        print(sep)
        print("  ERRORS (first 5)")
        for r in errors[:5]:
            print(f"  [{r.profile}] {r.transaction_id}: {r.error}")
    print(f"{sep}\n")


# ---------------------------------------------------------------------------
# Dry-run printer
# ---------------------------------------------------------------------------


def dry_run(url: str, count: int, concurrency: int) -> None:
    a_tasks = gen_profile_a(count)
    b_task = gen_profile_b()
    c_tasks = gen_profile_c()
    d_first, d_second = gen_profile_d()
    total = len(a_tasks) + 1 + len(c_tasks) + 2

    print(f"\n{'─' * _W}")
    print("  DRY-RUN — no HTTP requests will be sent")
    print(f"{'─' * _W}")
    print(f"  Target URL       {url}")
    print(f"  Concurrency      {concurrency} threads")
    print(f"  Total requests   {total}")
    print(f"    Profile A      {len(a_tasks)}  (valid — parallel in thread pool)")
    print(f"    Profile B      1   (amount limit — parallel in thread pool)")
    print(f"    Profile C      {len(c_tasks)}  (velocity burst — dedicated burst phase)")
    print(f"    Profile D      2   (geo anomaly — sequential pair)")
    print(f"{'─' * _W}")
    print("\n  Sample payloads:")
    print("\n  [A_VALID]")
    print(json.dumps(a_tasks[0][0], indent=4))
    print("\n  [B_AMOUNT]")
    print(json.dumps(b_task[0], indent=4))
    print("\n  [C_VELOCITY]  (first of 15 — all share same account_id)")
    print(json.dumps(c_tasks[0][0], indent=4))
    print("\n  [D_GEO_ANCHOR]  (first — US)")
    print(json.dumps(d_first[0], indent=4))
    print("\n  [D_GEO_FLAGGED]  (second — IN, same account_id)")
    print(json.dumps(d_second[0], indent=4))
    print(f"\n{'─' * _W}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load test the Fraud Detection Pipeline API Gateway endpoint.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--url",
        required=True,
        help="Full API Gateway transactions endpoint URL",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of Profile A (valid) transactions",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Thread pool size for parallel requests",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print sample payloads and exit without sending requests",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.dry_run:
        dry_run(args.url, args.count, args.concurrency)
        return

    results: list[RequestResult] = []

    def submit_and_collect(
        tasks: list[tuple[dict[str, Any], str]],
        label: str,
    ) -> None:
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            futures = {
                pool.submit(_post, args.url, payload, profile): (payload, profile)
                for payload, profile in tasks
            }
            for future in as_completed(futures):
                r = future.result()
                results.append(r)
                status_icon = "✓" if r.accepted else "✗"
                print(
                    f"  {status_icon} [{r.profile:<18}] "
                    f"{r.transaction_id}  HTTP {r.status_code}  {r.latency_ms:.0f}ms",
                    flush=True,
                )

    t_start = time.perf_counter()

    # Phase 1: Velocity burst — dedicated pool so all 15 fire simultaneously
    print(f"\n── Phase 1: Profile C — velocity burst (15 tx, same account) ──")
    c_tasks = gen_profile_c()
    submit_and_collect(c_tasks, "C_VELOCITY")

    # Phase 2: Profile A (bulk valid) + Profile B (amount limit) — parallel
    a_tasks = gen_profile_a(args.count)
    b_task = gen_profile_b()
    print(f"\n── Phase 2: Profile A ({args.count} valid) + Profile B (1 over-limit) ──")
    submit_and_collect(a_tasks + [b_task], "A+B")

    # Phase 3: Profile D — ordered sequential pair (anchor must precede flagged)
    print(f"\n── Phase 3: Profile D — geographic anomaly (US → IN, sequential) ──")
    d_first, d_second = gen_profile_d()
    r1 = _post(args.url, d_first[0], d_first[1])
    results.append(r1)
    print(f"  ✓ [D_GEO_ANCHOR   ] {r1.transaction_id}  HTTP {r1.status_code}  {r1.latency_ms:.0f}ms")
    r2 = _post(args.url, d_second[0], d_second[1])
    results.append(r2)
    print(f"  ✓ [D_GEO_FLAGGED  ] {r2.transaction_id}  HTTP {r2.status_code}  {r2.latency_ms:.0f}ms")

    elapsed = time.perf_counter() - t_start
    print_report(results, elapsed)


if __name__ == "__main__":
    main()
