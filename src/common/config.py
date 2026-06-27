from __future__ import annotations

import os


class Config:
    kinesis_stream_name: str = os.environ["KINESIS_STREAM_NAME"]
    dynamodb_table_name: str = os.environ["DYNAMODB_TABLE_NAME"]
    sns_alert_topic_arn: str = os.environ["SNS_ALERT_TOPIC_ARN"]
    dlq_url: str = os.environ["DLQ_URL"]

    # Velocity rule: max transactions per account within the window
    velocity_max_tx: int = int(os.getenv("VELOCITY_MAX_TX", "10"))
    velocity_window_seconds: int = int(os.getenv("VELOCITY_WINDOW_SECONDS", "60"))

    # Geography rule: impossible travel threshold in km/h
    impossible_travel_kmh: float = float(os.getenv("IMPOSSIBLE_TRAVEL_KMH", "900"))

    # Amount rule: single-transaction hard limit
    amount_hard_limit: float = float(os.getenv("AMOUNT_HARD_LIMIT", "50000"))

    # Blacklist cache TTL in seconds
    blacklist_cache_ttl: int = int(os.getenv("BLACKLIST_CACHE_TTL", "300"))


config = Config()
