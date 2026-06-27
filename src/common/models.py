from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, field_validator


class Verdict(str, Enum):
    APPROVED = "APPROVED"
    FLAGGED = "FLAGGED"
    BLOCKED = "BLOCKED"


class Transaction(BaseModel):
    transaction_id: str = Field(..., min_length=1)
    account_id: str = Field(..., min_length=1)
    amount: float = Field(..., gt=0)
    merchant_id: str = Field(..., min_length=1)
    currency: str = Field(..., min_length=3, max_length=3)
    country_code: str = Field(..., min_length=2, max_length=2)
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("currency", "country_code", mode="before")
    @classmethod
    def to_upper(cls, v: str) -> str:
        return v.upper()


class RuleResult(BaseModel):
    rule_name: str
    triggered: bool
    detail: str = ""


class FraudVerdict(BaseModel):
    transaction_id: str
    account_id: str
    verdict: Verdict
    triggered_rules: list[RuleResult]
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)
