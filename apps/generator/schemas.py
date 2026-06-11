"""Message schemas (Phase 1: JSON via Pydantic; Phase 2 adds Protobuf)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, NonNegativeInt, PositiveFloat


class Tick(BaseModel):
    """One synthetic trade tick on topic ``nse.ticks.raw``."""

    ticker: str = Field(min_length=1, max_length=20)
    timestamp_ist: datetime
    price: PositiveFloat
    volume: NonNegativeInt
    side: Literal["BUY", "SELL"]
    exchange: Literal["NSE"] = "NSE"
    session_id: str
    # Per-ticker monotonic sequence number. Lets downstream jobs and the Day 3
    # fault-injection test prove exactly-once (no gaps, no duplicates).
    seq: NonNegativeInt

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class AnomalyRecord(BaseModel):
    """Ground-truth record of one injected anomaly (for benchmarking)."""

    anomaly_id: str
    ticker: str
    anomaly_type: Literal["PRICE_SPIKE", "LEVEL_SHIFT", "VOLATILITY_BURST", "VOLUME_SURGE"]
    start_ts: datetime
    end_ts: datetime
    magnitude: float
    description: str


class GroundTruth(BaseModel):
    """Top-level document written to data/anomaly_ground_truth.json."""

    session_id: str
    seed: int
    trading_date: str
    generated_at: datetime
    anomalies: list[AnomalyRecord]
