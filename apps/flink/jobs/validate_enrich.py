"""Flink job 1: validate + enrich raw ticks (exactly-once).

    nse.ticks.raw ──▶ parse ──▶ validate ──▶ enrich(sector/industry) ──▶ nse.ticks.clean

- Validation: parseable JSON, required fields, price > 0, volume ≥ 0, plausible
  timestamp, known ticker. Rejects are counted (Flink metrics) and dropped.
- Enrichment: static Nifty 50 metadata joined in a RichMapFunction loaded at
  open() — deliberately NOT broadcast state (see ADR-007: the CSV is immutable
  reference data; a broadcast stream would add complexity with zero benefit in
  PyFlink).
- Exactly-once: transactional KafkaSink + 10 s checkpoints (RocksDB). Verified
  by the Day 3 fault-injection test (tests/integration/test_exactly_once.py).

Submit:
    flink run -py /opt/streampulse/flink/jobs/validate_enrich.py \
        --pyFiles /opt/streampulse/flink/jobs -d
"""

from __future__ import annotations

import json
import sys
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import RichMapFunction, RuntimeContext

sys.path.insert(0, "/opt/streampulse/flink/jobs")

from common.pipeline import (  # noqa: E402
    dumps,
    kafka_exactly_once_sink,
    kafka_json_source,
    load_metadata,
    make_env,
)

REQUIRED_FIELDS = ("ticker", "timestamp_ist", "price", "volume", "side", "session_id", "seq")
# Synthetic data is historical replay — accept any reasonable year range
_MIN_TS = datetime(2020, 1, 1).timestamp() * 1000
_MAX_TS = datetime(2035, 1, 1).timestamp() * 1000


class ValidateEnrich(RichMapFunction):
    """str(JSON tick) → str(enriched JSON) | None for rejects."""

    def open(self, runtime_context: RuntimeContext) -> None:
        self.metadata = load_metadata()
        group = runtime_context.get_metrics_group()
        self.rejected = group.counter("ticks_rejected")
        self.accepted = group.counter("ticks_accepted")

    def map(self, raw: str):
        try:
            tick = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            self.rejected.inc()
            return None

        if any(f not in tick for f in REQUIRED_FIELDS):
            self.rejected.inc()
            return None

        meta = self.metadata.get(tick["ticker"])
        try:
            price_ok = float(tick["price"]) > 0
            volume_ok = int(tick["volume"]) >= 0
            ts_ms = datetime.fromisoformat(tick["timestamp_ist"]).timestamp() * 1000
            ts_ok = _MIN_TS < ts_ms < _MAX_TS
            side_ok = tick["side"] in ("BUY", "SELL")
        except (ValueError, TypeError):
            self.rejected.inc()
            return None

        if not (meta and price_ok and volume_ok and ts_ok and side_ok):
            self.rejected.inc()
            return None

        tick["name"] = meta["name"]
        tick["sector"] = meta["sector"]
        tick["industry"] = meta["industry"]
        tick["mcap_bucket"] = meta["mcap_bucket"]
        self.accepted.inc()
        return dumps(tick)


def build(env: StreamExecutionEnvironment) -> None:
    source = kafka_json_source("nse.ticks.raw", group_id="flink-validate-enrich")
    sink = kafka_exactly_once_sink("nse.ticks.clean", transactional_prefix="validate-enrich")

    from pyflink.common import WatermarkStrategy

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "ticks-raw")
        .map(ValidateEnrich())
        .filter(lambda x: x is not None)
        .sink_to(sink)
        .name("ticks-clean-sink")
    )


if __name__ == "__main__":
    env = make_env(parallelism=2)
    build(env)
    env.execute("validate-enrich")
