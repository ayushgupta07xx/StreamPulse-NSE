"""Flink job 1: validate + enrich raw ticks (exactly-once).

    nse.ticks.raw ──▶ parse ──▶ validate ──▶ enrich(sector/industry) ──▶ nse.ticks.clean

- Validation: parseable JSON, required fields, price > 0, volume ≥ 0, plausible
  timestamp, known ticker. Rejects are counted (Flink metrics) and dropped.
- Enrichment: static Nifty 50 metadata joined in a MapFunction loaded at
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

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

# PyFlink has no Rich* variants — FlatMapFunction itself exposes open()/close()
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext

sys.path.insert(0, "/opt/streampulse/flink/jobs")

from common.pipeline import (  # noqa: E402
    dumps,
    kafka_exactly_once_sink,
    kafka_json_source,
    load_metadata,
    make_env,
    ooo_from_argv,
    record_ts_watermarks,
)

REQUIRED_FIELDS = ("ticker", "timestamp_ist", "price", "volume", "side", "session_id", "seq")
# Synthetic data is historical replay — accept any reasonable year range
_MIN_TS = datetime(2020, 1, 1).timestamp() * 1000
_MAX_TS = datetime(2035, 1, 1).timestamp() * 1000


class ValidateEnrich(FlatMapFunction):
    """str(JSON tick) → str(enriched JSON); rejects emit nothing."""

    def open(self, runtime_context: RuntimeContext) -> None:
        self.metadata = load_metadata()
        group = runtime_context.get_metrics_group()
        self.rejected = group.counter("ticks_rejected")
        self.accepted = group.counter("ticks_accepted")

    def flat_map(self, raw: str):
        try:
            tick = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            self.rejected.inc()
            return

        if any(f not in tick for f in REQUIRED_FIELDS):
            self.rejected.inc()
            return

        meta = self.metadata.get(tick["ticker"])
        try:
            price_ok = float(tick["price"]) > 0
            volume_ok = int(tick["volume"]) >= 0
            ts_ms = datetime.fromisoformat(tick["timestamp_ist"]).timestamp() * 1000
            ts_ok = _MIN_TS < ts_ms < _MAX_TS
            side_ok = tick["side"] in ("BUY", "SELL")
        except (ValueError, TypeError):
            self.rejected.inc()
            return

        if not (meta and price_ok and volume_ok and ts_ok and side_ok):
            self.rejected.inc()
            return

        tick["name"] = meta["name"]
        tick["sector"] = meta["sector"]
        tick["industry"] = meta["industry"]
        tick["mcap_bucket"] = meta["mcap_bucket"]
        self.accepted.inc()
        yield dumps(tick)


def build(env: StreamExecutionEnvironment) -> None:
    source = kafka_json_source("nse.ticks.raw", group_id="flink-validate-enrich")
    sink = kafka_exactly_once_sink("nse.ticks.clean", transactional_prefix="validate-enrich")

    (
        # record-timestamp watermarks: element timestamps flow through the
        # chain and the KafkaSink stamps them onto nse.ticks.clean records,
        # so downstream jobs inherit event time without parsing payloads
        env.from_source(source, record_ts_watermarks(ooo_from_argv()), "ticks-raw")
        # output_type is load-bearing: the Java Kafka sink needs Java Strings,
        # not pickled Python bytes (ADR-007)
        .flat_map(ValidateEnrich(), output_type=Types.STRING())
        .sink_to(sink)
        .name("ticks-clean-sink")
        # SINGLE producer: two parallel sink subtasks interleave into the same
        # (unkeyed) partitions with mutual event-time skew, creating
        # within-partition regressions that break downstream per-split
        # watermarks. One producer keeps every partition monotonic. At real
        # scale this becomes a keyed sink instead (ADR-007 / deep-dive doc).
        .set_parallelism(1)
    )


if __name__ == "__main__":
    env = make_env(parallelism=2)
    build(env)
    env.execute("validate-enrich")
