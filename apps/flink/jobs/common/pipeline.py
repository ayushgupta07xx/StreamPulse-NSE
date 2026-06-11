"""Shared PyFlink helpers: environment, Kafka sources/sinks, watermarks.

Design constraints (see docs/decisions.md ADR-003/ADR-007):
- These modules run ONLY inside the streampulse/flink image (Python 3.10,
  apache-flink 1.18.1). Keep dependencies stdlib-only — no pydantic here.
- Kafka sinks are value-only: PyFlink's KafkaRecordSerializationSchema builder
  cannot derive a record key from a field of the element. Downstream consumers
  re-establish per-ticker ordering via event-time watermarks.
"""

from __future__ import annotations

import contextlib
import json
import os

# NOTE: datetime.UTC is 3.11+; this module runs on the Flink image's 3.10
from datetime import datetime, timezone

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
DATA_DIR = os.environ.get("STREAMPULSE_DATA_DIR", "/opt/streampulse/data")

# §13 watermark strategy.
# IMPORTANT (replay semantics): the bound is event-time seconds. Replaying at
# N× multiplies effective disorder — 1 wall-second of partition-consumer skew
# becomes N event-seconds — so fast replays must widen the bound (pass
# --ooo-seconds ≈ 5×speed at submit time). 5 s is the live (1×) calibration.
OUT_OF_ORDERNESS_S = int(os.environ.get("WATERMARK_OOO_SECONDS", "5"))
IDLENESS_S = 10


def _argv_int(flag: str, default: int) -> int:
    """Read an integer job argument at graph-build time (client side)."""
    import sys

    argv = sys.argv
    if flag in argv:
        try:
            return int(argv[argv.index(flag) + 1])
        except (IndexError, ValueError):
            pass
    return default


def ooo_from_argv(default_s: int = OUT_OF_ORDERNESS_S) -> int:
    return _argv_int("--ooo-seconds", default_s)


def idle_from_argv(default_s: int = IDLENESS_S) -> int:
    """--idle-seconds N (0 disables idleness).

    Idleness is WALL-clock: a split with no records for N seconds is excluded
    from the min-watermark. Right for live 1× (thinly-traded stocks must not
    stall the pipeline) but wrong during accelerated replay/backfill, where
    fetch-scheduling gaps >N wall-seconds are routine and falsely idle splits
    — the watermark then leaps ahead and the lagging split's records land
    late. Pass --idle-seconds 0 for replay verification runs.
    """
    return _argv_int("--idle-seconds", default_s)


def make_env(parallelism: int = 2) -> StreamExecutionEnvironment:
    """Stream env with exactly-once checkpointing (matches FLINK_PROPERTIES).

    --parallelism N (job argv) overrides — lets all three jobs co-exist on a
    small slot budget for full-ensemble benchmark runs.
    """
    import sys

    argv = sys.argv
    if "--parallelism" in argv:
        with contextlib.suppress(IndexError, ValueError):
            parallelism = int(argv[argv.index("--parallelism") + 1])
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    env.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_timeout(120_000)
    return env


def kafka_json_source(topic: str, group_id: str) -> KafkaSource:
    """String-valued Kafka source reading committed transactional data only."""
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
        )
        .set_property("isolation.level", "read_committed")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def kafka_exactly_once_sink(topic: str, transactional_prefix: str) -> KafkaSink:
    """Transactional Kafka sink participating in Flink checkpoints (2PC)."""
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .set_transactional_id_prefix(transactional_prefix)
        # must exceed checkpoint interval; Redpanda default max is 15 min
        .set_property("transaction.timeout.ms", "600000")
        .build()
    )


def record_ts_watermarks(
    ooo_seconds: int | None = None, idle_seconds: int | None = None
) -> WatermarkStrategy:
    """Source-level watermarks from KAFKA RECORD TIMESTAMPS (§13 strategy).

    Three properties make this the production pattern:
    1. The generator stamps record timestamp = event time at produce, and
       Flink's KafkaSink propagates element timestamps to produced records —
       so event time survives every hop without touching payload JSON.
    2. Watermarking at the source keeps per-split (per-partition) watermark
       tracking: the operator watermark is min across partitions, absorbing
       any cross-partition consumption skew (critical at fast replay speeds).
    3. No Python timestamp assigner: the watermark path stays pure-Java, so
       records don't cross the JVM↔Python bridge before the pipeline starts.

    idle_seconds=0 disables idleness (replay/backfill mode — see idle_from_argv).
    """
    bound = OUT_OF_ORDERNESS_S if ooo_seconds is None else ooo_seconds
    idle = IDLENESS_S if idle_seconds is None else idle_seconds
    strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(bound))
    if idle > 0:
        strategy = strategy.with_idleness(Duration.of_seconds(idle))
    return strategy


def ts_to_epoch_ms(iso_ts: str) -> int:
    return int(datetime.fromisoformat(iso_ts).timestamp() * 1000)


def epoch_ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()  # noqa: UP017


def load_metadata() -> dict[str, dict]:
    """ticker → {name, sector, industry, mcap_bucket} from the committed CSV."""
    import csv

    path = os.path.join(DATA_DIR, "nifty50_metadata.csv")
    with open(path, newline="", encoding="utf-8") as fh:
        return {row["ticker"]: row for row in csv.DictReader(fh)}


def dumps(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"))
