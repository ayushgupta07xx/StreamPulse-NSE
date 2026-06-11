"""Shared PyFlink helpers: environment, Kafka sources/sinks, watermarks.

Design constraints (see docs/decisions.md ADR-003/ADR-007):
- These modules run ONLY inside the streampulse/flink image (Python 3.10,
  apache-flink 1.18.1). Keep dependencies stdlib-only — no pydantic here.
- Kafka sinks are value-only: PyFlink's KafkaRecordSerializationSchema builder
  cannot derive a record key from a field of the element. Downstream consumers
  re-establish per-ticker ordering via event-time watermarks.
"""

from __future__ import annotations

import json
import os
from datetime import datetime

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
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

# §13 watermark strategy
OUT_OF_ORDERNESS_S = 5
IDLENESS_S = 10


def make_env(parallelism: int = 2) -> StreamExecutionEnvironment:
    """Stream env with exactly-once checkpointing (matches FLINK_PROPERTIES)."""
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


class TickTimestampAssigner(TimestampAssigner):
    """Event time = the tick's own timestamp_ist (epoch millis)."""

    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        return ts_to_epoch_ms(value["timestamp_ist"])


def tick_watermarks() -> WatermarkStrategy:
    """Bounded out-of-orderness 5 s + idle-source detection 10 s (§13)."""
    return (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(OUT_OF_ORDERNESS_S))
        .with_idleness(Duration.of_seconds(IDLENESS_S))
        .with_timestamp_assigner(TickTimestampAssigner())
    )


def ts_to_epoch_ms(iso_ts: str) -> int:
    return int(datetime.fromisoformat(iso_ts).timestamp() * 1000)


def epoch_ms_to_iso(ms: int) -> str:
    from datetime import timezone

    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def load_metadata() -> dict[str, dict]:
    """ticker → {name, sector, industry, mcap_bucket} from the committed CSV."""
    import csv

    path = os.path.join(DATA_DIR, "nifty50_metadata.csv")
    with open(path, newline="", encoding="utf-8") as fh:
        return {row["ticker"]: row for row in csv.DictReader(fh)}


def dumps(obj: dict) -> str:
    return json.dumps(obj, separators=(",", ":"))
