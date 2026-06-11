"""Kafka producer wrapper (idempotent, batched) + topic bootstrap."""

from __future__ import annotations

import logging

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from generator import metrics
from generator.schemas import Tick

log = logging.getLogger(__name__)

# §13 topic design (local profile: replication factor 1)
TOPIC_SPECS = {
    "nse.ticks.raw": {"partitions": 12, "retention_ms": 24 * 3600 * 1000},
    "nse.ticks.clean": {"partitions": 12, "retention_ms": 24 * 3600 * 1000},
    "nse.bars.1m": {"partitions": 6, "retention_ms": 7 * 24 * 3600 * 1000},
    "nse.bars.5m": {"partitions": 6, "retention_ms": 30 * 24 * 3600 * 1000},
    "nse.bars.15m": {"partitions": 3, "retention_ms": 90 * 24 * 3600 * 1000},
    "nse.anomalies": {"partitions": 3, "retention_ms": 30 * 24 * 3600 * 1000},
    "nse.bars.late": {"partitions": 1, "retention_ms": 7 * 24 * 3600 * 1000},
    # extension to §13: session-window summaries (5-min gap), see ADR-008
    "nse.bars.session": {"partitions": 3, "retention_ms": 7 * 24 * 3600 * 1000},
}


def ensure_topics(bootstrap: str) -> None:
    """Create all pipeline topics if missing (idempotent)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = [
        NewTopic(
            name,
            num_partitions=spec["partitions"],
            replication_factor=1,
            config={"retention.ms": str(spec["retention_ms"])},
        )
        for name, spec in TOPIC_SPECS.items()
        if name not in existing
    ]
    if not wanted:
        return
    for name, future in admin.create_topics(wanted).items():
        try:
            future.result(timeout=15)
            log.info("created topic %s", name)
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


class TickSink:
    """Batched, idempotent producer keyed by ticker.

    Values are JSON by default; pass `serializer` (Tick → bytes) to switch the
    wire format — e.g. proto_format.ProtobufTickSerializer (brief §20 Phase 2).
    """

    def __init__(self, bootstrap: str, topic: str = "nse.ticks.raw", serializer=None) -> None:
        self.topic = topic
        self._serialize = serializer or Tick.to_json_bytes
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 25,
                "batch.num.messages": 5_000,
                "compression.type": "lz4",
            }
        )

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            metrics.DELIVERY_ERRORS.inc()
            log.error("delivery failed for key=%s: %s", msg.key(), err)
        else:
            metrics.MESSAGES_PRODUCED.labels(ticker=msg.key().decode()).inc()

    def send(self, tick: Tick) -> None:
        # Kafka record timestamp = event time. Downstream Flink jobs watermark
        # off record timestamps natively (pure-Java path, no Python assigner).
        event_ms = int(tick.timestamp_ist.timestamp() * 1000)
        while True:
            try:
                self._producer.produce(
                    self.topic,
                    key=tick.ticker.encode(),
                    value=self._serialize(tick),
                    timestamp=event_ms,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                # local queue full — let the producer drain, then retry
                self._producer.poll(0.05)
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush(30)
