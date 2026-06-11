"""Alternate generator sink: Google Cloud Pub/Sub (Day 12 demo cycle).

Mirrors kafka_sink.TickSink with the Pub/Sub publisher API — ordering key =
ticker (requires an ordering-enabled topic for strict per-key order; the demo
topic skips it since Dataflow re-orders by event time anyway).
"""

from __future__ import annotations

import logging
import os

from generator import metrics
from generator.schemas import Tick

log = logging.getLogger(__name__)


class PubSubSink:
    """Async-batched Pub/Sub publisher."""

    def __init__(self, topic: str = "nse-ticks") -> None:
        from google.cloud import pubsub_v1  # lazy: google deps only on Day 12

        project = os.environ["GCP_PROJECT"]
        self._publisher = pubsub_v1.PublisherClient(
            batch_settings=pubsub_v1.types.BatchSettings(
                max_messages=500, max_latency=0.05
            )
        )
        self._topic_path = self._publisher.topic_path(project, topic)
        self._pending = []

    def send(self, tick: Tick) -> None:
        future = self._publisher.publish(
            self._topic_path,
            tick.to_json_bytes(),
            ticker=tick.ticker,
        )
        future.add_done_callback(
            lambda f: metrics.MESSAGES_PRODUCED.labels(ticker=tick.ticker).inc()
            if not f.exception()
            else metrics.DELIVERY_ERRORS.inc()
        )
        self._pending.append(future)
        if len(self._pending) > 2000:
            self.flush()

    def flush(self) -> None:
        for f in self._pending:
            try:
                f.result(timeout=30)
            except Exception:  # noqa: BLE001
                metrics.DELIVERY_ERRORS.inc()
        self._pending.clear()
