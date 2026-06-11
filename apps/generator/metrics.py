"""Prometheus metrics for the generator (scraped at :8000/metrics)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, start_http_server

MESSAGES_PRODUCED = Counter(
    "generator_messages_produced_total",
    "Ticks successfully delivered to Kafka",
    ["ticker"],
)
ANOMALIES_INJECTED = Counter(
    "generator_anomalies_injected_total",
    "Ground-truth anomalies injected into the synthetic stream",
    ["anomaly_type"],
)
DELIVERY_ERRORS = Counter(
    "generator_delivery_errors_total",
    "Kafka delivery failures reported by the producer",
)
TARGET_RATE = Gauge(
    "generator_target_ticks_per_second",
    "Configured target emission rate",
)
EMITTED_SESSION_SECOND = Gauge(
    "generator_session_second",
    "Last simulated session second emitted (0..22499)",
)


def serve(port: int) -> None:
    start_http_server(port)
