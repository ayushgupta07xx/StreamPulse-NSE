# StreamPulse NSE

> Real-Time Anomaly Detection on Indian Equity Markets — Kafka (Redpanda) · Apache Flink (PyFlink) · ClickHouse · Grafana · Kubernetes · Multi-cloud (AWS via LocalStack, GCP Pub/Sub + Dataflow)

**Status: under active construction.** This README is a skeleton; the full write-up
(architecture, screenshots, demo video, quickstart) lands at v0.1.0.

## What this is

A synthetic tick generator calibrated on a year of real Nifty 50 historical data
emits up to 5,000 ticks/second into Kafka. PyFlink jobs validate, enrich, window
into 1m/5m/15m OHLCV bars, and run online anomaly detection (rolling Z-score,
EWMA statistical process control). A daily batch job retrains an Isolation Forest;
ARIMA forecast residuals catch a third class of anomalies. Everything lands in
ClickHouse via its native Kafka table engine and renders in Grafana with
sub-second refresh.

## Quickstart (local)

```bash
make up      # docker compose up: Redpanda, Flink, ClickHouse, Prometheus, Grafana, MLflow
make smoke   # produce + consume a test Kafka message
make down    # teardown
```

| UI | URL |
|---|---|
| Grafana | http://localhost:23000 (admin/admin) |
| Flink Dashboard | http://localhost:28088 |
| Redpanda Console | http://localhost:8085 |
| Prometheus | http://localhost:9090 |
| Alertmanager | http://localhost:9093 |
| MLflow | http://localhost:5000 |
| ClickHouse HTTP | http://localhost:8123 |
| Kafka (host clients) | localhost:29092 |

## Data & legal

All market data is **synthetic** — see [LEGAL.md](LEGAL.md).

## License

Apache 2.0 — see [LICENSE](LICENSE).
