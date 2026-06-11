# StreamPulse NSE — System Architecture

## Overview

```mermaid
flowchart TB
  subgraph SOURCE[Data source]
    YF[yfinance daily OHLC<br/>50 Nifty 50 tickers, 1y<br/>committed as parquet]
    GEN[Synthetic tick generator<br/>GBM + jump-diffusion<br/>anomaly injection + ground truth]
  end

  subgraph BUS[Redpanda - Kafka API]
    K1[nse.ticks.raw]
    K2[nse.ticks.clean]
    K3[nse.bars.1m / 5m / 15m]
    K6[nse.anomalies]
    K7[nse.bars.late]
    K8[nse.bars.session]
  end

  subgraph STREAM[Flink 1.18 - PyFlink]
    F1[validate_enrich<br/>exactly-once, sector metadata]
    F2[window_bars<br/>tumbling 1m/5m/15m OHLCV]
    F3[anomaly_online<br/>Z-score + EWMA SPC]
    F4[session_bars<br/>5-min-gap session windows]
  end

  subgraph ML[Batch + online ML]
    IF[Isolation Forest<br/>nightly retrain - CronJob/CI]
    PL[predict_loop<br/>streaming scorer]
    AR[arima_forecast<br/>state-space residuals]
    MLF[MLflow tracking]
  end

  subgraph STORE[ClickHouse 24.8]
    CH[(ticks_clean RMT<br/>bars RMT<br/>anomalies MT<br/>bars_1m_ch AggMT)]
    KE[Kafka engine tables + MVs]
  end

  subgraph OBS[Observability]
    PR[Prometheus]
    GR[Grafana - 4 dashboards]
    AM[Alertmanager - Discord]
  end

  YF --> GEN
  GEN -->|record ts = event time| K1
  K1 --> F1 --> K2
  K2 --> F2 --> K3
  K2 --> F3 --> K6
  K2 --> F4 --> K8
  F2 -.late.-> K7
  K2 & K3 & K6 & K7 --> KE --> CH
  CH --> IF --> PL
  K3 --> PL --> K6
  K3 --> AR --> K6
  IF & AR -.runs.-> MLF
  CH --> GR
  PR --> GR
  PR --> AM
```

## Components

| Layer | Technology | Why (ADR) |
|---|---|---|
| Bus | Redpanda 24.2 (Kafka API) | single binary, built-in schema registry + metrics (ADR-001/002) |
| Stream compute | Flink 1.18 + PyFlink, RocksDB, exactly-once | ADR-003, deep-dive doc |
| OLAP | ClickHouse 24.8, Kafka engine + MVs | native ingestion, Replacing/Aggregating MergeTree |
| ML | scikit-learn, statsmodels, MLflow | §14 four-method ensemble |
| Observability | Prometheus + Grafana + Alertmanager | 10 alert rules → Discord |
| Packaging | Docker Compose (dev) + Helm on kind (k8s) | 7 custom charts + umbrella |
| IaC | Terraform (AWS via LocalStack, GCP demo) | docs/cloud-architecture-*.md |

## Event-time contract

Event time = **Kafka record timestamp**, stamped by the generator at produce
and propagated by every Flink sink. Watermarks are per-partition at every
source; replay-speed calibration via `--ooo-seconds` / `--idle-seconds`
submit-time knobs. Full reasoning + the five defects this design fixed:
[streaming-deep-dive.md](streaming-deep-dive.md).

## Deployment topologies

1. **Docker Compose** (`make up`) — the dev loop. 9 containers, ports on a
   dedicated 2xxxx block (ADR-006).
2. **kind + Helm** (`make k8s`) — 3-node cluster; umbrella chart wires 7
   subcharts; post-install hooks apply ClickHouse DDL and submit Flink jobs;
   nightly retrain CronJob; HPA on TaskManagers (2→8 on CPU).
3. **AWS parallel** — Kinesis/Lambda/S3/DynamoDB/Glue via LocalStack
   ([cloud-architecture-aws.md](cloud-architecture-aws.md)).
4. **GCP demo cycle** — Pub/Sub → Dataflow (Beam) → BigQuery, ~3 h, then
   destroyed ([cloud-architecture-gcp.md](cloud-architecture-gcp.md)).

## Data persistence & retention

| Table | Engine | Retention |
|---|---|---|
| nse.ticks_clean | ReplacingMergeTree(_ingested_at) | 30 days |
| nse.bars | ReplacingMergeTree(_ingested_at) | 2 years |
| nse.bars_1m_ch | AggregatingMergeTree | 2 years |
| nse.anomalies / anomalies_ml | MergeTree | 1 year |
| nse.bars_late | MergeTree | 7 days |
