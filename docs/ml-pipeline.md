# ML Pipeline: Batch + Online Anomaly Detection

Four detectors contribute complementary signal to one anomaly stream
(`nse.anomalies`); ensemble severity = number of methods agreeing on a
(ticker, 30 s bucket). Methods 1–2 run inside Flink (see
`apps/flink/jobs/anomaly_online.py`); this doc covers the ML half.

## Method 3 — Isolation Forest (batch retrain, streaming score)

**Features** per (ticker, 5m bar) — `apps/ml/feature_builder.py`:

| Feature | Definition | Window |
|---|---|---|
| log_return | ln(close/prev close) | 1 bar |
| return_volatility | std of log returns | 6 bars (30 m) |
| volume_zscore | volume vs trailing mean/std | 36 bars (3 h) |
| vwap_deviation | (close − vwap)/vwap | 1 bar |
| tick_count_zscore | tick count vs trailing mean/std | 36 bars |
| pressure_proxy | (close − open)/(high − low) | 1 bar |

**Retrain** (`apps/ml/isolation_forest_retrain.py`): last 7 days of bars from
ClickHouse → `StandardScaler + IsolationForest(n_estimators=200,
contamination=0.01)` → holdout = most recent session day → metrics + params +
model artifact logged to **MLflow**; model saved as
`models/isolation_forest_latest.joblib` plus a run-id-stamped copy and a JSON
manifest (version, run id, trained_at, feature list).

**Model registry approach:** filesystem + MLflow run registry. The "latest"
joblib is the deploy pointer; every historical version remains addressable by
run id. Promotion = overwrite the latest pointer (atomic file replace);
rollback = re-point to any previous version file. For this project's scale a
full MLflow Model Registry server adds operational weight without adding
information — the run log already captures lineage. (Documented trade-off.)

**Scheduled retraining — two implementations, both real:**
1. **Kubernetes CronJob** (`helm/.../ml-retrain-cronjob.yaml`, 01:30 IST):
   retrains where the data lives, writes to the models PVC. The
   production-shaped path.
2. **GitHub Actions nightly** (`.github/workflows/scheduled-retrain.yml`):
   ephemeral ClickHouse service container, bars seeded from committed
   historical parquet through the same GBM engine
   (`scripts/seed_clickhouse.py`), retrain, model uploaded as a CI artifact.
   Proves the training pipeline end-to-end on neutral infrastructure nightly.

**Streaming scorer** (`apps/ml/predict_loop.py`): consumes `nse.bars.5m`
(read_committed), maintains per-ticker trailing windows (bootstrapped from
ClickHouse), scores every bar, writes the full audit trail to
`nse.anomalies_ml` (score, flag, feature vector JSON, model version) and
publishes flagged bars to `nse.anomalies` as `isolation_forest`.

## Method 4 — ARIMA forecast residuals

`apps/ml/arima_forecast.py`: per ticker, ARIMA(1,1,1) fit once on a 50-bar
warm-up, then **state-space `append()`** per new bar — Kalman-filter update,
no refit. Anomaly when |standardized 1-step-ahead residual| > 3; residual
scale adapts via slow EWMA. Catches *trend-relative* anomalies the
level-based methods miss; needs warm-up and is sensitive to misspecification
(documented weakness, §14).

## Drift monitoring

The **ML Performance** Grafana dashboard tracks: flag rate vs the 1%
contamination baseline (15 m buckets), anomaly-score quantiles over time
(p1/p50/p99 — downward p50 drift ⇒ feature shift ⇒ retrain), per-model-version
scored/flagged counts, and the latest flagged bars with their feature vectors
for case-by-case explanation.

## Benchmarking against ground truth

The generator records every injected anomaly. `tests/benchmarks/
evaluate_detection.py` joins detections against truth (ticker match + time
window + grace for bar-close latency) and emits per-method precision / recall
/ F1 / median detection latency plus the ensemble row — published in
`docs/detection-benchmarks.md` (Day 10).
