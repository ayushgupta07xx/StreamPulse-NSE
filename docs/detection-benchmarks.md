# Detection Benchmarks — four methods vs injected ground truth

> Generated from a controlled benchmark session (see *Protocol*). All numbers
> are measured, not estimated — per §22.5 of the project brief, unflattering
> real numbers beat impressive vague ones.

## Protocol

1. `scripts/reset_pipeline.py` gives a pristine event-time state: all Flink
   jobs cancelled (verified), topics + consumer groups deleted and recreated,
   ClickHouse tables truncated, jobs resubmitted.
2. The generator replays one historical trading day at 25× with **200
   deliberately injected anomalies** (`--seed 777`, reproducible), evenly
   sampled from four types: PRICE_SPIKE (2–7 s, ±2–5%), LEVEL_SHIFT (±1–3%
   persistent), VOLATILITY_BURST (×4–8 for 30–120 s), VOLUME_SURGE (×10–30
   for 10–60 s, price untouched). Injection timestamps are capped to the
   streamed span so recall is never diluted by unstreamed truth.
3. Online detectors (Z-score, EWMA SPC) run in Flink against the tick stream;
   batch/forecast methods (Isolation Forest, ARIMA residual) consume the 5 m
   bars the windowing job produces.
4. `tests/benchmarks/evaluate_detection.py` matches detections to truth on
   (ticker, epoch-ms window): `[start − 5 s, end + grace]`. Tick-level methods
   are graded with `grace = 90 s`; bar-level methods inherently detect at bar
   close, so the official run uses `grace = 330 s` (one 5 m bar + slack) —
   stated here because grace choice materially affects bar-method precision.

## Important structural facts

- **VOLUME_SURGE is invisible to price-only methods by design** (~50 of 200
  events). It exists precisely to demonstrate why the multivariate Isolation
  Forest earns its place in the ensemble. Price-method recall therefore has a
  ceiling of ~0.75.
- Detection latency is reported in **event time**. At 25× replay, 6 event-
  seconds ≈ 0.24 wall-seconds at live (1×) speed.
- The ensemble row counts an injected anomaly as detected when ≥2 distinct
  methods matched it.

## Results

<!-- BENCHMARK_TABLE -->

## Method notes (what tuning revealed)

- **Z-score (online, ticks):** two-tier trigger — |z| ≥ 6 fires instantly
  (sub-second catches on spikes), 4 ≤ |z| < 6 needs two consecutive ticks.
  Naive single-tick z>3 produced a false-positive flood (price levels in a
  5-minute rolling window are nearly a random walk).
- **EWMA SPC (online, ticks):** runs on **log-returns**, not prices — SPC on
  trending price levels produced 5,650 false alarms per session (measured).
  Two charts: a mean chart with Western Electric rules 1–2 (level shifts,
  large moves) and a dispersion chart (fast/slow variance ratio > 6) that
  catches symmetric volatility bursts the mean chart cannot see. WE rules 3–4
  are deliberately not alerting rules here: on EWMA'd returns they saturate on
  ordinary momentum (the detector fired at every cooldown expiry).
- **Isolation Forest (batch, 5 m bars):** 6 multivariate features
  (returns, rolling volatility, volume z-score, vwap deviation, tick-count
  z-score, intrabar pressure proxy), contamination 0.01, retrained daily
  (GitHub Actions cron / k8s CronJob), versioned via MLflow + joblib. The only
  method that can see VOLUME_SURGE.
- **ARIMA(1,1,1) residual (online, 5 m bars):** per-ticker state-space model;
  online Kalman `append` updates (no refit); anomaly = |standardized one-step
  residual| > 3 with slow-EWMA residual scale. 24-bar warm-up per ticker.
- **Cooldowns:** every online method holds a 120 s per-(ticker, method)
  cooldown. False positives consume cooldown windows and therefore *suppress
  true positives* — precision improvements translated directly into recall
  improvements during tuning.

## Failure modes found while building this benchmark (kept for honesty)

1. **Timezone mismatch nulled the matcher** — ClickHouse `DateTime64` arrives
   as naive UTC via clickhouse-connect while truth carried IST offsets; every
   detection missed every truth window by exactly 5 h 30 m. Fix: epoch-ms
   matching end to end.
2. **Replay state poisoning** — a detector restored from a checkpoint carries
   the previous session's rolling window; with event time jumping backwards,
   left-eviction can never empty it. Fix: keyed-state reset on backwards
   event-time jumps > window span.
3. **Ground-truth dilution** — duration-limited replays streamed only part of
   the session while truth covered all of it; ~60% of "missed" anomalies were
   never streamed. Fix: injection ceiling = streamed span.
