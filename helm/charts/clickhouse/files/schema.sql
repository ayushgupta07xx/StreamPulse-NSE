-- StreamPulse NSE — storage tables (Day 5)
-- Apply: clickhouse-client --multiquery < clickhouse/schema.sql
-- Kafka-engine ingestion tables and MVs live in materialized_views.sql.

CREATE DATABASE IF NOT EXISTS nse;

-- ─────────────────────────────── Ticks ───────────────────────────────
-- ReplacingMergeTree: the Kafka engine is at-least-once; replaying a batch
-- after a crash produces exact-duplicate rows that collapse on merge because
-- (ticker, timestamp_ist, seq) is identical. _ingested_at is the version
-- column, so the newest copy wins deterministically.
CREATE TABLE IF NOT EXISTS nse.ticks_clean
(
    ticker        LowCardinality(String),
    timestamp_ist DateTime64(3, 'Asia/Kolkata'),
    price         Float64 CODEC(Gorilla, ZSTD(1)),
    volume        UInt64,
    side          Enum8('BUY' = 1, 'SELL' = 2),
    exchange      LowCardinality(String) DEFAULT 'NSE',
    session_id    String,
    seq           UInt64,
    name          String,
    sector        LowCardinality(String),
    industry      LowCardinality(String),
    mcap_bucket   LowCardinality(String),
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY toDate(timestamp_ist)
ORDER BY (ticker, timestamp_ist, seq);

-- ─────────────────────────────── Bars ───────────────────────────────
-- Flink emits a final bar per (ticker, window); allowed-lateness re-fires
-- replace the earlier row (version = _ingested_at).
CREATE TABLE IF NOT EXISTS nse.bars
(
    ticker       LowCardinality(String),
    bar_size     LowCardinality(String),          -- '1m' | '5m' | '15m'
    window_start DateTime64(3, 'Asia/Kolkata'),
    window_end   DateTime64(3, 'Asia/Kolkata'),
    open         Float64,
    high         Float64,
    low           Float64,
    close        Float64,
    volume       UInt64,
    vwap         Float64,
    tick_count   UInt32,
    _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
PARTITION BY (bar_size, toYYYYMM(window_start))
ORDER BY (bar_size, ticker, window_start);

-- Late events beyond Flink's 30 s allowed lateness (inspection table)
CREATE TABLE IF NOT EXISTS nse.bars_late
(
    raw          String,
    _ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY _ingested_at;

-- ─────────────────────────────── Anomalies ───────────────────────────────
CREATE TABLE IF NOT EXISTS nse.anomalies
(
    ticker           LowCardinality(String),
    ts               DateTime64(3, 'Asia/Kolkata'),
    detection_method LowCardinality(String),   -- zscore | ewma_spc | isolation_forest | arima_residual
    score            Float64,
    severity         UInt8,                    -- ensemble: # methods firing
    context          String,                   -- JSON blob with method-specific detail
    session_id       String,
    _ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (ts, ticker);

-- Batch ML predictions (Day 9 predict_loop writes here directly)
CREATE TABLE IF NOT EXISTS nse.anomalies_ml
(
    ticker        LowCardinality(String),
    window_start  DateTime64(3, 'Asia/Kolkata'),
    model_version String,
    anomaly_score Float64,                     -- IsolationForest decision_function (lower = more anomalous)
    is_anomaly    UInt8,
    features      String,                      -- JSON feature vector for explainability
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(window_start)
ORDER BY (window_start, ticker);

-- ───────────────── ClickHouse-native pre-aggregation (AggregatingMergeTree) ─────────────────
-- Independent of Flink: aggregates raw ticks into 1m bars at the storage layer.
-- Serves as a cross-check of the Flink windows and demonstrates AggregateFunction
-- state columns. Query with -Merge combinators (see vw_bars_1m_ch below).
CREATE TABLE IF NOT EXISTS nse.bars_1m_ch
(
    ticker       LowCardinality(String),
    window_start DateTime('Asia/Kolkata'),
    open_state   AggregateFunction(argMin, Float64, DateTime64(3, 'Asia/Kolkata')),
    high_state   AggregateFunction(max, Float64),
    low_state    AggregateFunction(min, Float64),
    close_state  AggregateFunction(argMax, Float64, DateTime64(3, 'Asia/Kolkata')),
    volume_state AggregateFunction(sum, UInt64),
    pv_state     AggregateFunction(sum, Float64),
    n_state      AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (ticker, window_start);

CREATE VIEW IF NOT EXISTS nse.vw_bars_1m_ch AS
SELECT
    ticker,
    window_start,
    argMinMerge(open_state)            AS open,
    maxMerge(high_state)               AS high,
    minMerge(low_state)                AS low,
    argMaxMerge(close_state)           AS close,
    sumMerge(volume_state)             AS volume,
    sumMerge(pv_state) / sumMerge(volume_state) AS vwap,
    countMerge(n_state)                AS tick_count
FROM nse.bars_1m_ch
GROUP BY ticker, window_start;
