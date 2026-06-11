-- StreamPulse NSE — Kafka-engine ingestion + materialized views (Day 5)
-- Apply AFTER schema.sql: clickhouse-client --multiquery < clickhouse/materialized_views.sql
--
-- Pattern per topic:  Kafka engine table ──MV──▶ storage table
-- Gotcha guards (§20): small max block, error stream mode, JSON parsed
-- leniently (timestamps land as String and are parsed in the MV).

-- ─────────────────────────────── ticks.clean ───────────────────────────────
CREATE TABLE IF NOT EXISTS nse.kafka_ticks_clean
(
    ticker        String,
    timestamp_ist String,
    price         Float64,
    volume        UInt64,
    side          String,
    exchange      String,
    session_id    String,
    seq           UInt64,
    name          String,
    sector        String,
    industry      String,
    mcap_bucket   String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'nse.ticks.clean',
    kafka_group_name = 'clickhouse-ticks-clean',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS nse.mv_ticks_clean TO nse.ticks_clean AS
SELECT
    ticker,
    parseDateTime64BestEffort(timestamp_ist, 3, 'Asia/Kolkata') AS timestamp_ist,
    price,
    volume,
    CAST(side, 'Enum8(''BUY'' = 1, ''SELL'' = 2)') AS side,
    exchange,
    session_id,
    seq,
    name,
    sector,
    industry,
    mcap_bucket
FROM nse.kafka_ticks_clean
WHERE length(_error) = 0;

-- ClickHouse-native 1m pre-aggregation off the same Kafka feed (AggregatingMergeTree)
CREATE MATERIALIZED VIEW IF NOT EXISTS nse.mv_bars_1m_ch TO nse.bars_1m_ch AS
SELECT
    ticker,
    toStartOfMinute(parseDateTime64BestEffort(timestamp_ist, 3, 'Asia/Kolkata')) AS window_start,
    argMinState(price, parseDateTime64BestEffort(timestamp_ist, 3, 'Asia/Kolkata')) AS open_state,
    maxState(price)                       AS high_state,
    minState(price)                       AS low_state,
    argMaxState(price, parseDateTime64BestEffort(timestamp_ist, 3, 'Asia/Kolkata')) AS close_state,
    sumState(volume)                      AS volume_state,
    sumState(price * volume)              AS pv_state,
    countState()                          AS n_state
FROM nse.kafka_ticks_clean
WHERE length(_error) = 0
GROUP BY ticker, window_start;

-- ─────────────────────────────── bars (1m/5m/15m share one table) ───────────────────────────────
CREATE TABLE IF NOT EXISTS nse.kafka_bars_1m
(
    ticker String, bar_size String, window_start String, window_end String,
    open Float64, high Float64, low Float64, close Float64,
    volume UInt64, vwap Float64, tick_count UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'nse.bars.1m',
    kafka_group_name = 'clickhouse-bars-1m',
    kafka_format = 'JSONEachRow',
    kafka_max_block_size = 65536,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS nse.kafka_bars_5m AS nse.kafka_bars_1m
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'nse.bars.5m',
    kafka_group_name = 'clickhouse-bars-5m',
    kafka_format = 'JSONEachRow',
    kafka_max_block_size = 65536,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS nse.kafka_bars_15m AS nse.kafka_bars_1m
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'nse.bars.15m',
    kafka_group_name = 'clickhouse-bars-15m',
    kafka_format = 'JSONEachRow',
    kafka_max_block_size = 65536,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS nse.mv_bars_1m TO nse.bars AS
SELECT
    ticker, bar_size,
    parseDateTime64BestEffort(window_start, 3, 'Asia/Kolkata') AS window_start,
    parseDateTime64BestEffort(window_end, 3, 'Asia/Kolkata')   AS window_end,
    open, high, low, close, volume, vwap, tick_count
FROM nse.kafka_bars_1m
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS nse.mv_bars_5m TO nse.bars AS
SELECT
    ticker, bar_size,
    parseDateTime64BestEffort(window_start, 3, 'Asia/Kolkata') AS window_start,
    parseDateTime64BestEffort(window_end, 3, 'Asia/Kolkata')   AS window_end,
    open, high, low, close, volume, vwap, tick_count
FROM nse.kafka_bars_5m
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS nse.mv_bars_15m TO nse.bars AS
SELECT
    ticker, bar_size,
    parseDateTime64BestEffort(window_start, 3, 'Asia/Kolkata') AS window_start,
    parseDateTime64BestEffort(window_end, 3, 'Asia/Kolkata')   AS window_end,
    open, high, low, close, volume, vwap, tick_count
FROM nse.kafka_bars_15m
WHERE length(_error) = 0;

-- ─────────────────────────────── late bars (raw passthrough) ───────────────────────────────
CREATE TABLE IF NOT EXISTS nse.kafka_bars_late
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'nse.bars.late',
    kafka_group_name = 'clickhouse-bars-late',
    kafka_format = 'RawBLOB',
    kafka_max_block_size = 65536;

CREATE MATERIALIZED VIEW IF NOT EXISTS nse.mv_bars_late TO nse.bars_late AS
SELECT raw FROM nse.kafka_bars_late;

-- ─────────────────────────────── anomalies ───────────────────────────────
CREATE TABLE IF NOT EXISTS nse.kafka_anomalies
(
    ticker           String,
    ts               String,
    detection_method String,
    score            Float64,
    severity         UInt8,
    context          String,
    session_id       String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'nse.anomalies',
    kafka_group_name = 'clickhouse-anomalies',
    kafka_format = 'JSONEachRow',
    kafka_max_block_size = 65536,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS nse.mv_anomalies TO nse.anomalies AS
SELECT
    ticker,
    parseDateTime64BestEffort(ts, 3, 'Asia/Kolkata') AS ts,
    detection_method,
    score,
    severity,
    context,
    session_id
FROM nse.kafka_anomalies
WHERE length(_error) = 0;
