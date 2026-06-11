-- StreamPulse NSE — retention policies (Day 5)
-- Tick data: 30 days. Bar data: 2 years. Anomalies: 1 year. Late raw: 7 days.
-- Kept separate from schema.sql so retention can be tuned without touching DDL.

ALTER TABLE nse.ticks_clean
    MODIFY TTL toDateTime(timestamp_ist) + INTERVAL 30 DAY;

ALTER TABLE nse.bars
    MODIFY TTL toDateTime(window_start) + INTERVAL 2 YEAR;

ALTER TABLE nse.bars_1m_ch
    MODIFY TTL window_start + INTERVAL 2 YEAR;

ALTER TABLE nse.bars_late
    MODIFY TTL _ingested_at + INTERVAL 7 DAY;

ALTER TABLE nse.anomalies
    MODIFY TTL toDateTime(ts) + INTERVAL 1 YEAR;

ALTER TABLE nse.anomalies_ml
    MODIFY TTL toDateTime(window_start) + INTERVAL 1 YEAR;
