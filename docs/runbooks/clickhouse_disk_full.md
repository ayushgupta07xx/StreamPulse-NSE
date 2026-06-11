# Runbook: ClickHouse disk full / memory pressure

**Symptoms:** inserts failing with `DB::Exception: Cannot reserve ... disk space`,
`TOO_MANY_PARTS`, or the Kafka-engine consumers stalling; `ClickHouseDown` /
`ClickHouseSlowQueries` alerts.

## 1. What is consuming space?

```sql
-- per-table on-disk bytes
SELECT table, formatReadableSize(sum(bytes_on_disk)) AS size, count() AS parts
FROM system.parts WHERE active AND database = 'nse'
GROUP BY table ORDER BY sum(bytes_on_disk) DESC;

-- partition breakdown for the biggest table
SELECT partition, formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts WHERE active AND table = 'ticks_clean'
GROUP BY partition ORDER BY partition;
```

```bash
docker exec streampulse-clickhouse df -h /var/lib/clickhouse
```

## 2. Immediate relief

Tick data is the volume driver (50 tickers × 1/s ≈ 4M rows/day replayed).
Options in increasing severity:

```sql
-- 1. force TTL cleanup now (TTLs: ticks 30d, bars 2y, late 7d)
OPTIMIZE TABLE nse.ticks_clean FINAL;

-- 2. drop old daily partitions explicitly
ALTER TABLE nse.ticks_clean DROP PARTITION '2026-05-01';

-- 3. dev nuclear option: truncate replayable data (regenerate via make demo)
TRUNCATE TABLE nse.ticks_clean;
```

All tick data is synthetic and reproducible from committed parquet + seeds —
dropping it loses nothing permanent (bars/anomalies are the analytical record).

## 3. Tighten retention

Edit `clickhouse/ttl_policies.sql` (e.g. ticks 30d → 7d) and re-apply:

```bash
docker exec -i streampulse-clickhouse clickhouse-client --multiquery < clickhouse/ttl_policies.sql
```

## 4. TOO_MANY_PARTS (insert storm)

The Kafka engines flush small blocks under bursty replay. Mitigations already
configured: `kafka_max_block_size=65536`. If parts explode during a `--speed
max` bulk load:

```sql
SELECT table, count() AS active_parts FROM system.parts
WHERE active AND database='nse' GROUP BY table;

OPTIMIZE TABLE nse.ticks_clean PARTITION '<hot-partition>';
```

and replay at a lower speed (the pipeline's verified operating envelope is
100× with `--ooo-seconds 360 --idle-seconds 0`).

## 5. Memory pressure (shared 12.5 GB Docker VM)

ClickHouse spikes during merges/Kafka batches. The compose service caps usage
implicitly via the VM; on k8s the chart sets `limits.memory: 1536Mi`. If the
server OOMs repeatedly, lower `kafka_max_block_size` to 16384 and/or raise the
container limit — and check sibling stacks aren't squeezing the VM
(`docker stats --no-stream`).
