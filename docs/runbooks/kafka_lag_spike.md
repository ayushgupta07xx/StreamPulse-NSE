# Runbook: Kafka consumer lag spike

**Symptoms:** `KafkaConsumerLagHigh` alert (group lag > 10K for 2 min);
Pipeline Health dashboard lag panel climbing; end-to-end freshness > 10 s.

## 1. Which group, which topic?

```bash
docker exec streampulse-redpanda rpk group list -X brokers=localhost:9092
docker exec streampulse-redpanda rpk group describe <group> -X brokers=localhost:9092
```

Pipeline groups: `flink-validate-enrich` (nse.ticks.raw),
`flink-window-bars` / `flink-anomaly-online` (nse.ticks.clean),
`clickhouse-*` (ClickHouse Kafka engines), `ml-*` (Python consumers).

## 2. Producer surge or consumer stall?

```bash
# producer rate (Prometheus)
curl -s 'http://localhost:9090/api/v1/query?query=sum(rate(generator_messages_produced_total[1m]))'
# consumer side: Flink backpressure
#   Flink UI → job → vertex → BackPressure tab, or:
curl -s http://localhost:28088/jobs/<id>/vertices/<vid>/backpressure
```

- **Producer surge** (generator at `--speed max` ≈ 110k ticks/s vs pipeline
  ~5-15k/s): expected during bulk replay — lag drains after the burst. Confirm
  drain trend rather than absolute lag.
- **Consumer stall**: job RESTARTING (→ flink_job_failure runbook), or Python
  operator backpressure (check TaskManager CPU throttling).

## 3. Known structural causes

1. **TaskManager OOM-thrash before death** — consumption crawls (~17 rec/s),
   then exit 143/239. Fix memory, recreate TM; job recovers from checkpoint.
2. **ClickHouse Kafka engine paused** — `kafka_handle_error_mode='stream'`
   keeps consuming through bad records; check
   `SELECT * FROM system.kafka_consumers` for stalled assignments and
   `DETACH/ATTACH TABLE nse.kafka_<x>` to bounce one engine.
3. **Replay session collision** — late-storm floods `nse.bars.late` (designed
   safety valve; 1.1M events captured during fault testing). Reset between
   replays: `python scripts/reset_pipeline.py`.

## 4. Emergency relief valve

Throttle or stop the generator (it is the only producer):

```bash
docker stop streampulse-generator        # compose profile run
kubectl scale deploy/streampulse-generator --replicas=0   # k8s
```

Lag drains at the pipeline's steady-state rate; restart the generator at a
lower `--speed` afterwards.
