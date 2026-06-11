# Streaming Deep Dive: Exactly-Once, Watermarks, and Replay Correctness

This document records how StreamPulse achieves provable streaming correctness,
including five real defects found and fixed on the way to its verification
standard. Every claim below was measured on this repository — commands and
numbers included.

## 1. The verification standard

Most streaming demos assert "bars look right." StreamPulse holds itself to:

> **At 100× replay speed, every interior 1-minute window must contain exactly
> its 60 ticks — proven by consuming and auditing every produced bar — and a
> mid-stream TaskManager SIGKILL must produce zero duplicates and zero gaps.**

Final verified result (`scripts/verify_bars.py`, seed-123 session, 407,650
ticks at 4,520 ticks/s):

```
nse.bars.1m:  firings=6450 final_windows=6450 refinements=0 misaligned=0
              ohlc_violations=0 vwap_out_of_range=0 interior_bad_tick_count=0/6350
nse.bars.5m:  firings=1250 final_windows=1250 refinements=0 ... 0/1150
nse.bars.15m: firings=400  final_windows=400  refinements=0 ... 0/300
BARS VERIFIED
```

## 2. Exactly-once, end to end

**Mechanism (§13):**
- Generator → Kafka: idempotent producer (`enable.idempotence=true`, acks=all).
- Flink: `EXACTLY_ONCE` checkpointing every 10 s to RocksDB (incremental);
  Kafka sources resume from offsets committed atomically with checkpoints.
- Flink → Kafka: transactional `KafkaSink` (two-phase commit bound to
  checkpoints; `transaction.timeout.ms` raised above the checkpoint interval).
- All consumers (Flink, ClickHouse Kafka engine via librdkafka, verifier):
  `isolation.level=read_committed`.
- ClickHouse landing tables: `ReplacingMergeTree` keyed on `(ticker,
  timestamp_ist, seq)` — the Kafka engine is at-least-once, and replayed
  batches collapse on merge because replays are byte-identical.

**Fault-injection proof (Day 3).** While a 74,400-tick session streamed at
10×, the TaskManager hosting the job was killed with SIGKILL (`docker kill`,
no graceful shutdown), restarted 8 s later. Flink failed over and resumed from
the last checkpoint. The audit consumed all 1.36M messages then in the clean
topic (read_committed) and checked per-ticker sequence numbers:

```
tickers=50 | duplicates=0 | gaps=0
EXACTLY-ONCE VERIFIED
```

The per-ticker monotonic `seq` stamped by the generator is what makes this
audit cheap and total — design for verifiability up front.

**End-to-end reconciliation (Day 5).** After a clean 419,550-tick session:
`SELECT count() FROM nse.ticks_clean FINAL` = **419,550** — exact across five
hops. Independently, ClickHouse's own AggregatingMergeTree pre-aggregation
agreed with Flink's windows on **6,650 of 6,650** bars (OHLC within 0.005,
volume and tick_count exact) — two unrelated aggregation engines, same answer.

## 3. Event-time architecture (the five defects)

The path to the §1 numbers surfaced five real distributed-systems defects.
Each is a lesson in event-time mechanics.

### Defect 1 — watermarks assigned after the source

`assign_timestamps_and_watermarks()` downstream of the source collapses
watermarking to per-subtask max-seen. A subtask reading 6 partitions at
uneven offsets converts cross-partition consumption skew directly into
event-time disorder — at 100× replay, a 1-wall-second skew is 100 event-
seconds, and the 5 s bound was breached constantly (≈35% of windows lost
ticks to lateness).

**Fix:** pass the watermark strategy to `env.from_source(...)`. Flink then
tracks per-split watermarks and the operator watermark is the minimum across
splits — consumption skew is absorbed entirely.

### Defect 2 — Python timestamp assigner at the source

A Python `TimestampAssigner` forces every record across the JVM↔Python bridge
*before* the pipeline's batched Python operators. Throughput collapsed.

**Fix:** event time lives in the **Kafka record timestamp**. The generator
stamps `timestamp=event_ms` at produce; Flink's `KafkaSink` propagates element
timestamps onto produced records (verified: record ts == payload event time on
`nse.ticks.clean`), so every downstream hop watermarks off record timestamps
with zero Python in the path:

```python
WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(bound))
# no .with_timestamp_assigner — record timestamps, pure-Java
```

### Defect 3 — PyFlink cannot key sink records

`KafkaRecordSerializationSchema`'s Python builder cannot derive a message key
from an element field, so Flink-produced topics are unkeyed (ADR-007). Flink's
own keyed operators are unaffected (`key_by` is internal), but it sets up
defect 4.

### Defect 4 — parallel producers interleave event time within partitions

With sink parallelism 2, two producers append to the same unkeyed partitions
with mutual event-time skew — under backpressure, *within-partition
regressions* of tens of event-seconds. Per-split watermarking cannot help:
the regression is inside a single split.

**Fix at this scale:** sink parallelism 1 — a single producer's partition
streams are monotonic in send order. At real scale the equivalent is a keyed
sink (Java) or per-key producer pools. Measured effect: 15m-bar refinements
fell from 16,901 to 0.

### Defect 5 — wall-clock idleness during accelerated replay

`.with_idleness(10s)` exists for thinly-traded stocks at 1× (an idle split
must not stall the watermark). But idleness is **wall-clock**: during a 100×
backlog catch-up, fetch-scheduling gaps >10 s wall are routine, splits get
falsely idled, the watermark leaps ahead of them, and their records return
"late" — 102K ticks per session side-outputted. At 10× the pipeline keeps up
and the gaps never reach 10 s, which is why the defect hid at lower speeds.

**Fix:** `--idle-seconds 0` for replay/backfill submissions; 10 s stays the
live default.

## 4. The replay calibration rule

Bounded out-of-orderness is calibrated in *wall-clock margin at the replay
speed*:

```
ooo_event_seconds ≈ wall_margin_seconds × replay_speed
```

The dominant wall-clock disturbance is the exactly-once checkpoint barrier
(observed 2–4 s with Python operators). Hence `--ooo-seconds 360` (3.6 s wall)
at 100×, while live 1× runs the §13 default of 5 s. Submit-time knobs, not
code changes:

```bash
flink run ... window_bars.py --ooo-seconds 360 --idle-seconds 0   # backfill
flink run ... window_bars.py                                       # live
```

## 5. Late data is a feature, not a failure

Windows refine under `allowed_lateness(30s)`; every firing supersedes the
last, and `ReplacingMergeTree(_ingested_at)` keeps the final refinement —
the verifier evaluates exactly what the database keeps. Events later than the
lateness budget go to the `nse.bars.late` side-output rather than vanishing:
during the TM-death forensics this captured **1,097,124 events** — the entire
displaced stream body, recoverable and inspectable.

## 6. Topic design (§13 as built)

| Topic | Partitions | Retention | Notes |
|---|---|---|---|
| nse.ticks.raw | 12 | 24h | keyed by ticker (generator producer) |
| nse.ticks.clean | 12 | 24h | unkeyed (ADR-007); record-ts = event time |
| nse.bars.1m / 5m / 15m | 6/6/3 | 7d/30d/90d | window refinements appended |
| nse.anomalies | 3 | 30d | all four detectors, one schema |
| nse.bars.late | 1 | 7d | lateness side-output |
| nse.bars.session | 3 | 7d | session-window summaries (ADR-008) |

## 7. Reproducing the verification

```bash
make up                                   # full stack
python scripts/reset_pipeline.py --ooo-seconds 360 --idle-seconds 0
PYTHONPATH=apps python -m generator.main run --speed 100 --duration-s 90 --seed 123
# wait for lag 0 (rpk group describe flink-window-bars)
python scripts/verify_bars.py             # expects: BARS VERIFIED
python scripts/verify_exactly_once.py --session-id <from ground truth json>
```
