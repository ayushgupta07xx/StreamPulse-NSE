# Build Progress — session resume file

> **Purpose:** if a session dies (context limit, 5-hour window), the next session
> reads the project brief (`E:\Analyst files\04_StreamPulseNSE.md`), then
> `docs/decisions.md`, then this file, then `git log --oneline`, and continues
> from the first unchecked item.

## Environment facts (verified 2026-06-11)

- Machine: Windows 11, 20 logical CPUs, 15.6 GB RAM, Docker Desktop VM 12.5 GB / 4 CPUs
- Tools: Docker 29.3.1, Python 3.11.9 (`py -3.11`), Poetry 2.4.1, Terraform 1.15.6,
  kind 0.32.0, Helm 4.2.0, gh 2.93.0 (authed as ayushgupta07xx), GNU Make 3.81
  (PATH: `C:\Program Files (x86)\GnuWin32\bin\GnuWin32\bin`)
- Remote: https://github.com/ayushgupta07xx/StreamPulse-NSE (push via gh https auth)
- Discord webhook: in local `.env` (NOT committed)
- rpk: no native Windows install — always `docker exec streampulse-redpanda rpk ...`
- GCP: not yet provisioned; Day 12 needs user (billing, ~$3-5)

## Day checklist

- [x] **Day 1** — repo scaffold, docker-compose stack healthy (9 containers, Flink 2 TM / 8 slots), rpk smoke test passed
- [x] **Day 2** — 50/50 tickers cached (TATAMOTORS→TMPV, LTIM→TRENT — old symbols dead on Yahoo). Generator verified: 495 t/s @10×, **110,424 t/s @max** (criterion ≥1k). 7 topics created per §13. Ground truth JSON written. (typer pinned ^0.16 — 0.12 broke on click 8.3)
- [x] **Day 3** — validate/enrich job live (enrichment verified in nse.ticks.clean). Fault test: SIGKILL on task-hosting TM mid-stream (sess-02fcc37ed123, seed 99) → auto-recovery → verifier: 74,400/74,400 ticks, **0 dups / 0 gaps** over 1.36M total messages. PyFlink fixes: no Rich* classes, output_type=Types.STRING() mandatory before Java sinks, checkpoint volume needs flink-uid ownership (Dockerfile handles it)
- [x] **Day 4** — 1m/5m/15m bars VERIFIED AT 100× REPLAY: zero refinements, zero invariant violations, every interior window exact (60/300/900 ticks). Watermark architecture: record-ts at source (per-partition, pure-Java) + single-producer sink (monotonic partitions) + replay knobs (--ooo-seconds ≈ 3.6s-wall × speed, --idle-seconds 0 for backfill; 5s/10s live defaults). Late side-output proven under catastrophe (1.1M events captured during TM-death test). session_bars job written (verifies Day 7+)
- [x] **Day 5** — VERIFIED: ticks_clean FINAL == generator emit count exactly (419,550 through 5 hops). Flink bars vs ClickHouse AggregatingMergeTree cross-check: 6,650/6,650 windows agree on OHLC+volume+tick_count. Kafka engines + MVs + TTLs live. reset_pipeline now truncates CH tables (Kafka engines persist across topic resets)
- [x] **Day 6** — VERIFIED: Prometheus scraping redpanda/flink-jm/2×TM(DNS-SD)/clickhouse, 10 alert rules in 3 groups, Alertmanager→Discord test alert delivered, Grafana 4 dashboards + both datasources live-queried. Gotcha: CH 24.x `default` user is local-only → dedicated `streampulse` network user via users.d (compose+helm)
- [x] **Day 7** — full pipeline VERIFIED on kind (3 nodes): umbrella chart (7 subcharts), post-install hooks applied schema + submitted all 3 Flink jobs, 1.25M ticks / 2.3K bars / 7.6K anomalies in CH-on-k8s. k8s defects fixed: ConfigMap dir-shadowing (subPath!), CH startupProbe, pinned TM ports, submit-pod KAFKA_BOOTSTRAP, image-baked data. Evidence: docs/images/k8s-get-all.txt. kind torn down post-verification (make k8s recreates)
- [x] **Day 8** — online detectors live + benchmarked vs 200 injected anomalies (~150 price-visible; volume surges await IF). zscore: P=.29 R=.30 latency 6 event-s (≈0.24 s wall @1×); ewma_spc (mean+dispersion charts): P=.07 R=.46 latency 62 event-s; ensemble(≥2) R=.25. Honest numbers per §22.5; full 4-method benchmark on Day 10. Fixed en route: evaluator timezone bug (naive-UTC vs IST → zero matches), replay state-poisoning guard (backwards event-time jump resets keyed state), reset script verifies cancellations.
- [x] **Day 9** — IF v21e618c9 trained on full-day corpus (2,056 rows, val flag rate 1.02% ≈ contamination), MLflow run + joblib artifacts + version JSON. predict_loop: 2,600 bars scored, 50 flagged → anomalies_ml + ensemble topic. Fixes: TM slots 4→2 + 2g hard limit + restart policy (OOM-proof), corrupted MLflow volume wiped, PYTHONUTF8 for Windows console, utc=True for mixed-tz bars.
- [ ] Day 10 — ARIMA residuals + 4-method benchmark report
- [ ] Day 11 — LocalStack AWS (Kinesis/S3/Lambda/DynamoDB) via Terraform
- [ ] Day 12 — GCP Pub/Sub + Dataflow cycle (**blocked on user**)
- [ ] Day 13 — CI, Protobuf registry, Helm hardening, v0.1.0 tag
- [ ] Day 14 — docs, README, demo script, launch

## Session log

- **2026-06-11 (session 1):** tooling verified, permissions configured, repo
  initialized, Day 1 scaffold written + stack verified healthy + smoke test passed.
  Ports remapped to 2xxxx block (ADR-006) — sibling stacks own the defaults.
  Flink image = flink:1.18.1 + Python 3.10 + apache-flink pip (ADR-003).
  Day 2 data pulled; generator + Day 3/4/8 Flink jobs + Day 5 SQL written ahead,
  not yet executed. Next: run generator → verify throughput → Day 3 fault test.
