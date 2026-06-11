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
- [ ] Day 4 — Flink 1m/5m/15m OHLCV bars + late side-output
- [ ] Day 5 — ClickHouse schema, Kafka engines, MVs, TTLs
- [ ] Day 6 — Prometheus scrape all, 3 Grafana dashboards, Alertmanager→Discord
- [ ] Day 7 — kind + Helm umbrella chart, pipeline on k8s
- [ ] Day 8 — online anomaly job (Z-score + EWMA SPC), ≥80% recall vs ground truth
- [ ] Day 9 — Isolation Forest retrain + MLflow + predict loop
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
