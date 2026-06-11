# StreamPulse NSE — COMPLETE SESSION HANDOFF

> **Read this entire file before doing anything.** It is the full context of the
> build session that took this project from empty folder to Day 10 of 14. Read
> alongside: the project brief at `E:\Analyst files\04_StreamPulseNSE.md`
> (single source of truth — day plan §8, success criteria §21, rules §22),
> `docs/decisions.md` (ADRs), `docs/PROGRESS.md` (day checklist), `git log`.

---

## 1. Identity & access

| Item | Value |
|---|---|
| Working dir | `D:\StreamPulse NSE` (note the space — quote all paths) |
| GitHub | https://github.com/ayushgupta07xx/StreamPulse-NSE (public), pushed via `gh` auth (user `ayushgupta07xx`), git identity `zilianx <agcr7jw@gmail.com>` |
| Discord webhook | in local `.env` (gitignored, NEVER commit) — used by Alertmanager; a live test alert was verified delivered |
| ClickHouse | network user `streampulse`/`streampulse` (the `default` user is server-local-only in CH 24.x — provisioned via `clickhouse/config/users-streampulse.xml` and the Helm users ConfigMap) |
| GCP | **NOT yet provisioned** — Day 12 needs the user to supply a billing-enabled project + `gcloud auth login` (~$3–5 spend, brief §11 protocol) |
| MLflow | http://localhost:5000, experiment `streampulse-anomaly`, model v`21e618c9` logged |

## 2. ABSOLUTE USER REQUIREMENTS (violating these damages trust)

1. **NO AI ATTRIBUTION ANYWHERE.** The user removed all `Co-Authored-By` trailers
   (history rewritten + force-pushed) and disabled attribution. Never add
   AI/tool references to commits, code comments, docs, or the repo. This is
   presented as the user's solo project.
2. **Ask before deviating from the brief.** Any change vs
   `04_StreamPulseNSE.md` scope → stop, present options, wait for the user's
   choice. Approved deviations so far are all ADR'd in `docs/decisions.md`.
3. **Tuning iterations:** user authorized "try several times, then settle"
   (e.g. 100×-replay correctness; falls back to 10–25× if needed).
4. **No long blind waits.** Use instant snapshot checks (consumer lag twice,
   30 s apart = falling means working). The user interrupts anything that looks
   stuck. Long waits → background tasks + notifications.
5. **Token/time consciousness.** Trim tool output; don't re-verify what's already verified.
6. Custom Helm charts (not Bitnami wrappers) — user-confirmed choice.

## 3. Machine & environment facts (hard-won)

- Windows 11, 20 logical CPUs, 15.6 GB RAM; Docker Desktop VM **11.7 GB shared
  with sibling project stacks** (jobatlas, sentinelops kind cluster,
  creatorpulse postgres — all running; do NOT touch them). RAM is the #1
  constraint: three separate TaskManager OOM incidents.
- Host ports: defaults are TAKEN by siblings → StreamPulse uses **29092
  (Kafka), 28081 (schema registry), 28082 (proxy), 28088 (Flink UI), 29000
  (CH native), 23000 (Grafana)**; plain: 8123 (CH HTTP), 9090, 9093, 5000
  (MLflow), 8085 (Redpanda console), 9644, 8000.
- Tools: Python 3.11 = `py -3.11`, venv at `.venv` (Windows layout
  `.venv\Scripts\python.exe`), Poetry 2.4.1, Terraform 1.15.6, kind 0.32.0,
  Helm 4.2.0, gh 2.93.0, GNU Make 3.81 at
  `C:\Program Files (x86)\GnuWin32\bin\GnuWin32\bin` (nested path!).
- **`rpk` has no Windows build** → always
  `docker exec streampulse-redpanda rpk ... -X brokers=localhost:9092`
  (the -X flag is required for group/admin commands — rpk otherwise dials the
  advertised external listener which doesn't resolve in-container).
- **Shells:** PowerShell 5.1 — no `<` redirection (use git-bash for stdin
  redirects), no `&&`, writes BOM with `-Encoding utf8` (the `.env` must stay
  BOM-free), gh's `--jq` quoting breaks (use bash for gh). New shells have
  stale PATH → `$env:PATH = [Environment]::GetEnvironmentVariable('PATH','Machine') + ';' + [Environment]::GetEnvironmentVariable('PATH','User')`.
- **Windows console is cp1252** → set `PYTHONUTF8=1` for anything that prints
  emoji (MLflow).
- Flink container Python is **3.10** (PyFlink 1.18 ceiling): nothing under
  `apps/flink/` may use 3.11-only idioms (`datetime.UTC` etc.) — ruff
  per-file-ignore `UP017` guards this; mind it for new code.

## 4. State of each day (all verified results)

- **Day 1 ✅** 9-container compose stack healthy (Redpanda 24.2.18 KRaft,
  custom `streampulse/flink:1.18-py` image = flink:1.18.1 + Py3.10 +
  apache-flink + kafka connector jar + chowned /opt/flink/checkpoints,
  ClickHouse 24.8, Prometheus, Alertmanager (Discord via sed-templated env),
  Grafana 11.2 (+clickhouse plugin), MLflow, console). rpk smoke passed.
- **Day 2 ✅** 50/50 Nifty tickers, 1y daily OHLC parquet committed
  (TATAMOTORS→TMPV, LTIM→TRENT — old symbols dead on Yahoo). Generator: GBM +
  Brownian-bridge close-pinning + jump diffusion + U-shaped volume; anomaly
  injector (4 types) with ground-truth JSON; **measured 110,424 ticks/s at
  max** (criterion 1k). Ticks carry per-ticker `seq` + `session_id` and the
  Kafka **record timestamp = event time** (critical — see §5).
- **Day 3 ✅** validate_enrich job: exactly-once PROVEN — SIGKILLed the
  task-hosting TM mid-stream; `scripts/verify_exactly_once.py` over 1.36M
  msgs (read_committed): **0 dups, 0 gaps** across 50 tickers.
- **Day 4 ✅** window_bars (1m/5m/15m tumbling, late→`nse.bars.late`) +
  session_bars (5-min-gap session windows → `nse.bars.session`). Verified at
  **100×** after the watermark odyssey (§5): final windows exact (60/300/900
  interior ticks), 0 refinements in final run, OHLC/vwap invariants clean.
- **Day 5 ✅** Full CH schema applied: ReplacingMergeTree ticks (dedupe =
  at-least-once Kafka engine → exactly-once effective), bars
  (version=_ingested_at absorbs window refinements), AggregatingMergeTree
  `bars_1m_ch` (independent CH-side aggregation), anomalies, anomalies_ml,
  Kafka engine tables + MVs (`kafka_handle_error_mode='stream'`, ts parsed via
  `parseDateTime64BestEffort` from String). **Exact reconciliation passed**:
  CH tick count == generator count; CH-side 1m bars == Flink 1m bars
  cross-engine agreement on a verified session (Day 5 commit).
- **Day 6 ✅** Prometheus scraping redpanda/flink JM+TMs(dns_sd)/CH(9363)/
  generator; 4 Grafana dashboards provisioned (market_overview, anomaly_feed,
  pipeline_health, ml_performance); Alertmanager → Discord verified with a
  real alert. Full rule set in `observability/prometheus/alerts.yml`.
- **Day 7 ✅** Custom Helm charts (redpanda STS, clickhouse STS + init Job +
  users.d, flink JM/TM + HPA + jobs/refdata ConfigMaps, prometheus pod-SD +
  RBAC, grafana provisioning, generator, flink-jobs submit Job) + umbrella
  `streampulse-platform` + `helm/values/values-tiny.yaml`. Ran the FULL
  pipeline on kind: 1.25M ticks + 7.6K anomalies in CH-on-k8s; evidence in
  `docs/images/k8s-get-all.txt`. Cluster torn down (RAM) — `make k8s` rebuilds.
  Chart files synced from sources via `make sync-helm-files`
  (`scripts/ci_sync_helm.py` in CI).
- **Day 8 ✅** Online detectors (see §6) benchmarked vs 200 injected anomalies
  (~150 price-visible): zscore P=.29 R=.30 median latency 6 event-s; ewma
  P=.07 R=.46 lat 62 s; ensemble(≥2) R=.25. Honest-numbers doctrine (§22.5).
- **Day 9 ✅** IF v21e618c9 (contamination .01, 6 features) trained on
  full-day corpus (2,056 rows; val flag 1.02%), MLflow + joblib + meta JSON in
  `models/`. predict_loop verified: 2,600 bars → 50 flags → `anomalies_ml` +
  `isolation_forest` events to `nse.anomalies`. CronJob (k8s) +
  `scheduled-retrain.yml` (GH cron) exist.
- **Day 10 ✅** 4-method benchmark vs 200 injected anomalies finished
  (commit 5f5f8af): final table + tuning story in docs/detection-benchmarks.md.
  EWMA retuned on measurement — see §6/§7 for the updated config and lessons.
- **Day 13(a) ✅ early:** CI fully green end to end (run after 732eb71) — k3d
  root cause was charts hardcoding kind's `standard` StorageClass (k3s has
  none; PVCs now use the cluster default). sqlfluff config lives in
  pyproject.toml ([tool.sqlfluff.core] — it overrides .sqlfluff), hadolint
  failure-threshold=warning, actions upgraded for Node 24.
- **Days 11, 12, 13(b-d), 14 ⬜** — plan + status in §8.

## 5. THE deep technical lessons (do not relearn these)

1. **Event time lives in the Kafka record timestamp.** Generator stamps it at
   produce; Flink KafkaSink propagates element timestamps; ALL jobs watermark
   at the source with `record_ts_watermarks()` (pure-Java path — a Python
   timestamp assigner at the source throttled the pipeline to ~17 rec/s).
   Source-level = per-partition watermarks (min across splits) which absorbs
   consumer skew. Post-source assignment at N× replay = late-event storms.
2. **Replay speed multiplies disorder**: OOO bound is event-time; submit jobs
   with `--ooo-seconds ≈ 5×speed` for fast replays. `--idle-seconds 0` for
   replay verification (wall-clock idleness falsely idles splits during
   accelerated replay; keep default 10 s for live 1×).
3. **Same-trading-date replays merge into the same event-time windows.**
   ALWAYS `scripts/reset_pipeline.py` between sessions (cancels jobs
   verified, wipes topics+groups, truncates CH, resubmits with flags).
4. **Checkpoint-restored detector state + backwards event-time jump = poisoned
   rolling windows** (left-eviction can't remove "future" ticks). Guard
   implemented in anomaly_online `_rolling()` — resets keyed state on
   backwards jumps > window span.
5. **Timezones:** clickhouse-connect returns DateTime64 as naive UTC; truth
   JSON carries +05:30 → the evaluator matches in **epoch-ms only**. pandas
   `to_datetime(..., utc=True)` for mixed-tz frames.
6. **TaskManager OOM pattern:** PyFlink worker processes live OUTSIDE
   `taskmanager.memory.process.size`. Survival config (in compose): 2 slots/TM,
   process 1280m, managed.fraction 0.25, container `memory: 2g` hard limit,
   `restart: unless-stopped`. 3 Flink jobs simultaneously → submit with
   `--parallelism 1` (4 slots total). Flink UI is the truth: zero TMs
   registered = they crashed; jobs RESTARTING + NoResourceAvailable = slot
   starvation.
7. **PyFlink rough edges (ADR-007):** no Rich* classes (open() lives on plain
   Map/FlatMapFunction); `output_type=Types.STRING()` is MANDATORY before Java
   sinks (else pickled bytes → ClassCastException); sinks are value-only (no
   record keys); transactional sink needs `transaction.timeout.ms` ≤ broker
   cap; submit via
   `docker exec streampulse-flink-jm flink run -py /opt/streampulse/flink/jobs/<job>.py --pyFiles /opt/streampulse/flink/jobs -d [--ooo-seconds N] [--idle-seconds N] [--parallelism N]`.
8. **Windows window-fire refinements are normal** under allowed lateness;
   downstream ReplacingMergeTree keeps the last refinement; the bar verifier
   grades final refinement per (ticker, window).
9. **MLflow sqlite volume corrupts if the container dies mid-write** → wipe
   `streampulse_mlflow-data` volume and restart (no valuable data unless runs
   matter).
10. **typer ≥0.16 required** (0.12 crashes on click 8.3). B008 ignored in ruff
    (typer idiom). Generator CLI needs the root `@app.callback()` to keep
    `run` a subcommand.

## 6. Current detector configuration (apps/flink/jobs/anomaly_online.py)

- Z-score (price vs 5-min rolling window): warmup 60 ticks; **two-tier**:
  |z| ≥ 6 fires instantly; 4 ≤ |z| < 6 needs 2 consecutive ticks.
- EWMA SPC on **log-returns** (λ=0.2), retuned Day 10 on measurement:
  **mean chart = |EWMA| > 6σ_ewma** (WE rules dropped — fat-tailed tick
  returns saturate them); **dispersion chart** = fast/slow variance ratio
  > **10**; slow baseline updates UNCONDITIONALLY during the 240-tick warmup
  (freeze-gate deadlock fix), frozen during bursts after; **out-of-order
  ticks are skipped per key** (keyless clean topic ⇒ partition interleave ⇒
  artificial zigzag returns; measured 1,316 vs 39 dispersion alarms).
- 120 s per-(ticker, method) cooldown. FPs eat cooldowns and suppress TPs —
  precision work directly buys recall.
- VOLUME_SURGE (~50/200 of truth) is invisible to both by design → IF's job.
- Final benchmark (sess-533880cc7830, grace 330 s): zscore P=.19 R=.615
  lat 16 event-s; ewma_spc P=.597 R=.52 F1=.556; IF P=.135 R=.08 (single-day
  in-sample training — multi-day corpus is the known improvement); ARIMA
  P=.135 R=.035. Settled per the try-then-settle rule; full story in
  docs/detection-benchmarks.md.

## 7. Day 10 lessons (kept so they're not relearned)

1. **Tune detectors offline first**: replaying a session's ticks from
   ClickHouse through a faithful copy of the detector logic lets you sweep
   configs in minutes instead of one ~40-min pipeline cycle per attempt.
   Caveat: the offline replay is perfectly event-time-ordered — it CANNOT
   reproduce disorder-sensitive behavior (that's how the dispersion-chart
   storm was missed until the live re-run).
2. **An evaluator can flatter a saturated detector**: ewma's old R=.965 was
   an artifact of firing everywhere; precision collapsed once the baseline
   deadlock was fixed and the real recall surfaced.
3. predict_loop/arima_forecast have **no idle exit** — with --max-bars above
   the topic's message count they poll forever. Watch group lag (ml-predict-
   loop, ml-arima) and stop them at 0.
4. anomalies/anomalies_ml are **plain MergeTree** (no dedupe): never re-run
   scoring over a partially-scored topic without truncating + group reset.

## 8. Remaining days — exact plans

- **Day 11 (~45 min, all staged):** `docker compose --profile aws up -d localstack`;
  `terraform -chdir=infra/terraform/envs/local apply -auto-approve` (validates
  clean already; targets LocalStack endpoints; creates kinesis `nse-ticks-raw`,
  s3 `streampulse-archive`, lambda validate/enrich (event-source-mapped to the
  stream, writes DynamoDB `streampulse-state` + S3), glue catalog);
  `python -m generator.main run --speed 10 --duration-s 120 --target kinesis`;
  verify: `aws --endpoint-url http://localhost:4566 kinesis ...` / dynamodb
  scan / s3 ls (use bash + fake creds test/test, region ap-south-1); capture
  evidence to docs/images/aws/; docs/cloud-architecture-aws.md exists.
- **Day 12 (USER GATE):** brief §11 protocol exactly: budget alerts → terraform
  apply envs/demo (gcp module: pubsub `nse-ticks`, dataflow Beam job
  `apps/beam/` (exists), BigQuery) → 30 min ticks `--target pubsub`
  (`pip install google-cloud-pubsub` first; env GCP_PROJECT) → screenshots →
  `terraform destroy -auto-approve` → verify empty. $3–5.
- **Day 13 (~2 h):** (a) get CI green: lint+mypy+format pass locally as of last
  push; trivy fixed (`aquasecurity/trivy-action@v0.36.0` — tags need the v);
  k3d had a transient ghcr timeout, retry added; watch run for commit b544ae1+.
  (b) **Protobuf** (only substantial code left): write `schemas/protobuf/tick.proto`
  (+bar/anomaly), register with Redpanda's built-in schema registry
  (localhost:28081, Confluent-compatible), generator gains
  `--format protobuf` behind a flag (JSON stays default — brief §20 demands
  both paths concurrently), Flink jobs get a protobuf deserializer path.
  (c) Helm: add NetworkPolicies (PDBs/probes/limits/RBAC/HPA already done).
  (d) tag v0.1.0 + release.yml.
- **Day 14 (~45 min + user recording):** README rewrite (hero diagram, badges,
  measured numbers: 110k ticks/s, 0-dup fault test, 6-event-s detection,
  benchmark table, screenshots), capture Grafana screenshots during a
  `make demo` run (23000), docs/demo.md + scripts/inject_demo_anomalies.py
  exist; retro in docs/retro.md; §21 checklist tick-through; LinkedIn draft.

## 9. Reference: key commands

```text
make up / down / ps / logs / smoke      # compose lifecycle
make flink-jobs [OOO=n]                 # submit all jobs
make reset [OOO=n]                      # full event-time reset
make retrain / predict / benchmark      # ML + eval
make k8s / k8s-down                     # kind + umbrella chart (syncs files first)
python scripts/reset_pipeline.py --jobs validate_enrich,window_bars,anomaly_online \
    --ooo-seconds 240 --idle-seconds 0 --parallelism 1
python -m generator.main run --speed {1|10|25|100|max} [--duration-s N] \
    [--anomalies N] [--seed N] [--target kafka|kinesis|pubsub]
docker exec streampulse-redpanda rpk group describe <group> -X brokers=localhost:9092
# drain test: TOTAL-LAG falling = working; identical 2 min apart = stuck
# groups: flink-validate-enrich, flink-window-bars, flink-anomaly-online,
#         ml-predict-loop, ml-arima, clickhouse-*
```

## 10. Open items / honest debts

1. Day 10 evaluation unfinished (§7) — ewma precision likely still weak; the
   brief blesses honest numbers; one more tuning pass is allowed before
   settling (user rule).
2. CI k3d job has never gone green end-to-end (one transient infra failure,
   retry now in place — first real verdict comes from the next run).
3. predict_loop per-row inserts are slow — batch them if it matters.
4. The 1-day-corpus IF trains/validates in-sample (documented fallback);
   multi-day corpus would be better but costs stream time.
5. `data/training_ground_truth.json` (seed 555) is the training-day truth;
   `data/anomaly_ground_truth.json` is ALWAYS the latest benchmark session's.
6. Sibling Docker stacks throttle everything — if the user ever stops them,
   slots/parallelism can go back up (compose values are the safe floor).
```
