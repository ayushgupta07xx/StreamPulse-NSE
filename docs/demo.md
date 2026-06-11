# Demo Strategy

## Why there is no 24/7 hosted demo

StreamPulse runs Kafka + Flink + ClickHouse + Prometheus + Grafana — a stack
that needs ~6 GB RAM and real CPUs. Free-tier hosting cannot carry it, and a
$30+/month always-on VM contradicts the project's ₹0/month operating
principle (§10 of the project brief). This is a deliberate, documented
trade-off — streaming infrastructure is not trivially free-hostable, and the
demo strategy below is designed around it.

## The three demo surfaces

1. **90-second video** (linked from the README): live Grafana dashboards, an
   anomaly injected on camera, detection firing within ~1 s, the ensemble
   corroborating, the Discord alert arriving.
2. **Annotated screenshots** in the README (Market Overview, Anomaly Feed,
   Pipeline Health, ML Performance).
3. **One-command local reproduction** — anyone with Docker:

   ```bash
   git clone https://github.com/ayushgupta07xx/StreamPulse-NSE && cd StreamPulse-NSE
   make up          # full stack, ~3 min first time (image pulls)
   make flink-jobs  # submit the 3 streaming jobs
   make demo        # 5-minute live stream + a visible TCS spike
   # watch http://localhost:23000 (admin/admin) -> Anomaly Feed
   ```

## The 90-second script

> "This is StreamPulse — real-time anomaly detection for Indian equity
> markets. The generator is streaming live ticks into Kafka; Flink consumes
> them with exactly-once semantics, builds OHLCV bars, and runs four anomaly
> detectors; everything lands in ClickHouse and renders here in Grafana at
> sub-second refresh.
>
> I'm injecting a 4% spike into TCS — now. [run `make demo` beforehand;
> point at the moment the spike hits] Within a second, the Z-score detector
> fires at four-plus sigma — there's the red banner. A few seconds later the
> EWMA control chart confirms it with a Western Electric rule violation, and
> the Isolation Forest agrees from the five-minute bar features. Multiple
> methods firing means high ensemble severity — which routes through
> Alertmanager to Discord. [show the Discord ping]
>
> The Pipeline Health board shows what's under the hood: consumer lag near
> zero, Flink checkpointing every ten seconds, ClickHouse answering in
> milliseconds. The whole platform also deploys to Kubernetes with one Helm
> command, has a parallel AWS pipeline on Kinesis and Lambda, and ran a GCP
> cycle on Pub/Sub and Dataflow — all Terraform, all in the repo. Exactly-once
> isn't a slogan here: kill a TaskManager mid-stream and the audit shows zero
> duplicates, zero gaps."

## Recording checklist (Day 14)

- [ ] `make up && make flink-jobs`, dashboards open, Discord visible
- [ ] OBS/screen recorder at 1080p; Grafana in kiosk mode (`&kiosk`)
- [ ] `make demo` started ~1 min before recording (warm-up for detectors)
- [ ] One take, ≤95 s; upload; link in README hero
