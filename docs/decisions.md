# Architecture Decision Records

One entry per consequential choice. Newest at the bottom. When resuming this
project in a fresh session: read the brief (`E:\Analyst files\04_StreamPulseNSE.md`),
then this file, then `docs/PROGRESS.md`, then `git log`.

---

## ADR-001: Redpanda instead of Apache Kafka

**Date:** 2026-06-11 · **Status:** accepted

Single-binary, KRaft-native, Kafka-API-compatible, no ZooKeeper/JVM, built-in
schema registry and Prometheus endpoint. Dramatically lighter on a 16 GB laptop
than a Kafka+ZK+Registry stack. Every client speaks the Kafka protocol to it, so
the "Apache Kafka" skill transfer is 1:1. Pinned `v24.2.18`.

## ADR-002: Built-in Redpanda schema registry, not Apicurio

**Date:** 2026-06-11 · **Status:** accepted

Redpanda ships a Confluent-compatible schema registry on port 8081 — one fewer
container. Protobuf schema work (Day 13) targets this registry.

## ADR-003: PyFlink runs only inside the Flink image

**Date:** 2026-06-11 · **Status:** accepted

PyFlink 1.18 supports Python ≤3.10; the host has 3.11/3.14. Rather than fight
version skew, `docker/flink/Dockerfile` extends `flink:1.18.1` with Ubuntu jammy's
Python 3.10 + `apache-flink==1.18.1`. Jobs are bind-mounted at
`/opt/streampulse/flink` and submitted via `flink run -py`. Host Python (3.11 via
Poetry) is used for the generator, ML batch jobs, and tests — none of which need
PyFlink. Consequence: `apache-flink` is intentionally absent from pyproject.toml.

## ADR-004: Secrets via .env only; Alertmanager config is templated

**Date:** 2026-06-11 · **Status:** accepted

The repo is public. The Discord webhook URL lives in `.env` (gitignored); the
Alertmanager container substitutes it into `alertmanager.tmpl.yml` at startup via
a sed entrypoint (Alertmanager has no native env expansion). The same pattern will
apply to any future credential. `detect-private-key` pre-commit hook as backstop.

## ADR-005: Flink TaskManagers as a scaled service + DNS service discovery

**Date:** 2026-06-11 · **Status:** accepted

`deploy.replicas: 2` on one taskmanager service instead of two copy-pasted
services. Prometheus discovers all replicas via `dns_sd_configs` A-record lookup
on the compose network. Maps cleanly to the Day 7 Helm Deployment + headless
Service pattern.

## ADR-006: Host ports use a dedicated 2xxxx block

**Date:** 2026-06-11 · **Status:** accepted

The dev machine runs sibling project stacks (jobatlas, sentinelops, creatorpulse)
that already bind 19092, 18081-2, 8088, 9000-9001, and 3000. StreamPulse therefore
exposes: Kafka 29092, Schema Registry 28081, HTTP proxy 28082, Flink UI 28088,
ClickHouse native 29000, Grafana 23000. Unprefixed survivors: 8123 (CH HTTP),
9644, 9090, 9093, 5000, 8085, 8000. Internal container ports are unchanged, so
Helm/k8s manifests (Day 7) are unaffected.

## ADR-007: PyFlink workarounds (rough edges, §20 risk register)

**Date:** 2026-06-11 · **Status:** accepted

1. **Value-only Kafka sinks.** PyFlink's KafkaRecordSerializationSchema cannot
   derive a record key from an element field, so Flink-produced topics are not
   keyed by ticker. Per-ticker ordering downstream is re-established via
   event-time watermarks; Flink's own keyed operators are unaffected (key_by is
   internal). Java jobs could key — noted as the optional Java add-on.
2. **Static enrichment via open(), not broadcast state.** The Nifty 50 metadata
   CSV is immutable reference data; loading it in RichMapFunction.open() beats
   a broadcast stream in simplicity and is the documented pattern for static
   lookups in PyFlink.
3. **Pickled Python state.** Keyed detector state (deques, dicts) uses
   PICKLED_BYTE_ARRAY ValueState — idiomatic for PyFlink, and RocksDB handles
   persistence/incremental checkpoints transparently.
