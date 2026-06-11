# Runbook: Flink job failure / restart loop

**Symptoms:** `FlinkJobRestarting` or `FlinkCheckpointFailure` alert; job state
`RESTARTING`/`FAILED` in the Flink UI (compose: http://localhost:28088, k8s:
`kubectl port-forward svc/streampulse-flink-jobmanager 28088:8081`).

## 1. Triage — what is failing?

```bash
# job states
curl -s http://localhost:28088/jobs/overview | python -m json.tool

# root cause of the most recent failover
curl -s http://localhost:28088/jobs/<JOB_ID>/exceptions | python -m json.tool | head -60
```

Common signatures seen in practice:

| Signature in exception | Cause | Fix section |
|---|---|---|
| `NoResourceAvailableException` | TaskManagers dead/missing slots | §2 |
| `Failed to create checkpoint storage` | checkpoint dir not writable | §3 |
| `ClassCastException: [B → String` | Python operator missing `output_type=Types.STRING()` before a Java sink | code fix (ADR-007) |
| `Failed to deserialize consumer record` | upstream wrote non-JSON into the topic | inspect topic with `rpk topic consume` |
| Exit 143 / 239 on TaskManager | OOM kill (VM memory pressure) | §2 |

## 2. TaskManagers down / no slots

```bash
docker ps -a --filter "name=taskmanager"          # compose
kubectl get pods -l app.kubernetes.io/component=taskmanager   # k8s
curl -s http://localhost:28088/taskmanagers       # registered TMs + free slots
```

- Compose: `docker compose up -d --force-recreate flink-taskmanager`.
- k8s: pods self-heal; if CrashLoopBackOff, check `kubectl logs` — JVM OOM →
  lower `taskmanager.memory.process.size` or raise container limits.
- After TMs register, jobs in `RESTARTING` recover **automatically from the
  last checkpoint** — verified: exactly-once holds across TM SIGKILL (zero
  dups/gaps over 74k-tick session; see docs/streaming-deep-dive.md).

## 3. Checkpoint storage failure

The checkpoint volume must be writable by uid `flink`:

```bash
docker exec -u root streampulse-flink-jm chown -R flink:flink /opt/flink/checkpoints
```

(The image pre-creates the dir with correct ownership; this only recurs if the
volume was created by an older image.)

## 4. Resubmit a cancelled/failed job

```bash
docker exec streampulse-flink-jm flink run -py /opt/streampulse/flink/jobs/<job>.py \
  --pyFiles /opt/streampulse/flink/jobs -d [--ooo-seconds N] [--idle-seconds 0]
```

The Kafka source resumes from the group's committed offsets (committed on
checkpoint), and the transactional sink discards any uncommitted output from
the failed attempt — end-to-end exactly-once is preserved across manual
resubmission.

## 5. Full event-time reset (replay sessions colliding)

Replaying the same trading date into existing topics merges into the same
event-time windows. For a pristine state:

```bash
python scripts/reset_pipeline.py [--ooo-seconds 360 --idle-seconds 0]
```

This cancels jobs, wipes `nse.*` topics + consumer groups, truncates the
ClickHouse tables, recreates topics, and resubmits jobs.
