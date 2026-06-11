# GCP Demonstration Cycle (Pub/Sub → Dataflow → BigQuery)

GCP is a **one-time, ~3-hour architectural demonstration** (§11, Day 12), not
ongoing infrastructure. Expected total spend: **$3–5**, dominated by one
n1-standard-2 Dataflow worker for ~2 hours. Everything is provisioned by
Terraform and destroyed the same day.

## Architecture

```
generator --target pubsub ──▶ Pub/Sub topic nse-ticks
                                   │ subscription nse-ticks-dataflow
                                   ▼
                     Dataflow (Apache Beam Python, streaming)
                     ParseValidateEnrich DoFn  [apps/beam/pubsub_to_bigquery.py]
                                   ▼
                     BigQuery streampulse.ticks_clean (day-partitioned)
```

| Concern | Local stack | GCP equivalent |
|---|---|---|
| Bus | Redpanda/Kafka | **Cloud Pub/Sub** |
| Stream processor | Flink (PyFlink) | **Cloud Dataflow (Apache Beam)** |
| Analytical store | ClickHouse | **BigQuery** |

## Per-cycle protocol (§11 — follow exactly)

1. **Pre-flight:** verify credit balance; budget alerts at $50/$100 active.
2. Provision (~5 min):
   ```bash
   cd infra/terraform/envs/demo
   terraform init
   terraform apply -auto-approve \
     -var project_id=$PROJECT -var temp_bucket=$PROJECT-streampulse-tmp
   ```
3. Launch the Beam job on Dataflow (1 worker, capped):
   ```bash
   pip install "apache-beam[gcp]"
   python -m apps.beam.pubsub_to_bigquery \
     --runner DataflowRunner --project $PROJECT --region asia-south1 \
     --subscription projects/$PROJECT/subscriptions/nse-ticks-dataflow \
     --output-table $PROJECT:streampulse.ticks_clean \
     --temp_location gs://$PROJECT-streampulse-tmp/tmp \
     --streaming --max_num_workers 1 --machine_type n1-standard-2
   ```
4. Push ~30 minutes of ticks at 10×:
   ```bash
   GCP_PROJECT=$PROJECT PYTHONPATH=apps python -m generator.main run \
     --speed 10 --duration-s 1800 --target pubsub
   ```
5. Verify in BigQuery:
   ```sql
   SELECT ticker, count(*) ticks, min(event_ts), max(event_ts)
   FROM `streampulse.ticks_clean` GROUP BY ticker ORDER BY ticks DESC LIMIT 10;
   ```
6. **Screenshots** → `docs/images/gcp/`: Dataflow job graph, Pub/Sub
   throughput chart, BigQuery results.
7. Drain the Dataflow job (console → Drain), then:
   ```bash
   terraform destroy -auto-approve -var project_id=$PROJECT -var temp_bucket=...
   ```
8. **Verify empty:** `gcloud dataflow jobs list --status active`,
   `gcloud pubsub topics list`, billing dashboard the next day.

## Cost controls baked in

- `max_num_workers 1`, smallest practical machine type.
- BigQuery dataset `delete_contents_on_destroy = true`; GCS bucket
  `force_destroy = true` + 1-day lifecycle — `terraform destroy` is total.
- The demo pushes ~900K messages ≈ 0.2 GB through Pub/Sub (free tier: 10 GB).
- BigQuery storage/query volumes are deep inside the free tier.

## Status

- [ ] Cycle executed (Day 12 — requires the user's billing-enabled project)
- [x] Terraform module ready (`infra/terraform/modules/gcp/`)
- [x] Beam pipeline ready (`apps/beam/pubsub_to_bigquery.py`)
- [x] Generator Pub/Sub target ready (`--target pubsub`)
