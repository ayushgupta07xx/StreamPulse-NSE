# StreamPulse GCP demonstration cycle (Day 12, §11):
# Pub/Sub → Dataflow (Apache Beam streaming) → BigQuery.
# Spin up ~3h, push 30 min of ticks, screenshot, terraform destroy. $3-5 total.

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.8"
    }
  }
}

# ── Pub/Sub: Kafka-equivalent ────────────────────────────────────────────
resource "google_pubsub_topic" "ticks" {
  name = var.topic_name
}

resource "google_pubsub_subscription" "ticks_dataflow" {
  name  = "${var.topic_name}-dataflow"
  topic = google_pubsub_topic.ticks.id

  ack_deadline_seconds       = 60
  message_retention_duration = "3600s"
}

# ── GCS: Dataflow temp/staging ───────────────────────────────────────────
resource "google_storage_bucket" "dataflow_temp" {
  name                        = var.temp_bucket
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 1 }
    action { type = "Delete" }
  }
}

# ── BigQuery: sink ───────────────────────────────────────────────────────
resource "google_bigquery_dataset" "streampulse" {
  dataset_id    = var.dataset_id
  location      = var.region
  friendly_name = "StreamPulse NSE demo"

  # demo dataset: destroy cleanly even with data
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "ticks" {
  dataset_id          = google_bigquery_dataset.streampulse.dataset_id
  table_id            = "ticks_clean"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "event_ts"
  }

  schema = jsonencode([
    { name = "ticker", type = "STRING", mode = "REQUIRED" },
    { name = "event_ts", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "price", type = "FLOAT64", mode = "REQUIRED" },
    { name = "volume", type = "INT64", mode = "REQUIRED" },
    { name = "side", type = "STRING", mode = "NULLABLE" },
    { name = "sector", type = "STRING", mode = "NULLABLE" },
    { name = "session_id", type = "STRING", mode = "NULLABLE" },
  ])
}

# ── Dataflow: Beam streaming job ─────────────────────────────────────────
# The Beam pipeline (apps/beam/pubsub_to_bigquery.py) is launched via
# `python -m apps.beam.pubsub_to_bigquery --runner DataflowRunner ...` on Day
# 12 — Terraform provisions the infrastructure; the job submission is part of
# the demo script so its lifecycle (run 30 min, drain, destroy) stays explicit.
