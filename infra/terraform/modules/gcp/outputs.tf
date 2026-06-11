output "pubsub_topic" {
  value = google_pubsub_topic.ticks.id
}

output "pubsub_subscription" {
  value = google_pubsub_subscription.ticks_dataflow.id
}

output "bigquery_table" {
  value = "${google_bigquery_dataset.streampulse.dataset_id}.${google_bigquery_table.ticks.table_id}"
}

output "dataflow_temp_bucket" {
  value = google_storage_bucket.dataflow_temp.url
}
