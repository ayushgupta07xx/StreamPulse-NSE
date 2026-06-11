# Day 12 GCP demo environment — exists for ~3 hours, then destroyed (§11).
# terraform apply -var project_id=... -var temp_bucket=...

terraform {
  required_version = ">= 1.6"
}

provider "google" {
  project = var.project_id
  region  = "asia-south1"
}

variable "project_id" {
  type = string
}

variable "temp_bucket" {
  type = string
}

module "gcp_pipeline" {
  source      = "../../modules/gcp"
  project_id  = var.project_id
  temp_bucket = var.temp_bucket
}

output "pubsub_topic" {
  value = module.gcp_pipeline.pubsub_topic
}

output "pubsub_subscription" {
  value = module.gcp_pipeline.pubsub_subscription
}

output "bigquery_table" {
  value = module.gcp_pipeline.bigquery_table
}
