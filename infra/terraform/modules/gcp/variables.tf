variable "project_id" {
  description = "GCP project (billing-enabled; Day 12 demo cycle only)"
  type        = string
}

variable "region" {
  type    = string
  default = "asia-south1" # Mumbai
}

variable "topic_name" {
  type    = string
  default = "nse-ticks"
}

variable "dataset_id" {
  type    = string
  default = "streampulse"
}

variable "dataflow_max_workers" {
  type    = number
  default = 1 # cost guard: single n1-standard-2 worker, ~$1-2/hr
}

variable "temp_bucket" {
  description = "GCS bucket for Dataflow temp/staging"
  type        = string
}
