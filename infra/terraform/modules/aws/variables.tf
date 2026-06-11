variable "aws_endpoint" {
  description = "AWS API endpoint. LocalStack default; set to null (or override per-service) for real AWS — the module code is unchanged."
  type        = string
  default     = "http://localhost:4566"
}

variable "region" {
  type    = string
  default = "ap-south-1" # Mumbai — matches the NSE domain
}

variable "kinesis_stream_name" {
  type    = string
  default = "nse-ticks-raw"
}

variable "kinesis_shard_count" {
  type    = number
  default = 4
}

variable "archive_bucket" {
  type    = string
  default = "streampulse-archive"
}

variable "state_table" {
  type    = string
  default = "streampulse-state"
}
