# Local environment: AWS module against LocalStack ($0).
# Usage:  cd infra/terraform/envs/local && terraform init && terraform apply

terraform {
  required_version = ">= 1.6"
}

provider "aws" {
  region                      = "ap-south-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    kinesis  = var.aws_endpoint
    s3       = var.aws_endpoint
    lambda   = var.aws_endpoint
    dynamodb = var.aws_endpoint
    iam      = var.aws_endpoint
    glue     = var.aws_endpoint
    logs     = var.aws_endpoint
    sts      = var.aws_endpoint
  }
}

variable "aws_endpoint" {
  description = "LocalStack endpoint; point at real AWS by removing the endpoints block"
  type        = string
  default     = "http://localhost:4566"
}

module "aws_pipeline" {
  source       = "../../modules/aws"
  aws_endpoint = var.aws_endpoint
}

output "kinesis_stream_arn" {
  value = module.aws_pipeline.kinesis_stream_arn
}

output "archive_bucket" {
  value = module.aws_pipeline.archive_bucket
}
