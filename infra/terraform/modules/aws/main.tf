# StreamPulse AWS parallel pipeline (Day 11).
# Runs against LocalStack by default ($0); identical code targets real AWS by
# clearing var.aws_endpoint (see docs/cloud-architecture-aws.md).

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.6"
    }
  }
}

# ── Kinesis: Kafka-equivalent ingestion ──────────────────────────────────
resource "aws_kinesis_stream" "ticks_raw" {
  name             = var.kinesis_stream_name
  shard_count      = var.kinesis_shard_count
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = { project = "streampulse" }
}

# ── S3: raw tick archive ─────────────────────────────────────────────────
resource "aws_s3_bucket" "archive" {
  bucket        = var.archive_bucket
  force_destroy = true
  tags          = { project = "streampulse" }
}

resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    id     = "expire-raw-ticks"
    status = "Enabled"
    filter { prefix = "raw/" }
    expiration { days = 30 }
  }
}

# ── DynamoDB: keyed state store ──────────────────────────────────────────
resource "aws_dynamodb_table" "state" {
  name         = var.state_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "ticker"
  range_key    = "state_key"

  attribute {
    name = "ticker"
    type = "S"
  }
  attribute {
    name = "state_key"
    type = "S"
  }

  tags = { project = "streampulse" }
}

# ── Lambda: validate/enrich (Kinesis-triggered) ──────────────────────────
data "archive_file" "validate_lambda" {
  type        = "zip"
  source_file = "${path.module}/lambda/validate_enrich_lambda.py"
  output_path = "${path.module}/lambda/validate_enrich_lambda.zip"
}

resource "aws_iam_role" "lambda" {
  name = "streampulse-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name = "streampulse-lambda-policy"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:ListStreams"]
        Resource = aws_kinesis_stream.ticks_raw.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${aws_s3_bucket.archive.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:UpdateItem"]
        Resource = aws_dynamodb_table.state.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_lambda_function" "validate_enrich" {
  function_name    = "streampulse-validate-enrich"
  filename         = data.archive_file.validate_lambda.output_path
  source_code_hash = data.archive_file.validate_lambda.output_base64sha256
  handler          = "validate_enrich_lambda.handler"
  runtime          = "python3.11"
  role             = aws_iam_role.lambda.arn
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      ARCHIVE_BUCKET = aws_s3_bucket.archive.bucket
      STATE_TABLE    = aws_dynamodb_table.state.name
    }
  }
}

resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn  = aws_kinesis_stream.ticks_raw.arn
  function_name     = aws_lambda_function.validate_enrich.arn
  starting_position = "LATEST"
  batch_size        = 500
}

# ── Glue: catalog over the S3 archive (light usage) ──────────────────────
resource "aws_glue_catalog_database" "streampulse" {
  name = "streampulse"
}

resource "aws_glue_catalog_table" "ticks_archive" {
  database_name = aws_glue_catalog_database.streampulse.name
  name          = "ticks_raw_archive"
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.archive.bucket}/raw/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "ticker"
      type = "string"
    }
    columns {
      name = "timestamp_ist"
      type = "string"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "volume"
      type = "bigint"
    }
    columns {
      name = "side"
      type = "string"
    }
  }
}
