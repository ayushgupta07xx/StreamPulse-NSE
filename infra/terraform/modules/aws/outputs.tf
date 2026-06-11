output "kinesis_stream_arn" {
  value = aws_kinesis_stream.ticks_raw.arn
}

output "archive_bucket" {
  value = aws_s3_bucket.archive.bucket
}

output "state_table" {
  value = aws_dynamodb_table.state.name
}

output "lambda_function" {
  value = aws_lambda_function.validate_enrich.function_name
}
