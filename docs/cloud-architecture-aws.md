# AWS Parallel Pipeline (via LocalStack)

StreamPulse's AWS demonstration mirrors the Kafka/Flink pipeline with managed
AWS services, exercised end-to-end against **LocalStack Community Edition** —
a free, open-source AWS emulator running in Docker. **Zero AWS spend.**

> AWS architecture is demonstrated via LocalStack (a free OSS emulator). The
> Terraform code in `infra/terraform/modules/aws/` is unchanged for real AWS —
> clear the endpoint override and it deploys to production. LocalStack is the
> standard development pattern for AWS-targeting teams that don't want a
> corporate-card bill during portfolio work. This framing is deliberate
> cost-consciousness, not a limitation of the code.

## Service mapping

| StreamPulse (local) | AWS equivalent | Terraform resource |
|---|---|---|
| Kafka topic `nse.ticks.raw` | **Kinesis Data Streams** (4 shards, 24h) | `aws_kinesis_stream.ticks_raw` |
| Flink validate/enrich job | **Lambda** (Kinesis-triggered, batch 500) | `aws_lambda_function.validate_enrich` |
| ClickHouse tick store | **S3** archive (JSONL, date-partitioned, 30d lifecycle) | `aws_s3_bucket.archive` |
| Flink keyed state | **DynamoDB** (`ticker` + `state_key`) | `aws_dynamodb_table.state` |
| ClickHouse schema | **Glue** catalog over the S3 archive | `aws_glue_catalog_table.ticks_archive` |

The generator's `--target kinesis` path (`apps/generator/aws_target.py`) uses
`PutRecords` batches with `PartitionKey=ticker` — the same per-key ordering
contract as the Kafka path.

## Running it

```bash
# 1. LocalStack
docker run -d --name localstack -p 4566:4566 localstack/localstack

# 2. Provision (identical code path as real AWS)
cd infra/terraform/envs/local
terraform init && terraform apply -auto-approve

# 3. Stream ticks into Kinesis
PYTHONPATH=apps python -m generator.main run --speed 10 --duration-s 60 --target kinesis

# 4. Inspect
aws --endpoint-url http://localhost:4566 s3 ls s3://streampulse-archive/raw/ --recursive | head
aws --endpoint-url http://localhost:4566 dynamodb scan --table-name streampulse-state --max-items 5
```

## Retargeting real AWS

1. Remove/blank the `endpoints` block in `envs/local/main.tf` (or use a
   separate `envs/aws` with real credentials).
2. Set `localstack_mode = false` on the module — this enables the two
   resources LocalStack Community cannot emulate (measured on 3.8, not
   assumed): the Glue catalog returns 501 (Pro-only feature) and the S3
   lifecycle configuration's read-back never converges, timing out the
   provider after 3 minutes. Both are still `terraform validate`d in CI.
3. `terraform apply`. Same module, same resources; S3 bucket names must be
   globally unique (override `var.archive_bucket`).

Everything actually exercised in the local cycle — Kinesis, S3, Lambda +
event source mapping, DynamoDB, IAM, CloudWatch Logs — runs on LocalStack
Community at $0.
