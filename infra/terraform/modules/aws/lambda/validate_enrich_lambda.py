"""Lambda equivalent of the Flink validate/enrich job (simpler subset, Day 11).

Triggered by the Kinesis stream. For each tick batch:
1. validate (required fields, price > 0, volume >= 0, valid side)
2. enrich with sector metadata (compiled in — Lambda layers would carry the
   CSV at real scale)
3. archive valid ticks to S3 (JSONL, partitioned by date)
4. track last-seen price per ticker in DynamoDB (keyed-state stand-in)
"""

from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone

import boto3

ARCHIVE_BUCKET = os.environ["ARCHIVE_BUCKET"]
STATE_TABLE = os.environ["STATE_TABLE"]

# Endpoint override lets the same code run inside LocalStack's Lambda runtime
_endpoint = os.environ.get("AWS_ENDPOINT_URL") or None
s3 = boto3.client("s3", endpoint_url=_endpoint)
dynamodb = boto3.resource("dynamodb", endpoint_url=_endpoint)

# Top-of-index subset; the Flink path enriches all 50 from the CSV
SECTORS = {
    "RELIANCE": "Energy", "TCS": "Information Technology", "HDFCBANK": "Financials",
    "ICICIBANK": "Financials", "INFY": "Information Technology", "BHARTIARTL": "Telecom",
    "ITC": "Consumer Staples", "SBIN": "Financials", "LT": "Industrials",
    "HINDUNILVR": "Consumer Staples",
}

REQUIRED = ("ticker", "timestamp_ist", "price", "volume", "side")


def handler(event, context):
    valid, rejected = [], 0
    table = dynamodb.Table(STATE_TABLE)

    for record in event.get("Records", []):
        try:
            tick = json.loads(base64.b64decode(record["kinesis"]["data"]))
        except (KeyError, ValueError):
            rejected += 1
            continue

        if (
            any(f not in tick for f in REQUIRED)
            or float(tick["price"]) <= 0
            or int(tick["volume"]) < 0
            or tick["side"] not in ("BUY", "SELL")
        ):
            rejected += 1
            continue

        tick["sector"] = SECTORS.get(tick["ticker"], "Other")
        valid.append(tick)

        table.put_item(
            Item={
                "ticker": tick["ticker"],
                "state_key": "last_price",
                "price": str(tick["price"]),
                "ts": tick["timestamp_ist"],
            }
        )

    if valid:
        now = datetime.now(timezone.utc)
        key = f"raw/{now:%Y-%m-%d}/{context.aws_request_id}.jsonl"
        body = "\n".join(json.dumps(t) for t in valid)
        s3.put_object(Bucket=ARCHIVE_BUCKET, Key=key, Body=body.encode())

    return {"valid": len(valid), "rejected": rejected}
