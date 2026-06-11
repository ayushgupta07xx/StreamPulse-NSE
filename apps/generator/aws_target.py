"""Alternate generator sink: Amazon Kinesis Data Streams (Day 11, LocalStack).

Mirrors kafka_sink.TickSink with the Kinesis API — partition key = ticker
(same per-key ordering semantics as the Kafka path).

Env:
    AWS_ENDPOINT_URL=http://localhost:4566   (LocalStack; unset for real AWS)
"""

from __future__ import annotations

import json
import logging
import os

import boto3

from generator import metrics
from generator.schemas import Tick

log = logging.getLogger(__name__)

_BATCH_MAX = 500  # Kinesis PutRecords hard limit


class KinesisSink:
    """Batched PutRecords producer keyed by ticker."""

    def __init__(self, stream_name: str = "nse-ticks-raw") -> None:
        self.stream = stream_name
        self.client = boto3.client(
            "kinesis",
            endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566"),
            region_name=os.environ.get("AWS_REGION", "ap-south-1"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        )
        self._batch: list[dict] = []

    def send(self, tick: Tick) -> None:
        self._batch.append(
            {
                "Data": tick.to_json_bytes(),
                "PartitionKey": tick.ticker,
            }
        )
        if len(self._batch) >= _BATCH_MAX:
            self.flush()

    def flush(self) -> None:
        while self._batch:
            chunk, self._batch = self._batch[:_BATCH_MAX], self._batch[_BATCH_MAX:]
            resp = self.client.put_records(StreamName=self.stream, Records=chunk)
            failed = resp.get("FailedRecordCount", 0)
            ok = len(chunk) - failed
            for r in chunk[:ok]:
                metrics.MESSAGES_PRODUCED.labels(
                    ticker=json.loads(r["Data"])["ticker"]
                ).inc()
            if failed:
                metrics.DELIVERY_ERRORS.inc(failed)
                # retry transient throttles once
                retry = [
                    r
                    for r, res in zip(chunk, resp["Records"])
                    if "ErrorCode" in res
                ]
                log.warning("kinesis: %d failed records, retrying once", failed)
                self.client.put_records(StreamName=self.stream, Records=retry)
