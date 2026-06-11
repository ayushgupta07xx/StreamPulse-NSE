"""Apache Beam streaming pipeline: Pub/Sub → validate/enrich → BigQuery.

The GCP-flavored equivalent of the Flink validate_enrich job (Day 12 §11).
Runs on Cloud Dataflow for the ~3-hour demonstration cycle, or locally with
DirectRunner for smoke tests.

Dataflow (Day 12):
    python -m apps.beam.pubsub_to_bigquery \
        --runner DataflowRunner --project $PROJECT --region asia-south1 \
        --subscription projects/$PROJECT/subscriptions/nse-ticks-dataflow \
        --output-table $PROJECT:streampulse.ticks_clean \
        --temp_location gs://$TEMP_BUCKET/tmp --streaming \
        --max_num_workers 1 --machine_type n1-standard-2

Local smoke:
    python -m apps.beam.pubsub_to_bigquery --runner DirectRunner ...
"""

from __future__ import annotations

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

REQUIRED = ("ticker", "timestamp_ist", "price", "volume", "side")

# Compact sector map (full enrichment lives in the Flink path; Dataflow demo
# uses the top-of-index subset, mirroring the Lambda variant)
SECTORS = {
    "RELIANCE": "Energy",
    "TCS": "Information Technology",
    "HDFCBANK": "Financials",
    "ICICIBANK": "Financials",
    "INFY": "Information Technology",
    "BHARTIARTL": "Telecom",
    "ITC": "Consumer Staples",
    "SBIN": "Financials",
    "LT": "Industrials",
    "HINDUNILVR": "Consumer Staples",
}


class ParseValidateEnrich(beam.DoFn):
    def process(self, message: bytes):
        try:
            tick = json.loads(message.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            beam.metrics.Metrics.counter("streampulse", "rejected").inc()
            return
        if (
            any(f not in tick for f in REQUIRED)
            or float(tick["price"]) <= 0
            or int(tick["volume"]) < 0
            or tick["side"] not in ("BUY", "SELL")
        ):
            beam.metrics.Metrics.counter("streampulse", "rejected").inc()
            return
        beam.metrics.Metrics.counter("streampulse", "accepted").inc()
        yield {
            "ticker": tick["ticker"],
            "event_ts": tick["timestamp_ist"],
            "price": float(tick["price"]),
            "volume": int(tick["volume"]),
            "side": tick["side"],
            "sector": SECTORS.get(tick["ticker"], "Other"),
            "session_id": tick.get("session_id", ""),
        }


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--output-table", required=True, help="project:dataset.table")
    known, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=known.subscription)
            | "ValidateEnrich" >> beam.ParDo(ParseValidateEnrich())
            | "WriteBigQuery"
            >> beam.io.WriteToBigQuery(
                known.output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
