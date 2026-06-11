"""Register the Phase 2 protobuf schemas with Redpanda's schema registry.

Redpanda ships a Confluent-compatible registry on the schema-registry port
(host 28081). One schema per message family, registered under every topic's
``<topic>-value`` subject. Idempotent: re-posting an identical schema returns
the existing id.

Usage:
    python scripts/register_schemas.py [--registry http://localhost:28081]
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "schemas" / "protobuf"

SUBJECTS = {
    "nse.ticks.raw-value": "tick.proto",
    "nse.ticks.clean-value": "tick.proto",
    "nse.bars.1m-value": "bar.proto",
    "nse.bars.5m-value": "bar.proto",
    "nse.bars.15m-value": "bar.proto",
    "nse.bars.late-value": "bar.proto",
    "nse.bars.session-value": "bar.proto",
    "nse.anomalies-value": "anomaly.proto",
}


def register(registry: str, subject: str, proto_file: str) -> int:
    schema = (PROTO_DIR / proto_file).read_text(encoding="utf-8")
    body = json.dumps({"schemaType": "PROTOBUF", "schema": schema}).encode()
    req = urllib.request.Request(
        f"{registry}/subjects/{subject}/versions",
        data=body,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)["id"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--registry", default="http://localhost:28081")
    args = ap.parse_args()

    for subject, proto_file in SUBJECTS.items():
        schema_id = register(args.registry, subject, proto_file)
        print(f"{subject:28s} <- {proto_file:14s} id={schema_id}")

    with urllib.request.urlopen(f"{args.registry}/subjects", timeout=10) as resp:
        print("registry subjects:", sorted(json.load(resp)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
