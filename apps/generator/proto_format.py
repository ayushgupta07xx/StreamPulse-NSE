"""Phase 2 producer path: Confluent-framed protobuf ticks (brief §20).

JSON stays the default (--format json); this serializer activates behind
--format protobuf. The schema id is fetched from (or registered with)
Redpanda's Confluent-compatible registry at startup, so frames always carry
the registry's real id — any Confluent/Redpanda consumer can resolve them.

Frame layout: 0x00 magic + 4-byte big-endian schema id + 0x00 (message-index
array [0], zigzag-varint single-byte form) + serialized message.
"""

from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path

from generator.pb import tick_pb2
from generator.schemas import Tick

REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:28081")
_TICK_PROTO = Path(__file__).resolve().parents[2] / "schemas" / "protobuf" / "tick.proto"
# generated module has no stubs — attribute access is checked by the tests
_SIDE_VALUES = {"BUY": tick_pb2.Tick.BUY, "SELL": tick_pb2.Tick.SELL}  # type: ignore[attr-defined]


def get_or_register_schema_id(subject: str = "nse.ticks.raw-value") -> int:
    """Idempotent register-and-fetch against the Confluent-compatible API."""
    body = json.dumps(
        {"schemaType": "PROTOBUF", "schema": _TICK_PROTO.read_text(encoding="utf-8")}
    ).encode()
    req = urllib.request.Request(
        f"{REGISTRY_URL}/subjects/{subject}/versions",
        data=body,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return int(json.load(resp)["id"])


class ProtobufTickSerializer:
    """Tick (pydantic) → Confluent-framed protobuf bytes."""

    def __init__(self, schema_id: int | None = None) -> None:
        self.schema_id = get_or_register_schema_id() if schema_id is None else schema_id
        self._header = b"\x00" + self.schema_id.to_bytes(4, "big") + b"\x00"

    def __call__(self, tick: Tick) -> bytes:
        msg = tick_pb2.Tick(  # type: ignore[attr-defined]
            ticker=tick.ticker,
            timestamp_ist=tick.timestamp_ist.isoformat(),
            price=tick.price,
            volume=tick.volume,
            side=_SIDE_VALUES[tick.side],
            exchange=tick.exchange,
            session_id=tick.session_id,
            seq=tick.seq,
        )
        return self._header + msg.SerializeToString()
