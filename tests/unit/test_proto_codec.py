"""Round-trip: generator protobuf framing → Flink-side Confluent decode.

Proves the two independently-written halves of the Phase 2 path agree on the
wire format, including the latin-1 carrier trick the PyFlink source uses
(SimpleStringSchema('ISO-8859-1') — see kafka_bytes_source).
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "apps" / "flink" / "jobs"))

from common.proto_codec import split_confluent_frame, tick_from_confluent  # noqa: E402
from generator.proto_format import ProtobufTickSerializer  # noqa: E402
from generator.schemas import Tick  # noqa: E402

IST = UTC  # tz value irrelevant; isoformat round-trip is what matters

TICK = Tick(
    ticker="RELIANCE",
    timestamp_ist=datetime(2026, 6, 11, 9, 15, 0, tzinfo=IST),
    price=2891.55,
    volume=1200,
    side="BUY",
    session_id="sess-test",
    seq=42,
)


def test_frame_layout() -> None:
    framed = ProtobufTickSerializer(schema_id=7)(TICK)
    assert framed[0] == 0x00  # magic
    schema_id, msg_bytes = split_confluent_frame(framed)
    assert schema_id == 7
    assert len(msg_bytes) == len(framed) - 6  # header is exactly 6 bytes for index [0]


def test_roundtrip_through_latin1_carrier() -> None:
    framed = ProtobufTickSerializer(schema_id=1)(TICK)
    # what PyFlink does: Java decodes bytes as ISO-8859-1 → Python str
    carrier = framed.decode("latin-1")
    # what the job does: recover bytes, decode protobuf
    tick = tick_from_confluent(carrier.encode("latin-1"))
    assert tick["ticker"] == "RELIANCE"
    assert tick["price"] == 2891.55
    assert tick["volume"] == 1200
    assert tick["side"] == "BUY"
    assert tick["seq"] == 42
    assert tick["exchange"] == "NSE"
    assert tick["timestamp_ist"] == TICK.timestamp_ist.isoformat()
    assert "sector" not in tick  # enrichment fields absent on raw ticks


def test_invalid_frames_rejected() -> None:
    import pytest

    with pytest.raises(ValueError):
        tick_from_confluent(b'{"ticker": "json, not protobuf"}')
    with pytest.raises(ValueError):
        tick_from_confluent(b"\x00\x00")  # too short
