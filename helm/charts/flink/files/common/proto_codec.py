"""Confluent-framed protobuf decoding (Phase 2 ingest path, brief §20).

Wire format (Confluent Schema Registry serde, Redpanda-compatible):

    byte  0      magic 0x00
    bytes 1-4    schema id, big-endian uint32
    then         message-index array: zigzag varints, length first.
                 The common single-message case [0] is encoded as one 0x00.
    rest         the protobuf message

PyFlink 1.18 exposes no byte-array deserializer, so the protobuf source path
reads Kafka values through SimpleStringSchema('ISO-8859-1'): latin-1 maps
bytes 0-255 to codepoints 1:1, making str.encode('latin-1') a lossless
recovery of the original payload (see kafka_bytes_source in pipeline.py).

Runs on the Flink image's Python 3.10 + protobuf 4.23 — stdlib + protobuf only.
"""

from __future__ import annotations

from common.pb import tick_pb2

_SIDE_NAMES = {1: "BUY", 2: "SELL"}


def _read_zigzag_varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Kafka ByteUtils-style zigzag varint → (value, next_pos)."""
    shift = 0
    raw = 0
    while True:
        byte = buf[pos]
        pos += 1
        raw |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    return (raw >> 1) ^ -(raw & 1), pos


def split_confluent_frame(payload: bytes) -> tuple[int, bytes]:
    """(schema_id, message_bytes) from a Confluent-framed record value."""
    if len(payload) < 6 or payload[0] != 0:
        raise ValueError("not a Confluent-framed payload")
    schema_id = int.from_bytes(payload[1:5], "big")
    count, pos = _read_zigzag_varint(payload, 5)
    for _ in range(count):  # skip nested message indexes (we use index [0])
        _, pos = _read_zigzag_varint(payload, pos)
    return schema_id, payload[pos:]


def tick_from_confluent(payload: bytes) -> dict:
    """Framed protobuf Tick → dict in the exact shape of the JSON path."""
    _, msg_bytes = split_confluent_frame(payload)
    t = tick_pb2.Tick.FromString(msg_bytes)
    tick = {
        "ticker": t.ticker,
        "timestamp_ist": t.timestamp_ist,
        "price": t.price,
        "volume": t.volume,
        "side": _SIDE_NAMES.get(t.side, "UNSPECIFIED"),
        "exchange": t.exchange,
        "session_id": t.session_id,
        "seq": t.seq,
    }
    # enrichment fields ride along when present (clean-topic messages)
    for f in ("name", "sector", "industry", "mcap_bucket"):
        value = getattr(t, f)
        if value:
            tick[f] = value
    return tick
