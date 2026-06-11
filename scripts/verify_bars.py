"""Verify OHLCV bars produced by the window_bars Flink job.

Checks per bar topic (1m/5m/15m):
- window_start aligned to the bar-size boundary
- OHLC invariants: high >= max(open, close), low <= min(open, close), vwap within [low, high]
- interior bars carry exactly bar_seconds ticks (1 tick/sec/ticker generator);
  the first/last window of the replayed range may legitimately be partial

Usage:  python scripts/verify_bars.py [--bootstrap localhost:29092]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, TopicPartition

BAR_SECONDS = {"nse.bars.1m": 60, "nse.bars.5m": 300, "nse.bars.15m": 900}


def consume_all(bootstrap: str, topic: str) -> list[dict]:
    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"bar-verifier-{topic}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    meta = c.list_topics(topic, timeout=10)
    tps = [TopicPartition(topic, p) for p in meta.topics[topic].partitions]
    c.assign(tps)
    ends = {tp.partition: c.get_watermark_offsets(tp, timeout=10)[1] for tp in tps}
    out, done = [], {p for p, hi in ends.items() if hi == 0}
    while len(done) < len(tps):
        msg = c.poll(2.0)
        if msg is None:
            for tp in tps:
                if tp.partition not in done and c.position([tp])[0].offset >= ends[tp.partition]:
                    done.add(tp.partition)
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                done.add(msg.partition())
                continue
            raise RuntimeError(msg.error())
        out.append(json.loads(msg.value()))
        if msg.offset() + 1 >= ends[msg.partition()]:
            done.add(msg.partition())
    c.close()
    return out


def verify(topic: str, bars: list[dict]) -> bool:
    """Windows may legally fire multiple times under allowed lateness — each
    firing is a refinement superseding the last. Evaluate the FINAL refinement
    per (ticker, window), mirroring what ReplacingMergeTree keeps downstream."""
    span = BAR_SECONDS[topic]

    finals: dict[tuple, dict] = {}
    for b in bars:
        key = (b["ticker"], b["window_start"])
        if key not in finals or b["tick_count"] >= finals[key]["tick_count"]:
            finals[key] = b
    refinements = len(bars) - len(finals)

    misaligned = ohlc_bad = vwap_bad = 0
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for b in finals.values():
        by_ticker[b["ticker"]].append(b)
        ws = datetime.fromisoformat(b["window_start"])
        if (ws.minute * 60 + ws.second) % span != 0:
            misaligned += 1
        if not (b["high"] >= max(b["open"], b["close"]) and b["low"] <= min(b["open"], b["close"])):
            ohlc_bad += 1
        if not (b["low"] - 1e-6 <= b["vwap"] <= b["high"] + 1e-6):
            vwap_bad += 1

    # interior-window tick counts (drop first/last emitted window per ticker;
    # the trailing ~OOO-bound of event time is legitimately still open)
    bad_counts = total_interior = 0
    for tlist in by_ticker.values():
        tlist.sort(key=lambda b: b["window_start"])
        for b in tlist[1:-1]:
            total_interior += 1
            if b["tick_count"] != span:
                bad_counts += 1

    print(
        f"{topic}: firings={len(bars)} final_windows={len(finals)} refinements={refinements} "
        f"tickers={len(by_ticker)} misaligned={misaligned} ohlc_violations={ohlc_bad} "
        f"vwap_out_of_range={vwap_bad} interior_bad_tick_count={bad_counts}/{total_interior}"
    )
    return (
        len(finals) > 0
        and misaligned == 0
        and ohlc_bad == 0
        and vwap_bad == 0
        and bad_counts == 0
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:29092")
    args = ap.parse_args()
    ok = True
    for topic in BAR_SECONDS:
        ok &= verify(topic, consume_all(args.bootstrap, topic))
    print("BARS VERIFIED" if ok else "BAR VERIFICATION FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
