"""Exactly-once verification: consume all of nse.ticks.clean (read_committed)
and prove, per ticker within one generator session, that there are zero
duplicate and zero missing sequence numbers.

The generator stamps every tick with (session_id, ticker, seq) where seq is a
per-ticker monotonic counter starting at 0. After a mid-stream failure +
recovery, exactly-once holds iff for every ticker:
    distinct(seq) == count(seq) == max(seq) + 1

Usage:
    python scripts/verify_exactly_once.py --session-id sess-abc123 [--bootstrap localhost:29092]
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError, TopicPartition

TOPIC = "nse.ticks.clean"


def consume_all(bootstrap: str) -> list[dict]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "exactly-once-verifier",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    meta = consumer.list_topics(TOPIC, timeout=10)
    partitions = [TopicPartition(TOPIC, p) for p in meta.topics[TOPIC].partitions]
    consumer.assign(partitions)

    # snapshot high watermarks so we know when each partition is drained
    ends = {}
    for tp in partitions:
        _, hi = consumer.get_watermark_offsets(tp, timeout=10)
        ends[tp.partition] = hi

    out: list[dict] = []
    done: set[int] = {p for p, hi in ends.items() if hi == 0}
    while len(done) < len(partitions):
        msg = consumer.poll(2.0)
        if msg is None:
            # re-check: transactional control records can leave us just shy of hi
            for tp in partitions:
                if tp.partition in done:
                    continue
                pos = consumer.position([tp])[0].offset
                if pos >= ends[tp.partition]:
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
    consumer.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--session-id", required=True)
    ap.add_argument("--bootstrap", default="localhost:29092")
    args = ap.parse_args()

    print(f"consuming {TOPIC} (read_committed) ...", flush=True)
    msgs = consume_all(args.bootstrap)
    in_session = [m for m in msgs if m.get("session_id") == args.session_id]
    print(f"total messages: {len(msgs):,} | in session {args.session_id}: {len(in_session):,}")

    seqs: dict[str, list[int]] = defaultdict(list)
    for m in in_session:
        seqs[m["ticker"]].append(int(m["seq"]))

    dup_total = gap_total = 0
    for ticker, s in sorted(seqs.items()):
        n, uniq, expected = len(s), len(set(s)), max(s) + 1
        dups = n - uniq
        gaps = expected - uniq
        dup_total += dups
        gap_total += gaps
        if dups or gaps:
            print(
                f"  {ticker:<12} n={n:<7} uniq={uniq:<7} max+1={expected:<7} DUPS={dups} GAPS={gaps}"
            )

    print(f"\ntickers={len(seqs)} | duplicates={dup_total} | gaps={gap_total}")
    if dup_total == 0 and gap_total == 0 and seqs:
        print("EXACTLY-ONCE VERIFIED: zero duplicates, zero gaps")
        return 0
    print("EXACTLY-ONCE VIOLATION DETECTED" if seqs else "NO DATA FOR SESSION")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
