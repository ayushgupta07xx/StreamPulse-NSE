"""Flink job 2: tumbling-window OHLCV bars (1m / 5m / 15m).

    nse.ticks.clean ──▶ parse ──▶ watermarks ──▶ key_by(ticker) ─┬▶ 1m  ──▶ nse.bars.1m
                                                                 ├▶ 5m  ──▶ nse.bars.5m
                                                                 └▶ 15m ──▶ nse.bars.15m
    late events (beyond 30 s allowed lateness) ──▶ nse.bars.late

- Event-time tumbling windows aligned to clock minutes.
- OHLCV per ticker-window: open/high/low/close/volume + vwap + tick_count.
  Open/close are tied to earliest/latest event time, so out-of-order arrival
  within the watermark bound cannot corrupt them.
- Watermarks: 5 s bounded out-of-orderness, 10 s idle-source detection (§13).
- Allowed lateness 30 s; later events go to the nse.bars.late side output.

Submit:
    flink run -py /opt/streampulse/flink/jobs/window_bars.py \
        --pyFiles /opt/streampulse/flink/jobs -d
"""

from __future__ import annotations

import json
import sys

from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

sys.path.insert(0, "/opt/streampulse/flink/jobs")

from common.pipeline import (  # noqa: E402
    dumps,
    epoch_ms_to_iso,
    kafka_exactly_once_sink,
    kafka_json_source,
    make_env,
    tick_watermarks,
    ts_to_epoch_ms,
)

ALLOWED_LATENESS_S = 30

WINDOWS = {
    "1m": Time.minutes(1),
    "5m": Time.minutes(5),
    "15m": Time.minutes(15),
}


class OhlcvAggregate(AggregateFunction):
    """Incremental OHLCV accumulator (constant memory per open window)."""

    def create_accumulator(self):
        #      first_ts   open   high  low   last_ts  close  vol  pv_sum  n
        return [None, 0.0, float("-inf"), float("inf"), None, 0.0, 0, 0.0, 0]

    def add(self, tick: dict, acc):
        ts = ts_to_epoch_ms(tick["timestamp_ist"])
        price, vol = float(tick["price"]), int(tick["volume"])
        if acc[0] is None or ts < acc[0]:
            acc[0], acc[1] = ts, price
        if acc[4] is None or ts >= acc[4]:
            acc[4], acc[5] = ts, price
        acc[2] = max(acc[2], price)
        acc[3] = min(acc[3], price)
        acc[6] += vol
        acc[7] += price * vol
        acc[8] += 1
        return acc

    def get_result(self, acc):
        return {
            "open": acc[1],
            "high": acc[2],
            "low": acc[3],
            "close": acc[5],
            "volume": acc[6],
            "vwap": round(acc[7] / acc[6], 4) if acc[6] else acc[5],
            "tick_count": acc[8],
        }

    def merge(self, a, b):
        first = a if (b[0] is None or (a[0] is not None and a[0] <= b[0])) else b
        last = a if (b[4] is None or (a[4] is not None and a[4] >= b[4])) else b
        return [
            first[0], first[1],
            max(a[2], b[2]), min(a[3], b[3]),
            last[4], last[5],
            a[6] + b[6], a[7] + b[7], a[8] + b[8],
        ]


class AttachWindowMeta(ProcessWindowFunction):
    """Stamp ticker + window bounds + bar size onto the aggregate."""

    def __init__(self, bar_size: str) -> None:
        self.bar_size = bar_size

    def process(self, key: str, context: ProcessWindowFunction.Context, elements):
        bar = next(iter(elements))
        bar.update(
            {
                "ticker": key,
                "bar_size": self.bar_size,
                "window_start": epoch_ms_to_iso(context.window().start),
                "window_end": epoch_ms_to_iso(context.window().end),
            }
        )
        yield bar


def build(env: StreamExecutionEnvironment) -> None:
    source = kafka_json_source("nse.ticks.clean", group_id="flink-window-bars")
    from pyflink.common import WatermarkStrategy

    parsed = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "ticks-clean")
        .map(json.loads)
        .assign_timestamps_and_watermarks(tick_watermarks())
    )
    keyed = parsed.key_by(lambda t: t["ticker"], key_type=Types.STRING())

    late_streams = []
    for size, span in WINDOWS.items():
        late_tag = OutputTag(f"late-{size}")
        bars = (
            keyed.window(TumblingEventTimeWindows.of(span))
            .allowed_lateness(ALLOWED_LATENESS_S * 1000)
            .side_output_late_data(late_tag)
            .aggregate(OhlcvAggregate(), window_function=AttachWindowMeta(size))
        )
        (
            bars.map(dumps, output_type=Types.STRING())
            .sink_to(kafka_exactly_once_sink(f"nse.bars.{size}", f"bars-{size}"))
            .name(f"bars-{size}-sink")
        )
        late_streams.append(bars.get_side_output(late_tag))

    late = late_streams[0]
    for extra in late_streams[1:]:
        late = late.union(extra)
    (
        late.map(lambda t: dumps({**t, "late": True}), output_type=Types.STRING())
        .sink_to(kafka_exactly_once_sink("nse.bars.late", "bars-late"))
        .name("bars-late-sink")
    )


if __name__ == "__main__":
    env = make_env(parallelism=2)
    build(env)
    env.execute("window-bars")
