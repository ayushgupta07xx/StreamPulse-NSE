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
from pyflink.datastream.window import TumblingEventTimeWindows

sys.path.insert(0, "/opt/streampulse/flink/jobs")

from common.ohlcv import AttachWindowMeta, OhlcvAggregate  # noqa: E402
from common.pipeline import (  # noqa: E402
    dumps,
    kafka_exactly_once_sink,
    kafka_json_source,
    make_env,
    ooo_from_argv,
    record_ts_watermarks,
)

ALLOWED_LATENESS_S = 30

WINDOWS = {
    "1m": Time.minutes(1),
    "5m": Time.minutes(5),
    "15m": Time.minutes(15),
}


def build(env: StreamExecutionEnvironment) -> None:
    source = kafka_json_source("nse.ticks.clean", group_id="flink-window-bars")

    # Watermarks AT the source from Kafka record timestamps: per-partition
    # tracking absorbs consumption skew, zero Python in the watermark path
    parsed = (
        env.from_source(source, record_ts_watermarks(ooo_from_argv()), "ticks-clean")
        .map(json.loads)
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
