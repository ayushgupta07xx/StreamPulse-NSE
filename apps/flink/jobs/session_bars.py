"""Flink job 4: event-time session windows (5-minute inactivity gap).

    nse.ticks.clean ──▶ key_by(ticker) ──▶ EventTimeSessionWindows(gap=5m) ──▶ nse.bars.session

Purpose (§13 windowing strategy): session windows close after 5 minutes of
inactivity per ticker — covering after-hours / pre-market trickles and, with
the market-hours-only synthetic feed, emitting one whole-session OHLCV summary
per ticker per replayed trading range. Complements the tumbling-window job
(window_bars.py) and demonstrates the third windowing family.

Submit:
    flink run -py /opt/streampulse/flink/jobs/session_bars.py \
        --pyFiles /opt/streampulse/flink/jobs -d
"""

from __future__ import annotations

import json
import sys

from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import EventTimeSessionWindows

sys.path.insert(0, "/opt/streampulse/flink/jobs")

from common.ohlcv import AttachWindowMeta, OhlcvAggregate  # noqa: E402
from common.pipeline import (  # noqa: E402
    dumps,
    idle_from_argv,
    kafka_exactly_once_sink,
    kafka_json_source,
    make_env,
    ooo_from_argv,
    record_ts_watermarks,
)

SESSION_GAP_MIN = 5


def build(env: StreamExecutionEnvironment) -> None:
    source = kafka_json_source("nse.ticks.clean", group_id="flink-session-bars")
    (
        env.from_source(
            source, record_ts_watermarks(ooo_from_argv(), idle_from_argv()), "ticks-clean-session"
        )
        .map(json.loads)
        .key_by(lambda t: t["ticker"], key_type=Types.STRING())
        .window(EventTimeSessionWindows.with_gap(Time.minutes(SESSION_GAP_MIN)))
        .aggregate(OhlcvAggregate(), window_function=AttachWindowMeta("session"))
        .map(dumps, output_type=Types.STRING())
        .sink_to(kafka_exactly_once_sink("nse.bars.session", "bars-session"))
        .name("bars-session-sink")
    )


if __name__ == "__main__":
    env = make_env(parallelism=2)
    build(env)
    env.execute("session-bars")
