"""Flink job 3: online anomaly detection — rolling Z-score + EWMA SPC.

    nse.ticks.clean ──▶ key_by(ticker) ──▶ stateful detectors ──▶ nse.anomalies

Detectors (per ticker, keyed state):
- **Z-score**: |price − rolling_mean| / rolling_std > 3 over a 5-minute rolling
  window of ticks. Catches sudden spikes; sub-second detection latency.
- **EWMA SPC**: exponentially weighted moving average (λ=0.2) with Western
  Electric rules against the rolling baseline:
    R1: one point beyond 3σ_ewma          R2: 2 of 3 beyond 2σ (same side)
    R3: 4 of 5 beyond 1σ (same side)      R4: 8 consecutive on one side
  Catches gradual drifts the Z-score misses.

Each event carries detection_method + score + JSON context. Ensemble severity
(# methods agreeing) is computed downstream in ClickHouse (vw_anomaly_ensemble)
— detectors stay independent and single-purpose.

A 30 s per-(ticker, method) cooldown suppresses alert storms while a condition
persists.

Submit:
    flink run -py /opt/streampulse/flink/jobs/anomaly_online.py \
        --pyFiles /opt/streampulse/flink/jobs -d
"""

from __future__ import annotations

import json
import math
import sys
from collections import deque

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.typeinfo import Types as T
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

sys.path.insert(0, "/opt/streampulse/flink/jobs")

from common.pipeline import (  # noqa: E402
    dumps,
    kafka_exactly_once_sink,
    kafka_json_source,
    make_env,
    ooo_from_argv,
    tick_watermarks,
    ts_to_epoch_ms,
)

ROLLING_WINDOW_MS = 5 * 60 * 1000
WARMUP_TICKS = 60
Z_THRESHOLD = 3.0
EWMA_LAMBDA = 0.2
EWMA_L = 3.0
COOLDOWN_MS = 30_000
_EPS = 1e-9


class OnlineDetectors(KeyedProcessFunction):
    """Rolling Z-score + EWMA SPC over keyed per-ticker state."""

    def open(self, ctx: RuntimeContext) -> None:
        self.window_state = ctx.get_state(
            ValueStateDescriptor("rolling_window", Types.PICKLED_BYTE_ARRAY())
        )
        self.ewma_state = ctx.get_state(
            ValueStateDescriptor("ewma", Types.PICKLED_BYTE_ARRAY())
        )
        self.cooldown_state = ctx.get_state(
            ValueStateDescriptor("cooldowns", Types.PICKLED_BYTE_ARRAY())
        )

    # ── helpers ──────────────────────────────────────────────────────────
    def _rolling(self, ts_ms: int, price: float) -> tuple[int, float, float]:
        """Push (ts, price); evict >5 min old; return (n, mean, std)."""
        st = self.window_state.value() or {"dq": deque(), "s": 0.0, "s2": 0.0}
        dq: deque = st["dq"]
        dq.append((ts_ms, price))
        st["s"] += price
        st["s2"] += price * price
        cutoff = ts_ms - ROLLING_WINDOW_MS
        while dq and dq[0][0] < cutoff:
            _, old = dq.popleft()
            st["s"] -= old
            st["s2"] -= old * old
        n = len(dq)
        mean = st["s"] / n
        var = max(st["s2"] / n - mean * mean, 0.0)
        self.window_state.update(st)
        return n, mean, math.sqrt(var)

    def _cooldown_ok(self, method: str, ts_ms: int) -> bool:
        cds = self.cooldown_state.value() or {}
        if ts_ms - cds.get(method, -COOLDOWN_MS) < COOLDOWN_MS:
            return False
        cds[method] = ts_ms
        self.cooldown_state.update(cds)
        return True

    @staticmethod
    def _event(tick: dict, method: str, score: float, context: dict) -> str:
        return dumps(
            {
                "ticker": tick["ticker"],
                "ts": tick["timestamp_ist"],
                "detection_method": method,
                "score": round(score, 4),
                "severity": 1,
                "context": json.dumps(context),
                "session_id": tick.get("session_id", ""),
            }
        )

    # ── main ─────────────────────────────────────────────────────────────
    def process_element(self, tick: dict, ctx: KeyedProcessFunction.Context):
        price = float(tick["price"])
        ts_ms = ts_to_epoch_ms(tick["timestamp_ist"])

        n, mean, std = self._rolling(ts_ms, price)
        if n < WARMUP_TICKS or std < _EPS:
            return

        # ── Z-score ──
        z = (price - mean) / std
        if abs(z) > Z_THRESHOLD and self._cooldown_ok("zscore", ts_ms):
            yield self._event(
                tick, "zscore", abs(z),
                {"z": round(z, 3), "mean": round(mean, 2), "std": round(std, 4), "window_n": n},
            )

        # ── EWMA SPC ──
        est = self.ewma_state.value() or {"ewma": price, "t": 0, "recent": deque(maxlen=8)}
        est["t"] += 1
        est["ewma"] = EWMA_LAMBDA * price + (1 - EWMA_LAMBDA) * est["ewma"]
        t = est["t"]
        sigma_ewma = std * math.sqrt(
            (EWMA_LAMBDA / (2 - EWMA_LAMBDA)) * (1 - (1 - EWMA_LAMBDA) ** (2 * t))
        )
        dev = (est["ewma"] - mean) / max(sigma_ewma, _EPS)  # in σ_ewma units
        est["recent"].append(dev)
        self.ewma_state.update(est)

        rule = self._western_electric(est["recent"])
        if rule and self._cooldown_ok("ewma_spc", ts_ms):
            yield self._event(
                tick, "ewma_spc", abs(dev),
                {"rule": rule, "ewma": round(est["ewma"], 2), "dev_sigma": round(dev, 3),
                 "mean": round(mean, 2)},
            )

    @staticmethod
    def _western_electric(recent: deque) -> str | None:
        r = list(recent)
        if not r:
            return None
        if abs(r[-1]) > 3:
            return "WE1_beyond_3sigma"
        if len(r) >= 3:
            last3 = r[-3:]
            if sum(1 for d in last3 if d > 2) >= 2 or sum(1 for d in last3 if d < -2) >= 2:
                return "WE2_2of3_beyond_2sigma"
        if len(r) >= 5:
            last5 = r[-5:]
            if sum(1 for d in last5 if d > 1) >= 4 or sum(1 for d in last5 if d < -1) >= 4:
                return "WE3_4of5_beyond_1sigma"
        if len(r) >= 8 and (all(d > 0 for d in r[-8:]) or all(d < 0 for d in r[-8:])):
            return "WE4_8_one_side"
        return None


def build(env: StreamExecutionEnvironment) -> None:
    source = kafka_json_source("nse.ticks.clean", group_id="flink-anomaly-online")
    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "ticks-clean-anomaly")
        .map(json.loads)
        .assign_timestamps_and_watermarks(tick_watermarks(ooo_from_argv()))
        .key_by(lambda t: t["ticker"], key_type=T.STRING())
        .process(OnlineDetectors(), output_type=T.STRING())
        .sink_to(kafka_exactly_once_sink("nse.anomalies", "anomaly-online"))
        .name("anomalies-sink")
    )


if __name__ == "__main__":
    env = make_env(parallelism=2)
    build(env)
    env.execute("anomaly-online")
