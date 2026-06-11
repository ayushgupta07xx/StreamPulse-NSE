"""Flink job 3: online anomaly detection — rolling Z-score + EWMA SPC.

    nse.ticks.clean ──▶ key_by(ticker) ──▶ stateful detectors ──▶ nse.anomalies

Detectors (per ticker, keyed state):
- **Z-score**: price vs a 5-minute rolling window, two-tier (|z| ≥ 6 instant,
  4 ≤ |z| < 6 needs 2 consecutive ticks). Catches sudden spikes; sub-second
  detection latency.
- **EWMA SPC** on log-returns (λ=0.2), two charts:
    mean chart   — |EWMA| beyond 6σ_ewma (classic 3σ + WE rules saturate on
                   the fat-tailed tick returns here; thresholds settled by
                   replaying a 200-anomaly ground-truth session, see
                   docs/detection-benchmarks.md)
    dispersion   — fast(λ=.2)/slow(frozen-during-burst) variance ratio > 10
                   for symmetric volatility bursts the mean chart can't see.

Each event carries detection_method + score + JSON context. Ensemble severity
(# methods agreeing) is computed downstream in ClickHouse (vw_anomaly_ensemble)
— detectors stay independent and single-purpose.

A 120 s per-(ticker, method) cooldown suppresses alert storms while a condition
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

from pyflink.common import Types
from pyflink.common.typeinfo import Types as T
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

sys.path.insert(0, "/opt/streampulse/flink/jobs")

from common.pipeline import (  # noqa: E402
    dumps,
    idle_from_argv,
    kafka_exactly_once_sink,
    kafka_json_source,
    make_env,
    ooo_from_argv,
    record_ts_watermarks,
    ts_to_epoch_ms,
)

ROLLING_WINDOW_MS = 5 * 60 * 1000
WARMUP_TICKS = 60
EWMA_WARMUP_T = 240  # ticks before EWMA alerts (slow variance must settle)
Z_EXTREME = 6.0  # fire immediately — unambiguous single-tick spike
Z_THRESHOLD = 4.0  # §20: start conservative; tuned against ground truth
Z_PERSISTENCE = 2  # consecutive breaches for moderate z (kills 1-tick noise)
EWMA_LAMBDA = 0.2
# Mean-chart limit in σ_ewma. 3σ + Western Electric is textbook for Gaussian
# returns; tick-level returns here are jump-diffusion fat-tailed and 3σ fired
# at nearly every cooldown expiry (offline replay vs 200 injected anomalies:
# P=.10 R=.96). 6σ with no WE2 measured P=.69 R=.73 on the same replay.
EWMA_DEV_THRESHOLD = 6.0
VOL_RATIO_THRESHOLD = 10.0  # fast/slow variance ratio — volatility bursts (P/R-swept)
COOLDOWN_MS = 120_000  # per (ticker, method); suppresses alert storms
_EPS = 1e-9


class OnlineDetectors(KeyedProcessFunction):
    """Rolling Z-score + EWMA SPC over keyed per-ticker state."""

    def open(self, ctx: RuntimeContext) -> None:
        self.window_state = ctx.get_state(
            ValueStateDescriptor("rolling_window", Types.PICKLED_BYTE_ARRAY())
        )
        self.ewma_state = ctx.get_state(ValueStateDescriptor("ewma", Types.PICKLED_BYTE_ARRAY()))
        self.cooldown_state = ctx.get_state(
            ValueStateDescriptor("cooldowns", Types.PICKLED_BYTE_ARRAY())
        )

    # ── helpers ──────────────────────────────────────────────────────────
    def _rolling(self, ts_ms: int, price: float) -> tuple[int, float, float]:
        """Push (ts, price); evict >5 min old; return (n, mean, std).

        Replay guard: if event time jumps BACKWARDS by more than the window
        (a new replay session over checkpoint-restored state), the deque holds
        "future" ticks that left-eviction can never remove — poisoned stats
        forever. Detect the jump and start fresh for this key.
        """
        st = self.window_state.value() or {"dq": deque(), "s": 0.0, "s2": 0.0}
        dq: deque = st["dq"]
        if dq and ts_ms < dq[-1][0] - ROLLING_WINDOW_MS:
            st = {"dq": deque(), "s": 0.0, "s2": 0.0}
            dq = st["dq"]
            self.ewma_state.clear()
            self.cooldown_state.clear()
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

        # ── Z-score (price level vs rolling window): two-tier ──
        #    |z| ≥ 6 fires instantly (a 2-tick spike is unambiguous at 6σ and
        #    persistence would miss it); 4 ≤ |z| < 6 needs 2 consecutive ticks
        z = (price - mean) / std
        est = self.ewma_state.value() or {
            "ewma_r": 0.0,
            "var_slow": None,
            "var_fast": None,
            "t": 0,
            "z_run": 0,
            "last_price": price,
            "last_ts": 0,
        }
        if abs(z) > Z_THRESHOLD:
            est["z_run"] += 1
        else:
            est["z_run"] = 0
        z_fire = abs(z) >= Z_EXTREME or est["z_run"] >= Z_PERSISTENCE
        if z_fire and self._cooldown_ok("zscore", ts_ms):
            yield self._event(
                tick,
                "zscore",
                abs(z),
                {
                    "z": round(z, 3),
                    "mean": round(mean, 2),
                    "std": round(std, 4),
                    "window_n": n,
                    "run": est["z_run"],
                },
            )

        # ── EWMA SPC on log-RETURNS (stationary; price levels trend and
        #    poison control limits — measured: 5,650 false alarms/session).
        #    Two charts: mean chart (level shifts, big moves) and a
        #    dispersion chart (fast/slow variance ratio — volatility bursts,
        #    which are symmetric and invisible to the mean chart) ──
        #    Returns need consecutive event-time ticks: the clean topic is
        #    keyless (ADR-007: PyFlink sinks are value-only) so a ticker's
        #    ticks interleave across partitions, and a return computed across
        #    an out-of-order pair is an artificial zigzag that inflates the
        #    fast variance — measured: 1,316 dispersion false alarms/session
        #    vs 39 on the same ticks in event-time order. Skip backwards ticks
        #    here; the rolling Z-score window above handles them by timestamp.
        if ts_ms <= est["last_ts"]:
            self.ewma_state.update(est)  # persist z_run from the zscore tier
            return
        est["last_ts"] = ts_ms
        r = math.log(price / max(est["last_price"], _EPS))
        est["last_price"] = price
        est["t"] += 1
        est["ewma_r"] = EWMA_LAMBDA * r + (1 - EWMA_LAMBDA) * est["ewma_r"]
        est["var_fast"] = (
            r * r
            if est["var_fast"] is None
            else (EWMA_LAMBDA * r * r + (1 - EWMA_LAMBDA) * est["var_fast"])
        )
        # slow baseline freezes while dispersion is elevated, so a burst can't
        # contaminate its own reference. The freeze only protects an ESTABLISHED
        # baseline: during warmup it must update unconditionally, else a near-zero
        # initial r² deadlocks it (ratio stays huge, σ_ewma stays microscopic, and
        # the mean chart fires at every cooldown expiry — observed live).
        ratio = (est["var_fast"] / est["var_slow"]) if est["var_slow"] else 1.0
        if est["var_slow"] is None:
            est["var_slow"] = r * r
        elif est["t"] <= EWMA_WARMUP_T or ratio < VOL_RATIO_THRESHOLD / 2:
            est["var_slow"] = 0.98 * est["var_slow"] + 0.02 * r * r

        sigma_r = math.sqrt(max(est["var_slow"], _EPS * _EPS))
        sigma_ewma = sigma_r * math.sqrt(
            (EWMA_LAMBDA / (2 - EWMA_LAMBDA)) * (1 - (1 - EWMA_LAMBDA) ** (2 * est["t"]))
        )
        dev = est["ewma_r"] / max(sigma_ewma, _EPS)  # center line = 0 for returns
        self.ewma_state.update(est)

        if est["t"] < EWMA_WARMUP_T:
            return

        rule = "MEAN_beyond_6sigma" if abs(dev) > EWMA_DEV_THRESHOLD else None
        if rule is None and ratio > VOL_RATIO_THRESHOLD:
            rule = "DISPERSION_vol_ratio"
        if rule and self._cooldown_ok("ewma_spc", ts_ms):
            score = abs(dev) if rule != "DISPERSION_vol_ratio" else ratio
            yield self._event(
                tick,
                "ewma_spc",
                score,
                {
                    "rule": rule,
                    "ewma_return": round(est["ewma_r"], 6),
                    "dev_sigma": round(dev, 3),
                    "vol_ratio": round(ratio, 2),
                },
            )


def build(env: StreamExecutionEnvironment) -> None:
    source = kafka_json_source("nse.ticks.clean", group_id="flink-anomaly-online")
    (
        env.from_source(
            source, record_ts_watermarks(ooo_from_argv(), idle_from_argv()), "ticks-clean-anomaly"
        )
        .map(json.loads)
        .key_by(lambda t: t["ticker"], key_type=T.STRING())
        .process(OnlineDetectors(), output_type=T.STRING())
        .sink_to(kafka_exactly_once_sink("nse.anomalies", "anomaly-online"))
        .name("anomalies-sink")
    )


if __name__ == "__main__":
    env = make_env(parallelism=2)
    build(env)
    env.execute("anomaly-online")
