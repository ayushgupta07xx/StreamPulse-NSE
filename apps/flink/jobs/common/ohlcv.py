"""Shared OHLCV window aggregation (used by tumbling bars and session bars)."""

from __future__ import annotations

from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction

from common.pipeline import epoch_ms_to_iso, ts_to_epoch_ms


class OhlcvAggregate(AggregateFunction):
    """Incremental OHLCV accumulator (constant memory per open window).

    Open/close are tied to earliest/latest event time, so out-of-order arrival
    within the watermark bound cannot corrupt them.
    """

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
