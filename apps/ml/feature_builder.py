"""Multivariate features per (ticker, 5-minute bar) for anomaly models.

Feature set (brief §14, Method 3):
- log_return          ln(close / prev_close)
- return_volatility   rolling std of log returns (6 bars = 30 min)
- volume_zscore       volume vs trailing 36-bar (3 h) mean/std
- vwap_deviation      (close − vwap) / vwap
- tick_count_zscore   tick_count vs trailing 36-bar mean/std
- pressure_proxy      (close − open) / (high − low) — signed intrabar pressure,
                      a bid/ask-imbalance proxy when book data is unavailable
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd

import clickhouse_connect

FEATURES = [
    "log_return",
    "return_volatility",
    "volume_zscore",
    "vwap_deviation",
    "tick_count_zscore",
    "pressure_proxy",
]

_VOL_WINDOW = 6     # bars (30 min) for return volatility
_Z_WINDOW = 36      # bars (3 h) for volume / tick-count z-scores
_EPS = 1e-9


def clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "streampulse"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "streampulse"),
    )


def load_bars(days: int, bar_size: str = "5m") -> pd.DataFrame:
    """Last N days of bars from ClickHouse, deduplicated (ReplacingMergeTree FINAL)."""
    client = clickhouse_client()
    q = """
        SELECT ticker, window_start, open, high, low, close, volume, vwap, tick_count
        FROM nse.bars FINAL
        WHERE bar_size = {bar_size:String}
          AND window_start >= now() - INTERVAL {days:UInt32} DAY
        ORDER BY ticker, window_start
    """
    df = client.query_df(q, parameters={"bar_size": bar_size, "days": days})
    return df


def build_features(bars: pd.DataFrame) -> pd.DataFrame:
    """Append FEATURES columns; rows lacking warm-up history are dropped."""
    if bars.empty:
        return bars.assign(**{f: [] for f in FEATURES})

    def per_ticker(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("window_start").copy()
        g["log_return"] = np.log(g["close"] / g["close"].shift(1))
        g["return_volatility"] = g["log_return"].rolling(_VOL_WINDOW).std()

        vol_mean = g["volume"].rolling(_Z_WINDOW).mean()
        vol_std = g["volume"].rolling(_Z_WINDOW).std()
        g["volume_zscore"] = (g["volume"] - vol_mean) / (vol_std + _EPS)

        g["vwap_deviation"] = (g["close"] - g["vwap"]) / (g["vwap"] + _EPS)

        tc_mean = g["tick_count"].rolling(_Z_WINDOW).mean()
        tc_std = g["tick_count"].rolling(_Z_WINDOW).std()
        g["tick_count_zscore"] = (g["tick_count"] - tc_mean) / (tc_std + _EPS)

        rng = (g["high"] - g["low"]).clip(lower=_EPS)
        g["pressure_proxy"] = (g["close"] - g["open"]) / rng
        return g

    out = bars.groupby("ticker", group_keys=False).apply(per_ticker)
    return out.dropna(subset=FEATURES)
