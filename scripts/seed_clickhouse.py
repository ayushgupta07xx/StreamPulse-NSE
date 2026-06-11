"""Seed ClickHouse with synthetic 5m bars directly from committed daily OHLC.

Bypasses the streaming pipeline: for dev bootstraps and CI (scheduled retrain
needs multi-day feature history without running Kafka/Flink). Bars come from
the same GBM engine the generator uses, aggregated per 5-minute window.

Usage:
    python scripts/seed_clickhouse.py [--days 7] [--bar-size 5m]
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

sys.path.insert(0, "apps")

from generator.gbm import SESSION_SECONDS, DayBar, synth_price_path, synth_volume_path  # noqa: E402
from ml.feature_builder import clickhouse_client  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
REPO = Path(__file__).resolve().parent.parent


def bars_for_day(ticker: str, row: pd.Series, span_s: int, rng: np.random.Generator) -> list[list]:
    bar = DayBar(
        open=float(row["open"]), high=float(row["high"]),
        low=float(row["low"]), close=float(row["close"]), volume=int(row["volume"]),
    )
    prices = synth_price_path(bar, rng)
    volumes = synth_volume_path(bar, rng)
    base = datetime.combine(row["date"], datetime.min.time(), tzinfo=IST).replace(hour=9, minute=15)

    out = []
    for start in range(0, SESSION_SECONDS - span_s + 1, span_s):
        seg_p = prices[start : start + span_s]
        seg_v = volumes[start : start + span_s]
        vol = int(seg_v.sum())
        vwap = float((seg_p * seg_v).sum() / vol) if vol else float(seg_p[-1])
        ws = base + timedelta(seconds=start)
        out.append([
            ticker, "5m",
            ws.strftime("%Y-%m-%d %H:%M:%S"),
            (ws + timedelta(seconds=span_s)).strftime("%Y-%m-%d %H:%M:%S"),
            float(seg_p[0]), float(seg_p.max()), float(seg_p.min()), float(seg_p[-1]),
            vol, round(vwap, 4), span_s,
        ])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    client = clickhouse_client()
    rng = np.random.default_rng(args.seed)
    meta = pd.read_csv(REPO / "data" / "nifty50_metadata.csv")

    total = 0
    for ticker in meta["ticker"]:
        df = pd.read_parquet(REPO / "data" / "historical_ohlc" / f"{ticker}.parquet").tail(args.days)
        rows: list[list] = []
        for _, row in df.iterrows():
            rows.extend(bars_for_day(ticker, row, 300, rng))
        client.insert(
            "nse.bars", rows,
            column_names=["ticker", "bar_size", "window_start", "window_end",
                          "open", "high", "low", "close", "volume", "vwap", "tick_count"],
        )
        total += len(rows)
    print(f"seeded {total} 5m bars across {len(meta)} tickers x {args.days} days")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
