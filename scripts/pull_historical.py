"""Pull 1 year of daily OHLC for all Nifty 50 tickers via yfinance.

Writes one Parquet file per ticker to data/historical_ohlc/. Cached: existing
files are skipped unless --force. Committed to the repo so normal builds never
hit the network (see docs/decisions.md and LEGAL.md).

Usage:
    python scripts/pull_historical.py [--force] [--tickers TCS,RELIANCE]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

REPO_ROOT = Path(__file__).resolve().parent.parent
METADATA_CSV = REPO_ROOT / "data" / "nifty50_metadata.csv"
OUT_DIR = REPO_ROOT / "data" / "historical_ohlc"

EXPECTED_COLUMNS = ["open", "high", "low", "close", "volume"]


def pull_one(ticker: str, force: bool) -> str:
    """Download 1y daily OHLC for one NSE ticker. Returns status string."""
    out_path = OUT_DIR / f"{ticker}.parquet"
    if out_path.exists() and not force:
        return "cached"

    symbol = f"{ticker}.NS"
    df = yf.download(symbol, period="1y", interval="1d", auto_adjust=True, progress=False)
    if df is None or df.empty:
        return "EMPTY"

    # yfinance >=0.2.40 returns MultiIndex columns even for single tickers
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.rename(columns=str.lower)[EXPECTED_COLUMNS]
    df.index.name = "date"
    df = df.reset_index()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    bad = (df["high"] < df["low"]).sum()
    if bad:
        return f"INVALID ({bad} rows high<low)"

    df.to_parquet(out_path, index=False)
    return f"ok ({len(df)} rows)"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="re-download even if cached")
    parser.add_argument("--tickers", default="", help="comma-separated subset (default: all 50)")
    args = parser.parse_args()

    meta = pd.read_csv(METADATA_CSV)
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()] or list(meta["ticker"])

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []
    for i, ticker in enumerate(tickers, 1):
        try:
            status = pull_one(ticker, args.force)
        except Exception as exc:  # noqa: BLE001 — report and continue, retry pass below
            status = f"ERROR: {exc}"
        print(f"[{i:>2}/{len(tickers)}] {ticker:<12} {status}", flush=True)
        if status.startswith(("EMPTY", "ERROR", "INVALID")):
            failures.append(ticker)
        if status != "cached":
            time.sleep(0.8)  # stay polite to Yahoo; avoids rate-limit bans

    # one retry pass for transient failures
    still_failing = []
    for ticker in failures:
        time.sleep(2.0)
        try:
            status = pull_one(ticker, force=True)
        except Exception as exc:  # noqa: BLE001
            status = f"ERROR: {exc}"
        print(f"[retry] {ticker:<12} {status}", flush=True)
        if status.startswith(("EMPTY", "ERROR", "INVALID")):
            still_failing.append(ticker)

    if still_failing:
        print(f"\nFAILED tickers: {', '.join(still_failing)}", file=sys.stderr)
        return 1
    print(f"\nAll {len(tickers)} tickers cached in {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
