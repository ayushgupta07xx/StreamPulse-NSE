"""Live-demo anomaly injection (`make demo`).

Streams real-wall-clock ticks for a handful of tickers so the Grafana "now"
windows light up, then injects a visible price spike on the chosen ticker.
Watch the Market Overview + Anomaly Feed dashboards while it runs:

    within ~1 s of the spike the Z-score detector fires; EWMA confirms within
    a few seconds; the bars and annotations appear on the price chart.

Usage:
    python scripts/inject_demo_anomalies.py [--ticker TCS] [--minutes 5] [--spike-pct 4]
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, "apps")

from generator.kafka_sink import TickSink, ensure_topics  # noqa: E402
from generator.schemas import Tick  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
DEMO_TICKERS = {
    "TCS": 4200.0, "RELIANCE": 2950.0, "HDFCBANK": 1680.0,
    "INFY": 1850.0, "SBIN": 990.0,
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ticker", default="TCS", choices=sorted(DEMO_TICKERS))
    ap.add_argument("--minutes", type=float, default=5.0)
    ap.add_argument("--spike-pct", type=float, default=4.0)
    ap.add_argument("--bootstrap", default="localhost:29092")
    args = ap.parse_args()

    ensure_topics(args.bootstrap)
    sink = TickSink(args.bootstrap)
    rng = random.Random()

    prices = dict(DEMO_TICKERS)
    seqs = dict.fromkeys(DEMO_TICKERS, 0)
    session = f"demo-{int(time.time())}"
    total_s = int(args.minutes * 60)
    spike_at = total_s // 3            # spike a third of the way in
    spike_len = 6

    print(f"streaming {len(prices)} tickers at 1 tick/s for {args.minutes:.0f} min")
    print(f"+{args.spike_pct}% spike on {args.ticker} at t+{spike_at}s — watch the dashboards")

    for t in range(total_s):
        now = datetime.now(tz=IST)
        for ticker, p in list(prices.items()):
            drift = rng.gauss(0, p * 0.0004)
            price = max(p + drift, 1.0)
            if ticker == args.ticker and spike_at <= t < spike_at + spike_len:
                price = p * (1 + args.spike_pct / 100)
                if t == spike_at:
                    print(f"!!! SPIKE INJECTED on {ticker}: {p:.2f} -> {price:.2f}")
            else:
                prices[ticker] = price
            sink.send(
                Tick(
                    ticker=ticker, timestamp_ist=now, price=round(price, 2),
                    volume=rng.randint(50, 500),
                    side=rng.choice(["BUY", "SELL"]),
                    session_id=session, seq=seqs[ticker],
                )
            )
            seqs[ticker] += 1
        if t % 30 == 0 and t:
            print(f"  t+{t}s ...")
        time.sleep(1.0)

    sink.flush()
    print("demo stream complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
