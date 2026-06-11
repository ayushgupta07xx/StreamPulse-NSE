"""Synthetic tick generator CLI.

Replays one historical trading day as per-second synthetic ticks for all (or a
subset of) Nifty 50 tickers, at 1×/10×/100×/max speed, with deliberate anomaly
injection recorded to a ground-truth file.

Examples:
    python -m generator.main run --speed 10
    python -m generator.main run --speed max --tickers TCS,RELIANCE --anomalies 4
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import typer

from generator import metrics
from generator.anomaly_injector import inject
from generator.gbm import SESSION_SECONDS, DayBar, synth_price_path, synth_sides, synth_volume_path
from generator.kafka_sink import TickSink, ensure_topics
from generator.schemas import GroundTruth, Tick

IST = ZoneInfo("Asia/Kolkata")
DATA_DIR = Path(os.environ.get("STREAMPULSE_DATA_DIR", "data"))

app = typer.Typer(add_completion=False)
log = logging.getLogger("generator")


def _load_day(ticker: str, trading_date: str) -> tuple[DayBar, str]:
    df = pd.read_parquet(DATA_DIR / "historical_ohlc" / f"{ticker}.parquet")
    row = df.iloc[-1] if trading_date == "latest" else df[df["date"].astype(str) == trading_date].iloc[0]
    bar = DayBar(
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=int(row["volume"]),
    )
    return bar, str(row["date"])


def _session_timestamps(trading_date: str) -> list[datetime]:
    base = datetime.fromisoformat(trading_date).replace(hour=9, minute=15, tzinfo=IST)
    return [base + timedelta(seconds=s) for s in range(SESSION_SECONDS)]


@app.command()
def run(
    speed: str = typer.Option(os.environ.get("SPEED", "10"), help="1 | 10 | 100 | max"),
    bootstrap: str = typer.Option(os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")),
    topic: str = typer.Option("nse.ticks.raw"),
    tickers: str = typer.Option(os.environ.get("TICKERS", "ALL"), help="ALL or comma-separated"),
    trading_date: str = typer.Option("latest", help="YYYY-MM-DD from historical data, or 'latest'"),
    anomalies: int = typer.Option(12, help="number of anomalies to inject across the session"),
    seed: int = typer.Option(42, help="RNG seed — same seed, same synthetic day"),
    ground_truth_out: Path = typer.Option(DATA_DIR / "anomaly_ground_truth.json"),
    metrics_port: int = typer.Option(8000),
    duration_s: int = typer.Option(0, help="stop after N wall-clock seconds (0 = full session)"),
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    metrics.serve(metrics_port)

    meta = pd.read_csv(DATA_DIR / "nifty50_metadata.csv")
    universe = list(meta["ticker"]) if tickers.upper() == "ALL" else [t.strip() for t in tickers.split(",")]

    rng = np.random.default_rng(seed)
    session_id = f"sess-{uuid.uuid4().hex[:12]}"

    # ── Build the synthetic day for every ticker ──────────────────────────
    paths: dict[str, dict] = {}
    resolved_date = ""
    for ticker in universe:
        bar, resolved_date = _load_day(ticker, trading_date)
        prices = synth_price_path(bar, rng)
        volumes = synth_volume_path(bar, rng)
        paths[ticker] = {"prices": prices, "volumes": volumes}
    timestamps = _session_timestamps(resolved_date)
    log.info("synthesized %d tickers × %d ticks for %s", len(paths), SESSION_SECONDS, resolved_date)

    # ── Inject anomalies & record ground truth ────────────────────────────
    records = []
    for i in range(anomalies):
        ticker = universe[int(rng.integers(0, len(universe)))]
        rec = inject(
            ticker,
            paths[ticker]["prices"],
            paths[ticker]["volumes"],
            timestamps,
            rng,
            counter=i,
        )
        records.append(rec)
        metrics.ANOMALIES_INJECTED.labels(anomaly_type=rec.anomaly_type).inc()
        log.info("injected %s on %s at %s (%s)", rec.anomaly_type, ticker, rec.start_ts.time(), rec.description)

    truth = GroundTruth(
        session_id=session_id,
        seed=seed,
        trading_date=resolved_date,
        generated_at=datetime.now(tz=IST),
        anomalies=records,
    )
    ground_truth_out.parent.mkdir(parents=True, exist_ok=True)
    ground_truth_out.write_text(json.dumps(json.loads(truth.model_dump_json()), indent=2))
    log.info("ground truth (%d anomalies) → %s", len(records), ground_truth_out)

    # ── Stream ────────────────────────────────────────────────────────────
    ensure_topics(bootstrap)
    sink = TickSink(bootstrap, topic)
    sides = {t: synth_sides(paths[t]["prices"], rng) for t in universe}
    seqs = dict.fromkeys(universe, 0)

    sleep_per_step = 0.0 if speed == "max" else 1.0 / float(speed)
    metrics.TARGET_RATE.set(0 if speed == "max" else len(universe) * float(speed))
    log.info("streaming to %s at %s× (%s tickers)", topic, speed, len(universe))

    wall_start = time.monotonic()
    emitted = 0
    try:
        for second in range(SESSION_SECONDS):
            step_start = time.monotonic()
            for ticker in universe:
                p = paths[ticker]
                tick = Tick(
                    ticker=ticker,
                    timestamp_ist=timestamps[second],
                    price=round(float(p["prices"][second]), 2),
                    volume=int(p["volumes"][second]),
                    side=str(sides[ticker][second]),  # type: ignore[arg-type]
                    session_id=session_id,
                    seq=seqs[ticker],
                )
                seqs[ticker] += 1
                sink.send(tick)
                emitted += 1
            metrics.EMITTED_SESSION_SECOND.set(second)

            if duration_s and (time.monotonic() - wall_start) >= duration_s:
                log.info("duration limit reached at session second %d", second)
                break
            if sleep_per_step:
                remaining = sleep_per_step - (time.monotonic() - step_start)
                if remaining > 0:
                    time.sleep(remaining)
    finally:
        sink.flush()
        elapsed = time.monotonic() - wall_start
        log.info("emitted %d ticks in %.1fs (%.0f ticks/s)", emitted, elapsed, emitted / max(elapsed, 1e-9))


if __name__ == "__main__":
    app()
