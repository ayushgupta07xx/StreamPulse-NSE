"""Streaming Isolation Forest scoring (§14 method 3, online half).

Consumes nse.bars.5m, maintains a per-ticker trailing bar window in memory to
compute the same features as training (bootstrapped from ClickHouse at start),
scores each bar with models/isolation_forest_latest.joblib, and:

1. inserts every score into ClickHouse nse.anomalies_ml (full audit trail)
2. publishes flagged bars to nse.anomalies (detection_method='isolation_forest')
   so the ensemble sees all four methods on one topic

Run:    python -m ml.predict_loop run
"""

from __future__ import annotations

import json
import logging
import os
from collections import defaultdict, deque
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import typer
from confluent_kafka import Consumer, Producer

from ml.feature_builder import FEATURES, build_features, clickhouse_client, load_bars

MODELS_DIR = Path(os.environ.get("STREAMPULSE_MODELS_DIR", "models"))
WARMUP_BARS = 40  # >= _Z_WINDOW + slack so rolling features are defined

app = typer.Typer(add_completion=False)
log = logging.getLogger("predict-loop")


@app.callback()
def _root() -> None:
    """Streaming Isolation Forest scorer."""


class RollingScorer:
    def __init__(self) -> None:
        meta = json.loads((MODELS_DIR / "isolation_forest_latest.json").read_text())
        self.version: str = meta["version"]
        self.model = joblib.load(MODELS_DIR / "isolation_forest_latest.joblib")
        self.history: dict[str, deque] = defaultdict(lambda: deque(maxlen=WARMUP_BARS + 5))
        log.info("loaded isolation_forest v%s", self.version)

    def bootstrap(self) -> None:
        bars = load_bars(days=2, bar_size="5m")
        for _, row in bars.iterrows():
            self.history[row["ticker"]].append(row.to_dict())
        log.info("bootstrapped history for %d tickers", len(self.history))

    def score(self, bar: dict) -> tuple[float, bool, dict] | None:
        h = self.history[bar["ticker"]]
        h.append(bar)
        if len(h) < WARMUP_BARS:
            return None
        df = pd.DataFrame(list(h))
        # utc=True: bootstrap rows from ClickHouse are tz-naive while
        # streaming bars carry ISO offsets — mixing them raises in pandas
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
        feats = build_features(df)
        if feats.empty:
            return None
        row = feats.iloc[[-1]]
        score = float(self.model.decision_function(row[FEATURES])[0])
        flag = bool(self.model.predict(row[FEATURES])[0] == -1)
        vector = {f: float(row.iloc[0][f]) for f in FEATURES}
        return score, flag, vector


@app.command()
def run(
    bootstrap: str = typer.Option(os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")),
    max_bars: int = typer.Option(0, help="stop after N bars (0 = forever); used in tests"),
) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    scorer = RollingScorer()
    try:
        scorer.bootstrap()
    except Exception as exc:  # noqa: BLE001 — cold ClickHouse is fine, warm up from stream
        log.warning("bootstrap skipped: %s", exc)

    ch = clickhouse_client()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "ml-predict-loop",
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe(["nse.bars.5m"])
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})

    seen = flagged = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            bar = json.loads(msg.value())
            result = scorer.score(bar)
            seen += 1
            if result is None:
                continue
            score, flag, vector = result

            ch.insert(
                "nse.anomalies_ml",
                [[
                    bar["ticker"],
                    bar["window_start"].replace("T", " ").split("+")[0],
                    scorer.version,
                    score,
                    int(flag),
                    json.dumps(vector),
                ]],
                column_names=[
                    "ticker", "window_start", "model_version",
                    "anomaly_score", "is_anomaly", "features",
                ],
            )
            if flag:
                flagged += 1
                producer.produce(
                    "nse.anomalies",
                    key=bar["ticker"].encode(),
                    value=json.dumps(
                        {
                            "ticker": bar["ticker"],
                            "ts": bar["window_end"],
                            "detection_method": "isolation_forest",
                            "score": round(-score, 4),  # higher = more anomalous
                            "severity": 1,
                            "context": json.dumps({"model_version": scorer.version, **vector}),
                            "session_id": "",
                        }
                    ).encode(),
                )
                producer.poll(0)
            if seen % 500 == 0:
                log.info("scored=%d flagged=%d", seen, flagged)
            if max_bars and seen >= max_bars:
                break
    finally:
        producer.flush(10)
        consumer.close()
        log.info("done: scored=%d flagged=%d", seen, flagged)


if __name__ == "__main__":
    app()
