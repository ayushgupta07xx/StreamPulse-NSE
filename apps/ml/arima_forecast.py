"""ARIMA forecast-residual anomaly detection (§14 method 4).

Per ticker: fit ARIMA(1,1,1) on 5-minute closes (warm-up window), then update
online via Kalman-filter state extension (statsmodels ``append``) — no refit
per bar. Anomaly when |standardized one-step-ahead forecast residual| > 3.

Consumes nse.bars.5m → emits to nse.anomalies (detection_method='arima_residual').

Run:    python -m ml.arima_forecast run
"""

from __future__ import annotations

import json
import logging
import os
import warnings
from collections import defaultdict

import numpy as np
import typer
from confluent_kafka import Consumer, Producer
from statsmodels.tsa.arima.model import ARIMA

# 24 bars = 2 h of 5m closes: enough for ARIMA(1,1,1) MLE to stabilize while
# leaving most of a replay session for scoring (50 was longer than a typical
# benchmark session yields per ticker)
WARMUP_BARS = 24
RESIDUAL_THRESHOLD = 3.0
ORDER = (1, 1, 1)

app = typer.Typer(add_completion=False)
log = logging.getLogger("arima")
warnings.filterwarnings("ignore")  # statsmodels convergence chatter


@app.callback()
def _root() -> None:
    """ARIMA forecast-residual detector."""


class TickerArima:
    """Warm-up buffer → fitted ARIMA → O(1) online appends."""

    def __init__(self) -> None:
        self.buffer: list[float] = []
        self.result = None
        self.resid_std: float = 0.0

    def update(self, close: float) -> float | None:
        """Feed one close; return standardized residual once warmed up."""
        if self.result is None:
            self.buffer.append(close)
            if len(self.buffer) >= WARMUP_BARS:
                model = ARIMA(np.asarray(self.buffer), order=ORDER)
                self.result = model.fit(method_kwargs={"warn_convergence": False})
                self.resid_std = float(np.std(self.result.resid[5:])) or 1e-9
            return None

        forecast = float(self.result.forecast(1)[0])
        resid = close - forecast
        z = resid / self.resid_std
        # state-space append: extends the Kalman filter without refitting
        self.result = self.result.append([close], refit=False)
        # slow EWMA of residual scale keeps σ adaptive without windows
        self.resid_std = float(0.99 * self.resid_std + 0.01 * abs(resid)) or 1e-9
        return float(z)


@app.command()
def run(
    bootstrap: str = typer.Option(os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")),
    threshold: float = typer.Option(RESIDUAL_THRESHOLD),
    max_bars: int = typer.Option(0, help="stop after N bars (0 = forever); used in tests"),
) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    models: dict[str, TickerArima] = defaultdict(TickerArima)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "ml-arima",
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe(["nse.bars.5m"])
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})

    seen = fired = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            bar = json.loads(msg.value())
            z = models[bar["ticker"]].update(float(bar["close"]))
            seen += 1
            if z is not None and abs(z) > threshold:
                fired += 1
                producer.produce(
                    "nse.anomalies",
                    key=bar["ticker"].encode(),
                    value=json.dumps(
                        {
                            "ticker": bar["ticker"],
                            "ts": bar["window_end"],
                            "detection_method": "arima_residual",
                            "score": round(abs(float(z)), 4),
                            "severity": 1,
                            "context": json.dumps(
                                {"std_residual": round(float(z), 3), "order": "1,1,1"}
                            ),
                            "session_id": "",
                        }
                    ).encode(),
                )
                producer.poll(0)
            if seen % 500 == 0:
                log.info("bars=%d anomalies=%d", seen, fired)
            if max_bars and seen >= max_bars:
                break
    finally:
        producer.flush(10)
        consumer.close()
        log.info("done: bars=%d anomalies=%d", seen, fired)


if __name__ == "__main__":
    app()
