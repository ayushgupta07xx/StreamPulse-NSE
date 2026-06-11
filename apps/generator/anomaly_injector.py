"""Deliberate anomaly injection with ground-truth recording.

Anomalies are applied to the precomputed price/volume arrays *before*
streaming, and every injection is recorded so detection precision/recall can
be measured against truth (docs/detection-benchmarks.md).

Types:
- PRICE_SPIKE       short multiplicative price excursion (2–5%), then reverts
- LEVEL_SHIFT       persistent ±1–3% shift from onset to session end
- VOLATILITY_BURST  local noise amplified 4–8× for 30–120 s
- VOLUME_SURGE      volume ×10–30 for 10–60 s (price untouched — exercises
                    multivariate detectors that univariate price methods miss)
"""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np

from generator.schemas import AnomalyRecord

ANOMALY_TYPES = ("PRICE_SPIKE", "LEVEL_SHIFT", "VOLATILITY_BURST", "VOLUME_SURGE")

# Keep injections away from session edges so warm-up windows exist
_EDGE_BUFFER = 900  # seconds


def inject(
    ticker: str,
    prices: np.ndarray,
    volumes: np.ndarray,
    timestamps: list[datetime],
    rng: np.random.Generator,
    anomaly_type: str | None = None,
    counter: int = 0,
) -> AnomalyRecord:
    """Mutate ``prices``/``volumes`` in place with one anomaly; return its record."""
    n = len(prices)
    a_type = anomaly_type or rng.choice(ANOMALY_TYPES)
    start = int(rng.integers(_EDGE_BUFFER, n - _EDGE_BUFFER))

    if a_type == "PRICE_SPIKE":
        duration = int(rng.integers(2, 8))
        magnitude = float(rng.uniform(0.02, 0.05) * rng.choice([-1.0, 1.0]))
        prices[start : start + duration] *= 1.0 + magnitude
        desc = f"{magnitude:+.2%} spike for {duration}s"

    elif a_type == "LEVEL_SHIFT":
        duration = n - start
        magnitude = float(rng.uniform(0.01, 0.03) * rng.choice([-1.0, 1.0]))
        prices[start:] *= 1.0 + magnitude
        desc = f"{magnitude:+.2%} persistent level shift"

    elif a_type == "VOLATILITY_BURST":
        duration = int(rng.integers(30, 121))
        magnitude = float(rng.uniform(4.0, 8.0))
        seg = slice(start, min(start + duration, n))
        local_mean = prices[seg].mean()
        prices[seg] = local_mean + (prices[seg] - local_mean) * magnitude
        # amplified deviations could pierce zero on cheap stocks — floor them
        np.maximum(prices[seg], local_mean * 0.5, out=prices[seg])
        desc = f"volatility ×{magnitude:.1f} for {duration}s"

    else:  # VOLUME_SURGE
        duration = int(rng.integers(10, 61))
        magnitude = float(rng.uniform(10.0, 30.0))
        seg = slice(start, min(start + duration, n))
        volumes[seg] = (volumes[seg].astype(np.float64) * magnitude).astype(np.int64)
        desc = f"volume ×{magnitude:.0f} for {duration}s"

    end = min(start + duration, n - 1)
    return AnomalyRecord(
        anomaly_id=f"{ticker}-{counter:03d}",
        ticker=ticker,
        anomaly_type=a_type,  # type: ignore[arg-type]
        start_ts=timestamps[start],
        end_ts=timestamps[end] if a_type != "LEVEL_SHIFT" else timestamps[start] + timedelta(seconds=120),
        magnitude=magnitude,
        description=desc,
    )
