"""Geometric Brownian motion intraday tick synthesis.

For each ticker-day, the real daily OHLC row calibrates a per-second log-price
path:

- drift: total log return ln(close/open) spread across the session
- volatility: Parkinson range estimator from high/low
- a Brownian bridge correction pins the path's terminal value to the real close
  while preserving local volatility
- Poisson jump-diffusion adds occasional discrete jumps
- prices softly reflect off [low, high] so the synthetic day respects the real range

NSE cash session: 09:15:00–15:29:59 IST → 22,500 one-second ticks.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

SESSION_SECONDS = 22_500  # 09:15:00 .. 15:29:59
SESSION_OPEN = "09:15:00"

_PARKINSON = 4.0 * np.log(2.0)


@dataclass(frozen=True)
class DayBar:
    open: float
    high: float
    low: float
    close: float
    volume: int


def synth_price_path(
    bar: DayBar,
    rng: np.random.Generator,
    n: int = SESSION_SECONDS,
    jump_rate_per_day: float = 2.0,
) -> np.ndarray:
    """Return an ``n``-length price array starting at open, ending ≈ close."""
    log_open, log_close = np.log(bar.open), np.log(bar.close)

    sigma_day = np.sqrt(np.log(bar.high / bar.low) ** 2 / _PARKINSON)
    sigma_step = max(sigma_day, 1e-5) / np.sqrt(n)

    increments = rng.normal(0.0, sigma_step, size=n - 1)

    # Jump-diffusion: a few discrete log-jumps at Poisson-random seconds
    n_jumps = rng.poisson(jump_rate_per_day)
    if n_jumps:
        jump_idx = rng.integers(0, n - 1, size=n_jumps)
        jump_size = (
            rng.normal(0.0, 0.003, size=n_jumps) + np.sign(rng.standard_normal(n_jumps)) * 0.002
        )
        increments[jump_idx] += jump_size

    log_path = log_open + np.concatenate(([0.0], np.cumsum(increments)))

    # Brownian bridge: pin terminal value to the real close, keep local vol
    t = np.linspace(0.0, 1.0, n)
    log_path = log_path + t * (log_close - log_path[-1])

    # Soft reflection off the real day's range (small tolerance for realism)
    lo, hi = np.log(bar.low * 0.999), np.log(bar.high * 1.001)
    over, under = log_path > hi, log_path < lo
    log_path[over] = hi - (log_path[over] - hi) * 0.5
    log_path[under] = lo + (lo - log_path[under]) * 0.5

    return np.exp(log_path)


def synth_volume_path(
    bar: DayBar,
    rng: np.random.Generator,
    n: int = SESSION_SECONDS,
) -> np.ndarray:
    """Per-second volumes following the classic intraday U-shape, summing ≈ daily volume."""
    t = np.linspace(0.0, 1.0, n)
    u_shape = 1.6 - np.sin(np.pi * t)  # heavy at open/close, light midday
    weights = u_shape / u_shape.sum()
    lam = np.maximum(weights * max(bar.volume, n), 0.05)
    return rng.poisson(lam).astype(np.int64)


def synth_sides(prices: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    """BUY/SELL with momentum bias: upticks are 65% BUY, downticks 65% SELL."""
    up = np.diff(prices, prepend=prices[0]) >= 0
    buy_prob = np.where(up, 0.65, 0.35)
    return np.where(rng.random(len(prices)) < buy_prob, "BUY", "SELL")
