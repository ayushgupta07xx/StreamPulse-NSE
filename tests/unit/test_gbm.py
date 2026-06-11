"""Unit tests: GBM synthesis engine."""

import numpy as np
import pytest
from generator.gbm import SESSION_SECONDS, DayBar, synth_price_path, synth_sides, synth_volume_path

BAR = DayBar(open=100.0, high=104.0, low=98.0, close=102.0, volume=5_000_000)


def test_path_length_and_endpoints():
    rng = np.random.default_rng(1)
    path = synth_price_path(BAR, rng)
    assert len(path) == SESSION_SECONDS
    assert path[0] == pytest.approx(BAR.open, rel=1e-9)
    assert path[-1] == pytest.approx(BAR.close, rel=1e-6)  # Brownian bridge pins close


def test_path_respects_day_range():
    rng = np.random.default_rng(2)
    path = synth_price_path(BAR, rng)
    assert path.max() <= BAR.high * 1.002
    assert path.min() >= BAR.low * 0.998


def test_same_seed_same_path():
    p1 = synth_price_path(BAR, np.random.default_rng(42))
    p2 = synth_price_path(BAR, np.random.default_rng(42))
    np.testing.assert_array_equal(p1, p2)


def test_different_seed_different_path():
    p1 = synth_price_path(BAR, np.random.default_rng(1))
    p2 = synth_price_path(BAR, np.random.default_rng(2))
    assert not np.array_equal(p1, p2)


def test_volume_sums_near_daily_volume():
    rng = np.random.default_rng(3)
    vols = synth_volume_path(BAR, rng)
    assert len(vols) == SESSION_SECONDS
    assert vols.min() >= 0
    # Poisson draw around the daily total: within 5%
    assert abs(vols.sum() - BAR.volume) / BAR.volume < 0.05


def test_sides_valid_and_momentum_biased():
    rng = np.random.default_rng(4)
    prices = synth_price_path(BAR, rng)
    sides = synth_sides(prices, rng)
    assert set(np.unique(sides)) <= {"BUY", "SELL"}
    up = np.diff(prices, prepend=prices[0]) >= 0
    buy_on_up = (sides[up] == "BUY").mean()
    assert buy_on_up > 0.55  # 65% nominal, generous tolerance
