"""Unit tests: anomaly injector mutates paths and records truthful ground truth."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from generator.anomaly_injector import ANOMALY_TYPES, inject

IST = ZoneInfo("Asia/Kolkata")
N = 22_500


@pytest.fixture()
def day():
    rng = np.random.default_rng(7)
    prices = 100.0 + np.cumsum(rng.normal(0, 0.01, N))
    volumes = rng.poisson(200, N).astype(np.int64)
    base = datetime(2026, 1, 5, 9, 15, tzinfo=IST)
    timestamps = [base + timedelta(seconds=s) for s in range(N)]
    return prices, volumes, timestamps


@pytest.mark.parametrize("a_type", ANOMALY_TYPES)
def test_each_type_mutates_and_records(day, a_type):
    prices, volumes, timestamps = day
    p0, v0 = prices.copy(), volumes.copy()
    rec = inject("TCS", prices, volumes, timestamps, np.random.default_rng(11), a_type, 0)

    assert rec.anomaly_type == a_type
    assert rec.ticker == "TCS"
    assert rec.start_ts < rec.end_ts
    if a_type == "VOLUME_SURGE":
        assert not np.array_equal(volumes, v0)
        np.testing.assert_array_equal(prices, p0)  # price untouched
    else:
        assert not np.array_equal(prices, p0)


def test_injection_at_recorded_location(day):
    prices, volumes, timestamps = day
    p0 = prices.copy()
    rec = inject("INFY", prices, volumes, timestamps, np.random.default_rng(5), "PRICE_SPIKE", 1)
    start_idx = timestamps.index(rec.start_ts)
    changed = np.where(prices != p0)[0]
    assert changed.min() == start_idx  # mutation begins exactly at recorded ts


def test_prices_stay_positive(day):
    prices, volumes, timestamps = day
    for i, a_type in enumerate(ANOMALY_TYPES):
        inject("SBIN", prices, volumes, timestamps, np.random.default_rng(i), a_type, i)
    assert (prices > 0).all()
