# Legal & Data Provenance

## Summary

**StreamPulse NSE processes synthetic market data only.** No real-time NSE (National
Stock Exchange of India) tick data is consumed, stored, or redistributed by this
project.

## Why synthetic data

Real-time NSE tick data is licensed commercial data and is not freely or legally
available for redistribution. This project therefore generates synthetic ticks.

## How the synthetic data is produced

1. **Calibration input:** one year of *daily* OHLC (open/high/low/close) prices for
   Nifty 50 constituent stocks, fetched via the public
   [yfinance](https://github.com/ranaroussi/yfinance) library (Yahoo Finance data,
   `.NS` suffixed symbols). Daily OHLC is widely-published public reference data.
2. **Synthesis:** intraday ticks are generated with geometric Brownian motion
   (drift and volatility derived from each day's OHLC range) plus Poisson
   jump-diffusion events. The result is statistically plausible but **entirely
   artificial** — no generated tick ever occurred on a real exchange.
3. **Anomaly injection:** the generator deliberately injects anomalies (price
   spikes, level shifts, volatility bursts) at recorded timestamps so detection
   precision/recall can be benchmarked against ground truth.

## Optional "live mode"

The generator has an optional mode that polls yfinance 1-minute bars, which Yahoo
serves on a ~15-minute delay. This is used only for short demonstrations, respects
yfinance's terms of use, and is never redistributed.

## Trademarks

"NSE", "Nifty 50", and stock tickers are used nominatively to describe the data
domain being simulated. This project is not affiliated with, endorsed by, or
sponsored by NSE Indices Ltd or the National Stock Exchange of India.

## License

All original code in this repository is licensed under the Apache License 2.0
(see [LICENSE](LICENSE)).
