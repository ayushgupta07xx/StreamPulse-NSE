"""StreamPulse synthetic tick generator.

Calibrates on real Nifty 50 daily OHLC (yfinance) and synthesizes intraday
ticks via geometric Brownian motion + jump-diffusion, with deliberate anomaly
injection for detection benchmarking. See LEGAL.md.
"""

__version__ = "0.1.0"
