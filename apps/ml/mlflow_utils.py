"""MLflow helpers — local tracking server (docker compose `mlflow`, :5000)."""

from __future__ import annotations

import os

import mlflow

EXPERIMENT = "streampulse-anomaly"


def init_mlflow() -> None:
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment(EXPERIMENT)
