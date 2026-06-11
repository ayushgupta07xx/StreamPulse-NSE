"""Daily Isolation Forest retrain on multivariate bar features (§14 method 3).

Reads the last N days of 5-minute bars from ClickHouse, builds features
(feature_builder.FEATURES), trains IsolationForest(contamination=0.01) inside
a StandardScaler pipeline, logs everything to MLflow, and writes
models/isolation_forest_latest.joblib (+ a version-stamped copy).

Validation: the most recent session day is held out; we report its flag rate
and score distribution. (Precision/recall against injected ground truth is
computed in the Day 10 benchmark, where labels are joined.)

Run:    python -m ml.isolation_forest_retrain --days 7
Cron:   .github/workflows/scheduled-retrain.yml (nightly)
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import joblib
import mlflow
import numpy as np
import typer
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from ml.feature_builder import FEATURES, build_features, load_bars
from ml.mlflow_utils import init_mlflow

MODELS_DIR = Path(os.environ.get("STREAMPULSE_MODELS_DIR", "models"))

app = typer.Typer(add_completion=False)


@app.callback()
def _root() -> None:
    """Isolation Forest retraining job."""


@app.command()
def retrain(
    days: int = typer.Option(7, help="training window in days"),
    contamination: float = typer.Option(0.01),
    n_estimators: int = typer.Option(200),
    bar_size: str = typer.Option("5m"),
) -> None:
    init_mlflow()
    bars = load_bars(days, bar_size)
    feats = build_features(bars)
    if len(feats) < 500:
        typer.echo(f"not enough feature rows ({len(feats)}) — need >=500")
        raise typer.Exit(code=2)

    # hold out the latest session date for validation
    feats["date"] = feats["window_start"].dt.date
    val_date = feats["date"].max()
    train = feats[feats["date"] < val_date]
    val = feats[feats["date"] == val_date]
    if len(train) < 300:  # single-day corpus — train on all, validate in-sample
        train, val = feats, feats

    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "iforest",
                IsolationForest(
                    n_estimators=n_estimators,
                    contamination=contamination,
                    random_state=42,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    with mlflow.start_run(run_name=f"iforest-{datetime.now(UTC):%Y%m%d-%H%M}") as run:
        model.fit(train[FEATURES])
        val_scores = model.decision_function(val[FEATURES])
        val_flags = (model.predict(val[FEATURES]) == -1).mean()

        mlflow.log_params(
            {
                "days": days,
                "contamination": contamination,
                "n_estimators": n_estimators,
                "bar_size": bar_size,
                "n_train": len(train),
                "n_val": len(val),
                "features": ",".join(FEATURES),
            }
        )
        mlflow.log_metrics(
            {
                "val_flag_rate": float(val_flags),
                "val_score_p01": float(np.percentile(val_scores, 1)),
                "val_score_p50": float(np.percentile(val_scores, 50)),
                "val_score_p99": float(np.percentile(val_scores, 99)),
            }
        )
        mlflow.sklearn.log_model(model, "model")

        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        version = run.info.run_id[:8]
        joblib.dump(model, MODELS_DIR / f"isolation_forest_{version}.joblib")
        joblib.dump(model, MODELS_DIR / "isolation_forest_latest.joblib")
        (MODELS_DIR / "isolation_forest_latest.json").write_text(
            json.dumps(
                {
                    "version": version,
                    "mlflow_run_id": run.info.run_id,
                    "trained_at": datetime.now(UTC).isoformat(),
                    "n_train": len(train),
                    "features": FEATURES,
                }
            )
        )
        typer.echo(
            f"trained iforest v{version}: train={len(train)} val={len(val)} "
            f"val_flag_rate={val_flags:.3%}"
        )


if __name__ == "__main__":
    app()
