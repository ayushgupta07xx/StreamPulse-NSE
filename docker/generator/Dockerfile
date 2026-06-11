# StreamPulse Python apps image: tick generator (default entrypoint) + batch
# ML jobs (retrain CronJob / predict loop override the command).
# Build context is the repo root (see docker-compose.yml).
FROM python:3.11-slim

WORKDIR /app

# hadolint ignore=DL3013
RUN pip install --no-cache-dir \
    "confluent-kafka>=2.6,<3" \
    "pydantic>=2.9,<3" \
    "numpy>=1.26,<2" \
    "pandas>=2.2,<3" \
    "pyarrow>=17,<18" \
    "prometheus-client>=0.21,<1" \
    "typer>=0.16,<1" \
    "scikit-learn>=1.5,<2" \
    "statsmodels>=0.14,<1" \
    "joblib>=1.4,<2" \
    "mlflow-skinny>=2.16,<3" \
    "clickhouse-connect>=0.8,<1" \
    "tzdata"

COPY apps/generator /app/generator
COPY apps/ml /app/ml
COPY scripts /app/scripts
# Reference data baked in (committed to the repo; ~1.2 MB). Compose bind-mounts
# ./data over this for dev; k8s uses the baked copy (ConfigMaps cap at 1 MB).
COPY data /data

ENV PYTHONPATH=/app \
    STREAMPULSE_DATA_DIR=/data

EXPOSE 8000
ENTRYPOINT ["python", "-m", "generator.main", "run"]
