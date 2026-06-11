# Synthetic tick generator — slim runtime image.
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
    "typer>=0.12,<1" \
    "tzdata"

COPY apps/generator /app/generator

ENV PYTHONPATH=/app \
    STREAMPULSE_DATA_DIR=/data

EXPOSE 8000
ENTRYPOINT ["python", "-m", "generator.main", "run"]
