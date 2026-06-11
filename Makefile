# StreamPulse NSE — operational targets
# Windows note: requires GNU Make; all recipes are plain docker/git commands.

.PHONY: up down ps logs smoke build demo benchmark k8s

up:
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

build:
	docker compose build

# Produce + consume one message through Redpanda to prove the bus works
smoke:
	docker exec streampulse-redpanda rpk topic create smoke-test --partitions 1 -r 1
	docker exec streampulse-redpanda sh -c "echo smoke-$$RANDOM | rpk topic produce smoke-test"
	docker exec streampulse-redpanda rpk topic consume smoke-test --num 1 --offset start
	docker exec streampulse-redpanda rpk topic delete smoke-test

# Submit all Flink jobs (detached). OOO=<seconds> widens watermarks for fast replays.
flink-jobs:
	docker exec streampulse-flink-jm flink run -py /opt/streampulse/flink/jobs/validate_enrich.py --pyFiles /opt/streampulse/flink/jobs -d
	docker exec streampulse-flink-jm flink run -py /opt/streampulse/flink/jobs/window_bars.py --pyFiles /opt/streampulse/flink/jobs -d $(if $(OOO),--ooo-seconds $(OOO),)
	docker exec streampulse-flink-jm flink run -py /opt/streampulse/flink/jobs/anomaly_online.py --pyFiles /opt/streampulse/flink/jobs -d $(if $(OOO),--ooo-seconds $(OOO),)

# Full event-time reset: cancel jobs, wipe topics+groups, resubmit
reset:
	.venv/Scripts/python.exe scripts/reset_pipeline.py $(if $(OOO),--ooo-seconds $(OOO),)

verify-bars:
	.venv/Scripts/python.exe scripts/verify_bars.py

# 5-minute live demo: wall-clock ticks + a visible spike on TCS.
# Open Grafana (localhost:23000) -> Market Overview + Anomaly Feed first.
demo:
	.venv/Scripts/python.exe scripts/inject_demo_anomalies.py --ticker TCS --minutes 5

# Detection accuracy benchmark against injected ground truth
benchmark:
	.venv/Scripts/python.exe tests/benchmarks/evaluate_detection.py --markdown

# Copy SQL / job sources / dashboards / reference data into chart files/ dirs
# (charts must be self-contained for helm package + .Files access)
sync-helm-files:
	python -c "import shutil, pathlib; \
	[shutil.copytree(s, d, dirs_exist_ok=True) if pathlib.Path(s).is_dir() else shutil.copy2(s, d) for s, d in [ \
	('clickhouse', 'helm/charts/clickhouse/files'), \
	('apps/flink/jobs', 'helm/charts/flink/files/jobs'), \
	('observability/grafana/dashboards', 'helm/charts/grafana/files/dashboards'), \
	('data', 'helm/charts/streampulse-generator/files/data')]]"
	python -c "import shutil; shutil.copytree('apps/flink/jobs/common', 'helm/charts/flink/files/common', dirs_exist_ok=True); shutil.rmtree('helm/charts/flink/files/jobs/common', ignore_errors=True)"
	python -c "import shutil; shutil.rmtree('helm/charts/clickhouse/files/init', ignore_errors=True); shutil.rmtree('helm/charts/clickhouse/files/config', ignore_errors=True)"

# kind cluster + umbrella install (Day 7)
k8s: sync-helm-files
	-kind create cluster --name streampulse --config helm/kind-cluster.yaml
	kind load docker-image streampulse/flink:1.18-py --name streampulse
	kind load docker-image streampulse/generator:dev --name streampulse
	helm dependency update helm/charts/streampulse-platform
	helm upgrade --install streampulse helm/charts/streampulse-platform --wait --timeout 10m

k8s-down:
	kind delete cluster --name streampulse
