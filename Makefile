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

# Placeholders — implemented on later build days
demo:
	@echo "make demo lands on Day 14 (scripts/inject_demo_anomalies.py)"

benchmark:
	@echo "make benchmark lands on Day 10 (tests/benchmarks)"

k8s:
	@echo "make k8s lands on Day 7 (kind + Helm umbrella chart)"
