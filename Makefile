# TraceFleet developer entrypoints.
#
#     cp .env.example .env && make up && make demo
#
# `make help` lists every target. PROFILE selects the scale profile
# (dev | demo | full) and is threaded through to the stack and the simulator.

SHELL := /bin/bash
.DEFAULT_GOAL := help

PROFILE ?= dev
COMPOSE := docker compose
PY ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python)
ARTIFACTS := artifacts
export PYTHONPATH := $(CURDIR):$(CURDIR)/apps/stream-processor:$(CURDIR)/apps/simulator

# Anything that needs the owner role (migrations, retention, erasure) runs inside the
# `migrate` service, which is configured with POSTGRES_OWNER_URL.
OWNER_RUN := $(COMPOSE) run --rm --no-deps migrate

.PHONY: help up down restart logs ps clean nuke migrate migrate-down topics seed demo \
        simulate simulate-scenario simulate-burst control-api \
        test test-unit test-int test-bdd test-contract test-ai test-security coverage \
        lint format typecheck openapi gen-types feature-list plugins \
        load-test soak chaos-kill-consumer chaos-kill-broker chaos-recover \
        scan bench retention erasure evaluate reset k8s-dev k8s-prod k8s-validate

# ---------------------------------------------------------------------------
help:
	@echo "TraceFleet ($(PROFILE) profile)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# --- stack -----------------------------------------------------------------
up: ## Start the stack and wait for health (runs migrations first)
	PROFILE=$(PROFILE) $(COMPOSE) up -d --build --wait
	@echo ""
	@echo "  Dashboard  http://localhost:5173"
	@echo "  API docs   http://localhost:8000/docs"
	@echo "  Health     http://localhost:8000/api/v1/health"
	@echo "  Grafana    http://localhost:3000  (admin/admin)"
	@echo "  Prometheus http://localhost:9090"
	@echo "  Tempo      http://localhost:3200"

down: ## Stop the stack, keeping volumes
	$(COMPOSE) down

restart: down up ## Restart the stack

logs: ## Follow logs from every service
	$(COMPOSE) logs -f --tail=100

ps: ## Show service status
	$(COMPOSE) ps

clean: ## Stop and remove containers, keeping volumes
	$(COMPOSE) down --remove-orphans

nuke: ## Stop and DELETE ALL DATA (volumes included)
	@echo "This deletes every Postgres, ClickHouse, Kafka and Redis volume."
	@read -p "Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ]
	$(COMPOSE) down -v --remove-orphans

# --- schema ----------------------------------------------------------------
migrate: ## Apply database migrations
	$(OWNER_RUN) alembic upgrade head

migrate-down: ## Roll back one migration
	$(OWNER_RUN) alembic downgrade -1

topics: ## Create or widen Kafka topics for the active profile
	PROFILE=$(PROFILE) $(COMPOSE) up --force-recreate kafka-init

# --- data ------------------------------------------------------------------
seed: ## Seed tenants, fleets and vehicles for the active profile
	PROFILE=$(PROFILE) $(COMPOSE) run --rm simulator \
		python apps/simulator/sim/main.py --profile $(PROFILE) --seed-fleet

simulate: ## Stream baseline telemetry at the profile's target rate
	PROFILE=$(PROFILE) $(COMPOSE) run --rm simulator \
		python apps/simulator/sim/main.py kafka:9092 --profile $(PROFILE) --duration 60

simulate-scenario: ## Inject one scenario (SCENARIO=collision_like COUNT=1)
	PROFILE=$(PROFILE) $(COMPOSE) run --rm simulator \
		python apps/simulator/sim/main.py kafka:9092 --profile $(PROFILE) \
		--scenario $(or $(SCENARIO),collision_like) --batches $(or $(COUNT),1)

simulate-burst: ## Stream with a 3x burst window
	PROFILE=$(PROFILE) $(COMPOSE) run --rm simulator \
		python apps/simulator/sim/main.py kafka:9092 --profile $(PROFILE) \
		--duration 420 --burst --burst-after 60 \
		--report /app/$(ARTIFACTS)/burst-report.json

control-api: ## Serve the simulator control API on :8100
	SIMULATOR_CONTROL_ENABLED=true PROFILE=$(PROFILE) $(COMPOSE) run --rm --service-ports simulator \
		python apps/simulator/sim/main.py kafka:9092 --control-api

reset: ## Clear demo data, keeping the schema
	$(COMPOSE) exec -T api python scripts/reset_demo_data.py

demo: up migrate seed ## Full demo: start, seed, inject a mixed scenario set
	@$(MAKE) --no-print-directory simulate-scenario SCENARIO=mixed COUNT=6
	@echo ""
	@echo "  Injected 6 scenario instances. Follow docs/DEMO_SCRIPT.md."
	@echo "  Dashboard  http://localhost:5173"
	@echo "  Login      analyst / tracefleet-demo"

# --- tests -----------------------------------------------------------------
test: test-unit test-contract test-bdd test-ai test-security ## Run every test that needs no external stack

test-unit: ## Unit tests
	$(PY) -m pytest tests/unit -q

test-int: ## Integration tests (requires the stack to be up)
	$(COMPOSE) exec -T -e TRACEFLEET_INTEGRATION=1 -e OTEL_SDK_DISABLED=true api python -m pytest tests/integration -q

test-bdd: ## BDD scenarios from the PRD
	$(PY) -m pytest tests/bdd -q

test-contract: ## OpenAPI and Kafka schema contract tests
	$(PY) -m pytest tests/contract -q

test-ai: ## AI safety suite (prompt injection, hallucination, fault language)
	$(PY) -m pytest tests/ai_safety -q

test-security: ## Tenant isolation and RBAC tests
	$(PY) -m pytest tests/security -q

coverage: ## Complete real-store coverage with the 80% gate (requires stack)
	$(COMPOSE) exec -T -e TRACEFLEET_INTEGRATION=1 -e OTEL_SDK_DISABLED=true api python -m pytest tests \
		--cov=packages --cov=apps --cov-report=term-missing \
		--cov-report=html:artifacts/coverage --cov-report=json:artifacts/coverage-full.json \
		--cov-report=xml:artifacts/coverage.xml --cov-fail-under=80

# --- quality ---------------------------------------------------------------
lint: ## ruff + compile check + web build
	$(PY) -m ruff check packages apps scripts tests
	$(PY) -m compileall -q packages apps scripts
	npm --prefix apps/web run build

format: ## Apply ruff formatting and import sorting
	$(PY) -m ruff check --fix packages apps scripts tests
	$(PY) -m ruff format packages apps scripts tests

typecheck: ## mypy on packages/, tsc on the web app
	$(PY) -m mypy
	npm --prefix apps/web run typecheck

# --- api contract ----------------------------------------------------------
openapi: ## Export the OpenAPI document
	@mkdir -p docs/api
	curl --fail -s http://localhost:8000/openapi.json | $(PY) -m json.tool > docs/api/openapi.json
	@echo "wrote docs/api/openapi.json"

gen-types: openapi ## Generate TypeScript types from OpenAPI
	npx --yes openapi-typescript docs/api/openapi.json -o apps/web/src/api/schema.ts

feature-list: ## List features and their state
	$(PY) scripts/feature.py list

plugins: ## List every registered plug-in
	python apps/simulator/sim/main.py --list-scenarios

# --- performance -----------------------------------------------------------
load-test: ## k6 load test against the API
	@mkdir -p $(ARTIFACTS)
	LOCAL_UID=$$(id -u) LOCAL_GID=$$(id -g) $(COMPOSE) run --rm k6 run --summary-export=/artifacts/k6-summary.json /tests/load.js

bench: ## Measure ingest throughput and write docs/BENCHMARKS.md
	@mkdir -p $(ARTIFACTS)
	$(COMPOSE) exec -T api python scripts/benchmark.py --profile $(PROFILE) --duration 60

soak: ## Sample live health, lag and memory (SOAK_MINUTES=30)
	$(PY) -m scripts.soak --minutes $(or $(SOAK_MINUTES),30)

chaos-kill-consumer: ## Interrupt mid-scenario, assert durable recovery
	$(PY) -m scripts.recovery_check --service incident-processor --report artifacts/chaos-consumer.json

chaos-kill-broker: ## Restart broker and reconcile queued telemetry
	$(PY) -m scripts.recovery_check --service kafka --report artifacts/chaos-broker.json

chaos-recover: chaos-kill-consumer chaos-kill-broker ## Full local recovery checks

# --- security --------------------------------------------------------------
scan: ## bandit, pip-audit, npm audit, trivy
	@mkdir -p $(ARTIFACTS)
	bash tests/security/scan.sh

# --- compliance jobs -------------------------------------------------------
retention: ## Apply retention policies (RETENTION_ARGS=--dry-run to preview)
	@mkdir -p $(ARTIFACTS)
	$(OWNER_RUN) python scripts/retention_job.py $(or $(RETENTION_ARGS),--verify) \
		--report /app/$(ARTIFACTS)/retention-report.json

erasure: ## Process pending erasure requests
	@mkdir -p $(ARTIFACTS)
	$(OWNER_RUN) python scripts/erasure_job.py \
		--report /app/$(ARTIFACTS)/erasure-report.json

evaluate: ## Score detection against the simulator's hidden ground truth
	@mkdir -p $(ARTIFACTS)
	$(OWNER_RUN) python scripts/evaluate_reconstruction.py \
		--report /app/$(ARTIFACTS)/accuracy-report.json --minimum 0.95

# --- kubernetes ------------------------------------------------------------
k8s-validate: ## Render both overlays without applying
	kubectl kustomize --load-restrictor LoadRestrictionsNone infra/k8s/overlays/dev > /dev/null && echo "dev overlay renders"
	kubectl kustomize --load-restrictor LoadRestrictionsNone infra/k8s/overlays/prod > /dev/null && echo "prod overlay renders"

k8s-dev: ## Apply the dev overlay
	kubectl kustomize --load-restrictor LoadRestrictionsNone infra/k8s/overlays/dev | kubectl apply -f -

k8s-prod: ## Apply the prod overlay
	kubectl kustomize --load-restrictor LoadRestrictionsNone infra/k8s/overlays/prod | kubectl apply -f -
