UV ?= uv
PODMAN ?= podman
PODMAN_COMPOSE ?= $(CURDIR)/.venv/bin/podman-compose
COMPOSE = $(PODMAN_COMPOSE) --podman-path $(PODMAN)
COMPOSE_SERVICES = postgres redis minio fake-llm-primary fake-llm-secondary litellm prometheus grafana
FAKE_LLM_IMAGE ?= localhost/agent-platform-fake-llm:local
SANDBOX_IMAGE ?= localhost/agent-platform-sandbox:sequence-10
ENV_FILE ?= .env
COMPOSE_WAIT_TIMEOUT ?= 180
AUDIT_REQUIREMENTS ?= .cache/audit-requirements.txt
export UV_CACHE_DIR ?= $(CURDIR)/.cache/uv
export PRE_COMMIT_HOME ?= $(CURDIR)/.cache/pre-commit

.PHONY: bootstrap sync format lint typecheck unit integration coverage audit check test load-sim chaos-sim podman-images sandbox-security gateway-security postgres-security migrate migration-check api compose-config compose-up compose-smoke compose-down

bootstrap:
	$(UV) python install 3.12
	$(UV) sync --all-packages --frozen

sync:
	$(UV) sync --all-packages

format:
	$(UV) run ruff format apps packages scripts tests services/fake-llm/app.py
	$(UV) run ruff check --fix apps packages scripts tests services/fake-llm/app.py

lint:
	$(UV) run ruff format --check apps packages scripts tests services/fake-llm/app.py
	$(UV) run ruff check apps packages scripts tests services/fake-llm/app.py

typecheck:
	$(UV) run mypy apps packages scripts tests

unit:
	$(UV) run pytest tests/unit

integration:
	$(UV) run pytest tests/integration

coverage:
	$(UV) run pytest tests/unit --cov --cov-report=term-missing --cov-report=xml

audit:
	$(UV) export --all-packages --frozen --no-emit-workspace --no-hashes --output-file $(AUDIT_REQUIREMENTS)
	$(UV) run pip-audit --requirement $(AUDIT_REQUIREMENTS) --no-deps --disable-pip --cache-dir .cache/pip-audit

check: lint typecheck coverage

test: check integration

load-sim:
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile simulation-10 --mode simulation --report benchmarks/reports/load-simulation.json

chaos-sim:
	$(UV) run python -m scripts.chaos_test --scenarios benchmarks/chaos/scenarios.yaml --mode simulation --report benchmarks/reports/chaos-simulation.json

podman-images:
	$(PODMAN) build --tag $(FAKE_LLM_IMAGE) --file services/fake-llm/Containerfile services/fake-llm
	$(PODMAN) build --tag $(SANDBOX_IMAGE) --file services/sandbox/Containerfile services/sandbox

sandbox-security:
	AGENT_PLATFORM_RUN_PODMAN_SECURITY=1 AGENT_PLATFORM_SANDBOX_IMAGE=$(SANDBOX_IMAGE) $(UV) run pytest -W error::pytest.PytestUnraisableExceptionWarning tests/security/test_podman_sandbox_security.py

gateway-security:
	AGENT_PLATFORM_RUN_PODMAN_GATEWAY=1 AGENT_PLATFORM_PODMAN_ENV_FILE=$(ENV_FILE) $(UV) run pytest tests/security/test_litellm_podman_deployment.py

postgres-security:
	AGENT_PLATFORM_RUN_POSTGRES_INTEGRATION=1 $(UV) run pytest tests/security/test_postgres_persistence.py

migrate:
	$(UV) run --env-file $(ENV_FILE) alembic upgrade head

migration-check:
	$(UV) run --env-file $(ENV_FILE) alembic check

api:
	$(UV) run --env-file $(ENV_FILE) uvicorn agent_api.factory:create_production_app --factory --host 127.0.0.1 --port 8000

compose-config:
	$(COMPOSE) --env-file $(ENV_FILE) config --quiet

compose-up: podman-images
	$(COMPOSE) --env-file $(ENV_FILE) run --rm --no-deps -T prometheus-credentials
	$(COMPOSE) --env-file $(ENV_FILE) up --detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT) $(COMPOSE_SERVICES)
	$(COMPOSE) --env-file $(ENV_FILE) run --rm --no-deps -T minio-init

compose-smoke:
	$(UV) run python scripts/verify_local_stack.py --env-file $(ENV_FILE) --timeout-seconds $(COMPOSE_WAIT_TIMEOUT)

compose-down:
	$(COMPOSE) --env-file $(ENV_FILE) down
