UV ?= uv
PODMAN ?= podman
PODMAN_COMPOSE ?= $(CURDIR)/.venv/bin/podman-compose
COMPOSE = $(PODMAN_COMPOSE) --podman-path $(PODMAN)
COMPOSE_SERVICES = postgres redis minio fake-llm-primary fake-llm-secondary litellm prometheus grafana
ENV_FILE ?= .env
COMPOSE_WAIT_TIMEOUT ?= 180
AUDIT_REQUIREMENTS ?= .cache/audit-requirements.txt
export UV_CACHE_DIR ?= $(CURDIR)/.cache/uv
export PRE_COMMIT_HOME ?= $(CURDIR)/.cache/pre-commit

.PHONY: bootstrap sync format lint typecheck unit integration coverage audit check test compose-config compose-up compose-smoke compose-down

bootstrap:
	$(UV) python install 3.12
	$(UV) sync --all-packages --frozen

sync:
	$(UV) sync --all-packages

format:
	$(UV) run ruff format packages scripts tests services/fake-llm/app.py
	$(UV) run ruff check --fix packages scripts tests services/fake-llm/app.py

lint:
	$(UV) run ruff format --check packages scripts tests services/fake-llm/app.py
	$(UV) run ruff check packages scripts tests services/fake-llm/app.py

typecheck:
	$(UV) run mypy packages scripts tests

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

compose-config:
	$(COMPOSE) --env-file $(ENV_FILE) config --quiet

compose-up:
	$(COMPOSE) --env-file $(ENV_FILE) run --rm --no-deps -T prometheus-credentials
	$(COMPOSE) --env-file $(ENV_FILE) up --detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT) $(COMPOSE_SERVICES)
	$(COMPOSE) --env-file $(ENV_FILE) run --rm --no-deps -T minio-init

compose-smoke:
	$(UV) run python scripts/verify_local_stack.py --env-file $(ENV_FILE) --timeout-seconds $(COMPOSE_WAIT_TIMEOUT)

compose-down:
	$(COMPOSE) --env-file $(ENV_FILE) down
