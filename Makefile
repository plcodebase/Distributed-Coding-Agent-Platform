UV ?= uv
PODMAN ?= podman
PODMAN_COMPOSE ?= $(CURDIR)/.venv/bin/podman-compose
COMPOSE = $(PODMAN_COMPOSE) --podman-path $(PODMAN)
COMPOSE_SERVICES = postgres redis minio fake-llm-primary fake-llm-secondary litellm prometheus grafana
FAKE_LLM_IMAGE ?= localhost/agent-platform-fake-llm:local
SANDBOX_IMAGE ?= localhost/agent-platform-sandbox:sequence-10
PLATFORM_IMAGE ?= localhost/agent-platform:local
NODE_IMAGE ?= localhost/agent-platform-node:local
ENV_FILE ?= .env
COMPOSE_WAIT_TIMEOUT ?= 180
AUDIT_REQUIREMENTS ?= .cache/audit-requirements.txt
BENCHMARK_SOURCE_REVISION ?= unavailable
BENCHMARK_SOURCE_DIRTY ?= --source-dirty
RELEASE_EVIDENCE_DIR ?= $(CURDIR)/release-evidence
RELEASE_REGISTRY_PREFIX ?=
RELEASE_SOURCE_REVISION ?=
RELEASE_SOURCE_URI ?=
RELEASE_CERTIFICATE_IDENTITY ?=
RELEASE_CERTIFICATE_OIDC_ISSUER ?= https://token.actions.githubusercontent.com
RELEASE_GIT ?=
RELEASE_SYFT ?=
RELEASE_GRYPE ?=
RELEASE_COSIGN ?=
RELEASE_GIT_SHA256 ?=
RELEASE_PODMAN_SHA256 ?=
RELEASE_SYFT_SHA256 ?=
RELEASE_GRYPE_SHA256 ?=
RELEASE_COSIGN_SHA256 ?=
RELEASE_PYTHON_SLIM_IMAGE ?=
RELEASE_PYTHON_ALPINE_IMAGE ?=
RELEASE_LITELLM_IMAGE ?=
LIFECYCLE_ARGS ?=
RECOVERY_ARGS ?=
ACCEPTANCE_REVISION ?= $(shell git rev-parse HEAD)
ACCEPTANCE_DIR ?= $(CURDIR)/.cache/acceptance/$(ACCEPTANCE_REVISION)
LOCAL_ACCEPTANCE_PROJECT ?= agent-platform-local-acceptance
export UV_CACHE_DIR ?= $(CURDIR)/.cache/uv
export PRE_COMMIT_HOME ?= $(CURDIR)/.cache/pre-commit

.PHONY: bootstrap sync format lint typecheck unit integration coverage audit lock-check build-packages pre-commit check test release-check release-contract release-tool-contract release-evidence release-evidence-verify load-sim load-sim-all chaos-sim kubernetes-contract deployment-sim coding-sim quality-report final-compile final-baseline acceptance-sim podman-preflight podman-images podman-production-images sandbox-security gateway-security postgres-security redis-security local-stack-acceptance e2e-happy distributed-e2e local-acceptance migrate migration-check lifecycle-admin recovery-verify api compose-config compose-up compose-smoke compose-down

bootstrap:
	$(UV) python install 3.12
	$(UV) sync --all-packages --frozen

sync:
	$(UV) sync --all-packages

format:
	$(UV) run ruff format apps packages scripts tests services/fake-llm
	$(UV) run ruff check --fix apps packages scripts tests services/fake-llm

lint:
	$(UV) run ruff format --check apps packages scripts tests services/fake-llm
	$(UV) run ruff check apps packages scripts tests services/fake-llm

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

lock-check:
	$(UV) lock --check

build-packages:
	$(UV) build --all-packages --out-dir dist --clear

pre-commit:
	$(UV) run pre-commit run --all-files

check: lint typecheck coverage

test: check integration

release-check: release-contract test pre-commit audit lock-check build-packages

release-contract:
	$(UV) run python -m scripts.release_supply_chain validate --repository .

release-tool-contract:
	$(UV) run python -m scripts.release_supply_chain verify-tools \
		--tool "git=$(RELEASE_GIT)" \
		--tool "podman=$(PODMAN)" \
		--tool "syft=$(RELEASE_SYFT)" \
		--tool "grype=$(RELEASE_GRYPE)" \
		--tool "cosign=$(RELEASE_COSIGN)" \
		--tool-sha256 "git=$(RELEASE_GIT_SHA256)" \
		--tool-sha256 "podman=$(RELEASE_PODMAN_SHA256)" \
		--tool-sha256 "syft=$(RELEASE_SYFT_SHA256)" \
		--tool-sha256 "grype=$(RELEASE_GRYPE_SHA256)" \
		--tool-sha256 "cosign=$(RELEASE_COSIGN_SHA256)"

release-evidence:
	$(UV) run python -m scripts.release_supply_chain build \
		--repository . \
		--inventory release/images.yaml \
		--output "$(RELEASE_EVIDENCE_DIR)" \
		--registry-prefix "$(RELEASE_REGISTRY_PREFIX)" \
		--source-revision "$(RELEASE_SOURCE_REVISION)" \
		--source-uri "$(RELEASE_SOURCE_URI)" \
		--certificate-identity "$(RELEASE_CERTIFICATE_IDENTITY)" \
		--certificate-oidc-issuer "$(RELEASE_CERTIFICATE_OIDC_ISSUER)" \
		--material "python-slim=$(RELEASE_PYTHON_SLIM_IMAGE)" \
		--material "python-alpine=$(RELEASE_PYTHON_ALPINE_IMAGE)" \
		--material "litellm=$(RELEASE_LITELLM_IMAGE)" \
		--tool "git=$(RELEASE_GIT)" \
		--tool "podman=$(PODMAN)" \
		--tool "syft=$(RELEASE_SYFT)" \
		--tool "grype=$(RELEASE_GRYPE)" \
		--tool "cosign=$(RELEASE_COSIGN)" \
		--tool-sha256 "git=$(RELEASE_GIT_SHA256)" \
		--tool-sha256 "podman=$(RELEASE_PODMAN_SHA256)" \
		--tool-sha256 "syft=$(RELEASE_SYFT_SHA256)" \
		--tool-sha256 "grype=$(RELEASE_GRYPE_SHA256)" \
		--tool-sha256 "cosign=$(RELEASE_COSIGN_SHA256)"

release-evidence-verify:
	$(UV) run python -m scripts.release_supply_chain verify \
		--evidence "$(RELEASE_EVIDENCE_DIR)" \
		--expected-source-revision "$(RELEASE_SOURCE_REVISION)" \
		--certificate-identity "$(RELEASE_CERTIFICATE_IDENTITY)" \
		--certificate-oidc-issuer "$(RELEASE_CERTIFICATE_OIDC_ISSUER)" \
		--cosign "$(RELEASE_COSIGN)" \
		--cosign-sha256 "$(RELEASE_COSIGN_SHA256)"

load-sim:
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile simulation-10 --mode simulation --report benchmarks/reports/load-simulation.json

load-sim-all:
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile api-50 --mode simulation --report benchmarks/reports/load-simulation-api.json
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile websocket-50 --mode simulation --report benchmarks/reports/load-simulation-websocket.json
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile event-throughput-50 --mode simulation --report benchmarks/reports/load-simulation-events.json
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile worker-saturation-100 --mode simulation --report benchmarks/reports/load-simulation-workers.json
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile gateway-rate-limit-50 --mode simulation --report benchmarks/reports/load-simulation-rate-limit.json
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile provider-fallback-50 --mode simulation --report benchmarks/reports/load-simulation-fallback.json
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile postgres-contention-50 --mode simulation --report benchmarks/reports/load-simulation-postgres.json
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile redis-contention-50 --mode simulation --report benchmarks/reports/load-simulation-redis.json
	$(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile sandbox-saturation-50 --mode simulation --report benchmarks/reports/load-simulation-sandbox.json

chaos-sim:
	$(UV) run python -m scripts.chaos_test --scenarios benchmarks/chaos/scenarios.yaml --mode simulation --report benchmarks/reports/chaos-simulation.json

kubernetes-contract:
	$(UV) run python -m scripts.kubernetes_contracts deployments/kubernetes/base
	$(UV) run python -c "from pathlib import Path; from scripts.kubernetes_contracts import validate_production_admission_contract; validate_production_admission_contract(Path('deployments/kubernetes/production'))"

deployment-sim:
	$(UV) run python -m scripts.deployment_test --mode simulation --campaign-id baseline-deployment --source-revision unavailable --output benchmarks/reports/deployment-simulation.json

coding-sim:
	$(UV) run python -m scripts.coding_evaluation --mode simulation --campaign-id baseline-coding --source-revision unavailable --output benchmarks/reports/coding-simulation.json

quality-report:
	$(UV) run python -m scripts.quality_gate_report --repository . --source-revision $(BENCHMARK_SOURCE_REVISION) $(BENCHMARK_SOURCE_DIRTY) --output benchmarks/reports/quality-gates.json

final-compile:
	$(UV) run python -m scripts.final_benchmark_report benchmarks/reports/deployment-simulation.json benchmarks/reports/coding-simulation.json benchmarks/reports/load-simulation-api.json benchmarks/reports/load-simulation-websocket.json benchmarks/reports/load-simulation-events.json benchmarks/reports/load-simulation-workers.json benchmarks/reports/load-simulation-rate-limit.json benchmarks/reports/load-simulation-fallback.json benchmarks/reports/load-simulation-postgres.json benchmarks/reports/load-simulation-redis.json benchmarks/reports/load-simulation-sandbox.json benchmarks/reports/chaos-simulation.json benchmarks/reports/quality-gates.json --expected-revision $(BENCHMARK_SOURCE_REVISION) --json-output benchmarks/reports/final-benchmark-report.json --markdown-output benchmarks/reports/final-benchmark-report.md

final-baseline: deployment-sim coding-sim load-sim-all chaos-sim quality-report final-compile

acceptance-sim:
	mkdir -p "$(ACCEPTANCE_DIR)"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.deployment_test --mode simulation --campaign-id local-acceptance-deployment --source-revision "$(ACCEPTANCE_REVISION)" --output "$(ACCEPTANCE_DIR)/deployment.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.coding_evaluation --mode simulation --campaign-id local-acceptance-coding --source-revision "$(ACCEPTANCE_REVISION)" --output "$(ACCEPTANCE_DIR)/coding.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile api-50 --mode simulation --report "$(ACCEPTANCE_DIR)/load-api.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile websocket-50 --mode simulation --report "$(ACCEPTANCE_DIR)/load-websocket.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile event-throughput-50 --mode simulation --report "$(ACCEPTANCE_DIR)/load-events.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile worker-saturation-100 --mode simulation --report "$(ACCEPTANCE_DIR)/load-workers.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile gateway-rate-limit-50 --mode simulation --report "$(ACCEPTANCE_DIR)/load-rate-limit.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile provider-fallback-50 --mode simulation --report "$(ACCEPTANCE_DIR)/load-fallback.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile postgres-contention-50 --mode simulation --report "$(ACCEPTANCE_DIR)/load-postgres.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile redis-contention-50 --mode simulation --report "$(ACCEPTANCE_DIR)/load-redis.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.load_test --profiles benchmarks/gateway_load/profiles.yaml --profile sandbox-saturation-50 --mode simulation --report "$(ACCEPTANCE_DIR)/load-sandbox.json"
	AGENT_PLATFORM_BENCHMARK_GIT_REVISION="$(ACCEPTANCE_REVISION)" AGENT_PLATFORM_BENCHMARK_GIT_DIRTY=true $(UV) run python -m scripts.chaos_test --scenarios benchmarks/chaos/scenarios.yaml --mode simulation --report "$(ACCEPTANCE_DIR)/chaos.json"
	$(UV) run python -m scripts.quality_gate_report --repository . --source-revision "$(ACCEPTANCE_REVISION)" --source-dirty --output "$(ACCEPTANCE_DIR)/quality-gates.json"
	$(UV) run python -m scripts.final_benchmark_report "$(ACCEPTANCE_DIR)/deployment.json" "$(ACCEPTANCE_DIR)/coding.json" "$(ACCEPTANCE_DIR)/load-api.json" "$(ACCEPTANCE_DIR)/load-websocket.json" "$(ACCEPTANCE_DIR)/load-events.json" "$(ACCEPTANCE_DIR)/load-workers.json" "$(ACCEPTANCE_DIR)/load-rate-limit.json" "$(ACCEPTANCE_DIR)/load-fallback.json" "$(ACCEPTANCE_DIR)/load-postgres.json" "$(ACCEPTANCE_DIR)/load-redis.json" "$(ACCEPTANCE_DIR)/load-sandbox.json" "$(ACCEPTANCE_DIR)/chaos.json" "$(ACCEPTANCE_DIR)/quality-gates.json" --expected-revision "$(ACCEPTANCE_REVISION)" --json-output "$(ACCEPTANCE_DIR)/final-report.json" --markdown-output "$(ACCEPTANCE_DIR)/final-report.md"

podman-preflight:
	@rootless="$$($(PODMAN) info --format '{{.Host.Security.Rootless}}')"; \
	test "$$rootless" = "true" || { echo "A healthy rootless Podman engine is required." >&2; exit 1; }

podman-images:
	$(PODMAN) build --tag $(FAKE_LLM_IMAGE) --file services/fake-llm/Containerfile services/fake-llm
	$(PODMAN) build --tag $(SANDBOX_IMAGE) --file services/sandbox/Containerfile services/sandbox

podman-production-images:
	$(PODMAN) build --tag $(PLATFORM_IMAGE) --file services/platform/Containerfile .
	$(PODMAN) build --tag $(NODE_IMAGE) --file services/node/Containerfile .

sandbox-security: podman-preflight podman-images
	AGENT_PLATFORM_RUN_PODMAN_SECURITY=1 AGENT_PLATFORM_SANDBOX_IMAGE=$(SANDBOX_IMAGE) $(UV) run pytest -W error::pytest.PytestUnraisableExceptionWarning tests/security/test_podman_sandbox_security.py

gateway-security: podman-preflight podman-images
	AGENT_PLATFORM_RUN_PODMAN_GATEWAY=1 AGENT_PLATFORM_PODMAN_ENV_FILE=$(ENV_FILE) $(UV) run pytest tests/security/test_litellm_podman_deployment.py

postgres-security:
	AGENT_PLATFORM_RUN_POSTGRES_INTEGRATION=1 $(UV) run pytest tests/security/test_postgres_persistence.py

redis-security: podman-preflight
	@set -eu; \
	project=agent-platform-redis-security; \
	cleanup() { \
		status=$$?; trap - EXIT INT TERM; cleanup_status=0; \
		for container in "$${project}_redis_1" "$${project}_postgres_1"; do \
			if $(PODMAN) container exists "$$container"; then $(PODMAN) rm --force "$$container" || cleanup_status=$$?; fi; \
		done; \
		if $(PODMAN) network exists "$${project}_default"; then $(PODMAN) network rm "$${project}_default" || cleanup_status=$$?; fi; \
		if $(PODMAN) volume exists "$${project}_postgres-data"; then $(PODMAN) volume rm "$${project}_postgres-data" || cleanup_status=$$?; fi; \
		if [ "$$status" -eq 0 ]; then status=$$cleanup_status; fi; \
		exit "$$status"; \
	}; \
	trap cleanup EXIT INT TERM; \
	$(COMPOSE) --project-name "$$project" --env-file $(ENV_FILE) up --detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT) postgres redis; \
	$(UV) run --env-file $(ENV_FILE) alembic upgrade head; \
	AGENT_PLATFORM_RUN_REDIS_INTEGRATION=1 \
	AGENT_PLATFORM_REDIS_TEST_CONTAINER="$${project}_redis_1" \
	$(UV) run --env-file $(ENV_FILE) pytest -v tests/security/test_redis_wakeup_persistence.py

local-stack-acceptance: podman-preflight podman-images
	@set -eu; \
	project=$(LOCAL_ACCEPTANCE_PROJECT); \
	cleanup() { \
		status=$$?; trap - EXIT INT TERM; cleanup_status=0; \
		for service in grafana prometheus litellm fake-llm-secondary fake-llm-primary minio-init prometheus-credentials minio redis postgres; do \
			container="$${project}_$${service}_1"; \
			if $(PODMAN) container exists "$$container"; then $(PODMAN) rm --force "$$container" || cleanup_status=$$?; fi; \
		done; \
		for network in default llm-egress llm-upstreams; do \
			name="$${project}_$$network"; \
			if $(PODMAN) network exists "$$name"; then $(PODMAN) network rm "$$name" || cleanup_status=$$?; fi; \
		done; \
		for volume in postgres-data minio-data prometheus-data prometheus-secrets grafana-data; do \
			name="$${project}_$$volume"; \
			if $(PODMAN) volume exists "$$name"; then $(PODMAN) volume rm "$$name" || cleanup_status=$$?; fi; \
		done; \
		if [ "$$status" -eq 0 ]; then status=$$cleanup_status; fi; \
		exit "$$status"; \
	}; \
	trap cleanup EXIT INT TERM; \
	$(COMPOSE) --project-name "$$project" --env-file $(ENV_FILE) run --rm --no-deps -T prometheus-credentials; \
	$(COMPOSE) --project-name "$$project" --env-file $(ENV_FILE) up --detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT) $(COMPOSE_SERVICES); \
	$(COMPOSE) --project-name "$$project" --env-file $(ENV_FILE) run --rm --no-deps -T minio-init; \
	$(UV) run --env-file $(ENV_FILE) alembic downgrade base; \
	$(UV) run --env-file $(ENV_FILE) alembic upgrade head; \
	$(UV) run --env-file $(ENV_FILE) alembic check; \
	$(UV) run python scripts/verify_local_stack.py --env-file $(ENV_FILE) --timeout-seconds $(COMPOSE_WAIT_TIMEOUT)

e2e-happy: podman-preflight podman-images
	@set -eu; \
	cleanup() { \
		status=$$?; cleanup_status=0; trap - EXIT INT TERM; \
		for container in agent-platform_litellm_1 agent-platform_fake-llm-primary_1 agent-platform_fake-llm-secondary_1; do \
			if $(PODMAN) container exists "$$container"; then $(PODMAN) rm --force "$$container" || cleanup_status=$$?; fi; \
		done; \
		for network in agent-platform_llm-egress agent-platform_llm-upstreams; do \
			if $(PODMAN) network exists "$$network"; then $(PODMAN) network rm "$$network" || cleanup_status=$$?; fi; \
		done; \
		if [ "$$status" -ne 0 ]; then exit "$$status"; fi; \
		exit "$$cleanup_status"; \
	}; \
	trap cleanup EXIT INT TERM; \
	$(COMPOSE) --env-file $(ENV_FILE) up --detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT) fake-llm-primary fake-llm-secondary litellm; \
	AGENT_PLATFORM_RUN_CODING_E2E=1 AGENT_PLATFORM_SANDBOX_IMAGE=$(SANDBOX_IMAGE) $(UV) run --env-file $(ENV_FILE) pytest -v tests/end_to_end/test_coding_agent_happy_path.py

distributed-e2e: podman-preflight podman-images
	@set -eu; \
	project=agent-platform-distributed; \
	cleanup() { \
		status=$$?; trap - EXIT INT TERM; cleanup_status=0; \
		for container in "$${project}_minio_1" "$${project}_postgres_1"; do \
			if $(PODMAN) container exists "$$container"; then $(PODMAN) rm --force "$$container" || cleanup_status=$$?; fi; \
		done; \
		if $(PODMAN) network exists "$${project}_default"; then $(PODMAN) network rm "$${project}_default" || cleanup_status=$$?; fi; \
		for volume in "$${project}_minio-data" "$${project}_postgres-data"; do \
			if $(PODMAN) volume exists "$$volume"; then $(PODMAN) volume rm "$$volume" || cleanup_status=$$?; fi; \
		done; \
		if [ "$$status" -eq 0 ]; then status=$$cleanup_status; fi; \
		exit "$$status"; \
	}; \
	trap cleanup EXIT INT TERM; \
	$(COMPOSE) --project-name "$$project" --env-file $(ENV_FILE) up --detach --wait --wait-timeout $(COMPOSE_WAIT_TIMEOUT) postgres minio; \
	$(COMPOSE) --project-name "$$project" --env-file $(ENV_FILE) run --rm --no-deps -T minio-init; \
	$(UV) run --env-file $(ENV_FILE) alembic downgrade base; \
	$(UV) run --env-file $(ENV_FILE) alembic upgrade head; \
	socket="$${AGENT_PLATFORM_PODMAN_SOCKET:-}"; \
	if [ -z "$$socket" ]; then \
		machine=""; \
		for candidate in $$($(PODMAN) system connection list --format '{{if .Default}}{{.Name}}{{end}}'); do machine="$$candidate"; break; done; \
		if [ -n "$$machine" ]; then socket="$$($(PODMAN) machine inspect "$$machine" --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null || true)"; fi; \
	fi; \
	if [ -z "$$socket" ]; then socket="$$($(PODMAN) info --format '{{.Host.RemoteSocket.Path}}')"; fi; \
	socket="$${socket#unix://}"; \
	test -S "$$socket" || { echo "A real rootless Podman service socket is required." >&2; exit 1; }; \
	AGENT_PLATFORM_RUN_DISTRIBUTED_E2E=1 \
	AGENT_PLATFORM_PODMAN_SOCKET="$$socket" \
	AGENT_PLATFORM_SANDBOX_IMAGE=$(SANDBOX_IMAGE) \
	$(UV) run --env-file $(ENV_FILE) pytest -v tests/end_to_end/test_distributed_production_happy_path.py

local-acceptance: release-check kubernetes-contract compose-config acceptance-sim podman-production-images sandbox-security gateway-security postgres-security redis-security local-stack-acceptance e2e-happy distributed-e2e

migrate:
	$(UV) run --env-file $(ENV_FILE) alembic upgrade head

migration-check:
	$(UV) run --env-file $(ENV_FILE) alembic check

lifecycle-admin:
	@test -n "$(strip $(LIFECYCLE_ARGS))" || { echo "Set LIFECYCLE_ARGS to one reviewed lifecycle command." >&2; exit 2; }
	$(UV) run --env-file $(ENV_FILE) python -m scripts.data_lifecycle $(LIFECYCLE_ARGS)

recovery-verify:
	@test -n "$(strip $(RECOVERY_ARGS))" || { echo "Set RECOVERY_ARGS to one isolated-restore verification command." >&2; exit 2; }
	$(UV) run --env-file $(ENV_FILE) python -m scripts.recovery_verification $(RECOVERY_ARGS)

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
