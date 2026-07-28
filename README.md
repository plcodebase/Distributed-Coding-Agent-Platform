# Distributed Coding Agent Platform

This repository implements the system specified in [`DESIGN.md`](DESIGN.md). Work is
delivered phase by phase so each layer has a runnable verification gate before later
distributed-system and sandbox features are added.

## Implemented sequences

### Sequence 1: repository bootstrap and CI

Sequence 1 provides:

- a Python 3.12 `uv` workspace with locked dependencies;
- lint, type-check, unit-test, coverage, pre-commit, and CI configuration;
- local PostgreSQL, Redis, S3-compatible storage, LiteLLM, Prometheus, Grafana, and
  deterministic fake-provider services orchestrated by Podman;
- Pydantic Settings, structured JSON logging, and recursive secret redaction.

### Sequence 2: core agent events and domain models

Sequence 2 provides:

- immutable, validated session, run, tool-call, checkpoint, and model-call models;
- a centrally enforced run state machine with structured transition errors;
- canonical tool-argument hashes for stable idempotency comparisons;
- typed payload contracts for every agent event named in the design;
- finite, immutable domain JSON with ordinary JSON wire serialization;
- closed-schema parsing, 1 MiB event-payload limits, and UTC-aware timestamps;
- explicit approval policies and lease-free approval/retry suspension states.

### Sequence 3: fake model and deterministic agent loop

Sequence 3 provides:

- a provider-neutral streaming model-gateway contract;
- a deterministic scripted gateway that requires no credentials or network access;
- a bounded async agent loop with typed event emission and injected clocks/IDs;
- Pydantic tool schemas that validate model-generated arguments before execution;
- model/tool timeouts plus turn, tool-call, semantic-retry, and UTF-8 byte limits;
- unit and integration tests for final text, tool calls, malformed arguments, failures,
  and termination limits.

## Local setup

```shell
cp .env.example .env
make bootstrap
make check
make compose-config
make compose-up
make compose-smoke
```

`make compose-up` starts shared infrastructure only and invokes the locked
`podman-compose` package with the native Podman CLI explicitly. `make compose-smoke`
verifies every exposed dependency and routes requests through LiteLLM to both
deterministic fake providers. Agent applications and workers are added in later
implementation sequences. The fake LiteLLM routes are the default local configuration
and do not need provider credentials.

Do not put real credentials in `.env.example`, source control, worker environments, or
sandbox environments.
