# Distributed Coding Agent Platform

This repository implements the system specified in [`DESIGN.md`](DESIGN.md). Work is
delivered phase by phase so each layer has a runnable verification gate before later
distributed-system and sandbox features are added.

## Sequence 1: repository bootstrap and CI

The current sequence provides:

- a Python 3.12 `uv` workspace with locked dependencies;
- lint, type-check, unit-test, coverage, pre-commit, and CI configuration;
- local PostgreSQL, Redis, S3-compatible storage, LiteLLM, Prometheus, Grafana, and
  deterministic fake-provider services orchestrated by Podman;
- Pydantic Settings, structured JSON logging, and recursive secret redaction.

## Local setup

```shell
cp .env.example .env
make bootstrap
make check
make compose-config
make compose-up
make compose-smoke
```

`make compose-up` starts shared infrastructure only and pins `podman-compose` as
Podman's Compose provider. `make compose-smoke` verifies every exposed dependency and
routes requests through LiteLLM to both deterministic fake providers. Agent
applications and workers are added in later implementation sequences. The fake
LiteLLM routes are the default local configuration and do not need provider
credentials.

Do not put real credentials in `.env.example`, source control, worker environments, or
sandbox environments.
