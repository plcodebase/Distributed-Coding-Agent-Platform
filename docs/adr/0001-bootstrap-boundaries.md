# ADR 0001: Bootstrap boundaries

- Status: Accepted
- Date: 2026-07-27

## Decision

The first implementation sequence creates two importable packages:

- `agent-core` owns shared validated settings.
- `platform-telemetry` owns structured logging and secret redaction.

Domain models, agent behavior, and provider adapters remain for later sequences.
Provider SDKs and infrastructure clients do not enter the bootstrap packages.

Local Compose starts shared dependencies only. Host worker processes will use rootless
Podman without passing its socket into an application container. Kubernetes execution
will use a separate sandbox adapter.

## Consequences

- Core unit tests require no network, provider key, container runtime, or database.
- Foundation packages cannot import FastAPI, Podman, Redis, PostgreSQL, LiteLLM, or
  Kubernetes packages.
- Package additions in later sequences must keep the dependency direction inward.
