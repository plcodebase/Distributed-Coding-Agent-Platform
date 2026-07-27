# ADR 0002: Use Podman for local containers

- Status: Accepted
- Date: 2026-07-27

## Context

The design document originally names Docker for local container execution. The project
owner explicitly requires Podman and prohibits use of Docker tooling.

## Decision

- Use rootless Podman for local images, networks, volumes, and containers.
- Use `podman compose` with the locked `podman-compose` provider.
- Name build recipes `Containerfile`.
- Keep the provider-neutral `Sandbox` boundary so Kubernetes and future runtimes do not
  enter `agent-core`.
- Do not mount the Podman service socket into application or sandbox containers.

## Consequences

The security intent and hardened-container requirements remain unchanged. Runtime tests
will target Podman behavior, and documentation or automation must not require Docker
commands.
