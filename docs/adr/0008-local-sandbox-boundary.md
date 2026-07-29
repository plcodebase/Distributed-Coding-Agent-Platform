# ADR 0008: Provider-neutral sandbox boundary and local adapter

- Status: Accepted
- Date: 2026-07-28

## Context

Sequence 8 establishes the command-execution interface before the hardened Podman
sandbox is implemented. Development tests need a real streaming process adapter, but a
host subprocess is not a security boundary and must never be mistaken for the
production sandbox.

Commands must not invoke a shell implicitly, inherit the worker environment, retain
unbounded output, survive cancellation, or bypass the checkpoint policy.

## Decision

- Define provider-neutral `CommandSpec`, ordered stdout/stderr events, terminal command
  outcomes, snapshots, and a `Sandbox` protocol in `agent-core`.
- Implement `LocalSandbox` only in `sandbox-runtime`. It is disabled by default,
  requires `unsafe_allow_host_execution=True`, and rejects enabled construction when
  the declared runtime environment is production.
- Accept argv sequences only. Do not accept a shell string or enable shell evaluation.
  Resolve the working directory through the contained workspace.
- Construct a minimal environment with a temporary HOME, deterministic locale, and
  explicit executable path. Do not inherit the worker environment. Optional trusted
  values may not override sandbox-owned variables.
- Read stdout and stderr concurrently, preserve observed chunk ordering, account for
  raw bytes before UTF-8 decoding, stop at the configured ceiling, and terminate the
  process group on output overflow, timeout, stream cancellation, rewind, or destroy.
- Expose `run_command` only when the composition root explicitly asks the toolset to
  include it and supplies a sandbox. Mark it as a command effect so the core requires a
  checkpoint before execution.
- Convert timeout, output limit, non-zero exit, protocol violation, and startup failure
  into structured opaque errors.
- Use Podman exclusively for the future hardened container sandbox. Sequence 8 makes
  no production isolation claim and invokes no container runtime.

## Consequences

- Unit and integration tests can exercise real command streaming and cleanup without
  weakening the core's tool interface.
- `LocalSandbox` remains unsafe by name and behavior: a command can access host
  resources available to the current user. It is prohibited in production.
- Network, filesystem, CPU, memory, PID, privilege, and secret isolation are not
  Sequence 8 properties. They remain acceptance criteria for the hardened Podman
  adapter and its security-test sequence.
