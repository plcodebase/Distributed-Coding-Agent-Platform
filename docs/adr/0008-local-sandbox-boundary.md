# ADR 0008: Provider-neutral sandbox boundary and local adapter

- Status: Accepted
- Date: 2026-07-28
- Hardened: 2026-07-29

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
  requires `unsafe_allow_host_execution=True`, accepts only the closed development,
  test, and production environment labels, and rejects enabled construction in
  production. Unknown labels fail closed.
- Accept argv sequences only. Do not accept a shell string or enable shell evaluation.
  Resolve the working directory through the contained workspace.
- Construct a minimal environment with a temporary HOME, deterministic locale, and
  explicit executable path. Do not inherit the worker environment. Optional trusted
  values, the executable path, entry counts, and UTF-8 byte sizes are validated and
  bounded. Loader, interpreter, Git-control, and sandbox-owned variables cannot be
  overridden.
- Read stdout and stderr concurrently, preserve observed chunk ordering, account for
  raw bytes before UTF-8 decoding, stop at the configured ceiling, and terminate the
  process group on output overflow, timeout, stream cancellation, rewind, or destroy.
- Serialize process start, cancellation, and close registration so a process cannot
  start between active-process discovery and teardown. A cancelled process start is
  allowed to finish registration and is then terminated before cancellation
  propagates. Output-consumer failures also terminate the process group.
- Track command tasks before they can start. Cancellation increments a generation so
  commands already waiting for a concurrency slot fail without execution. Direct
  workspace operations are serialized and reject overlap with active commands.
- Bound direct writes, concurrent commands, trusted environment data, and retained
  snapshots. Restore accepts only an exact snapshot created by that sandbox and
  invalidates snapshots from the abandoned future branch.
- Destruction is cancellation-safe, targeted, idempotent, and retryable. The adapter is
  marked destroyed only after command, runner, worktree, and private-HOME cleanup all
  succeed; partial failure blocks reuse with a structured cleanup-required error.
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
- Process lifecycle guarantees prevent accidental local child leaks, but they do not
  turn a host subprocess into an isolation boundary.
- Network, filesystem, CPU, memory, PID, privilege, and secret isolation are not
  Sequence 8 properties. They remain acceptance criteria for the hardened Podman
  adapter and its security-test sequence.
