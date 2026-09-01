# ADR 0035: Typed CLI and bounded local session

## Status

Accepted

## Context

The original Phase 1 contract required a terminal renderer that consumes agent events and a local
in-memory session. The durable HTTP API and worker topology did not satisfy those developer-facing
interfaces. A local client must not create a second orchestration policy, expose terminal control
sequences, mutate the source checkout directly, or reintroduce an unsafe container-runtime path.

## Decision

- `agent-core` provides `InMemoryAgentSession` and `InMemoryTranscriptJournal`. They retain a
  bounded normalized transcript, serialize runs, inject an `AgentLoop` factory, and keep the
  provider-neutral loop as the only turn and tool-policy owner.
- The local session always uses `auto_approve`; this bypasses only human confirmation. Argument
  validation, protected paths, tool limits, and sandbox controls remain mandatory.
- `agent-cli` validates the existing fourteen-event discriminated union before rendering. JSON
  Lines are byte bounded, strict UTF-8, and reject duplicate keys. Terminal rendering escapes
  control characters and omits tool arguments and result bodies.
- Local repository work occurs in a private detached Git worktree. Read access is the default;
  edit registration requires `--allow-edit`.
- Command registration requires both `--allow-edit` and `--allow-commands` and uses the hardened
  rootless `PodmanSandbox`. No Docker-compatible runtime or direct host-command mode is exposed by
  the CLI.
- Local changes never update the source checkout. The client reports a final patch identity and
  writes patch bytes only to a caller-selected, previously nonexistent path.
- The production API and PostgreSQL session remain authoritative for durable, resumable,
  multi-process use. The local session intentionally does not survive process exit.

## Consequences

The Phase 1 CLI and in-memory-session omissions are closed without coupling `agent-core` to the
Agents SDK, Podman, HTTP, or persistence. Local commands require a running rootless Podman engine
and a prebuilt sandbox image. Human approval suspension remains a durable API workflow; the local
CLI uses explicit capability flags and automatic confirmation for its single-process developer
mode.
