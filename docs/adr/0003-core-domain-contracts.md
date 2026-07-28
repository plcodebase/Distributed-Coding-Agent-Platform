# ADR 0003: Core domain contracts

- Status: Accepted
- Date: 2026-07-27

## Context

Sequence 2 introduces the domain values consumed by the upcoming deterministic agent
loop and, later, persistence adapters. The design specifies the entity fields and main
run-state diagram, but intentionally leaves some enum members, recovery transitions,
and event payload shapes implicit.

## Decision

- Keep domain models and events in `agent-core`; they import no provider or
  infrastructure adapters.
- Use immutable Pydantic models with forbidden extra fields, JSON-safe payloads, and
  timezone-aware timestamps normalized to UTC.
- Require callers to supply durable identifiers and timestamps. Core code does not use
  global ID or clock state.
- Number durable agent events from one; message and checkpoint counters may begin at
  zero.
- Validate every run state change through `transition_run`. Direct model mutation is
  rejected.
- Treat `LEASED → QUEUED` as lease release, `WAITING_APPROVAL → RUNNING` as approval
  resumption, `RETRY_PENDING → RUNNING` as retry resumption, and `LOST → QUEUED` as a
  new attempt.
- Use session statuses `active`, `completed`, and `cancelled`.
- Use approval modes `always`, `on_request`, and `never`. Policy interpretation belongs
  to the later approval component.
- Hash canonical UTF-8 JSON tool arguments with SHA-256, and recursively freeze JSON
  values after validation so the arguments cannot diverge from their recorded hash.
- Represent the design's event names as a discriminated union of concrete envelopes.
  Payloads contain only fields needed to correlate durable operations and render agent
  progress.

## Consequences

- Malformed persisted state, event payloads, timestamps, and tool argument hashes fail
  at the core boundary.
- Invalid run transitions produce stable codes and JSON-safe details for future API and
  worker adapters.
- The `(run_id, sequence)` event key and stable tool/model call IDs survive later
  serialization and recovery.
- Sequence 2 does not implement an event store, agent loop, model gateway, or tools.
  Those remain assigned to later implementation sequences.
