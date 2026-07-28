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
- Use immutable Pydantic models with forbidden extra fields, finite JSON-safe payloads,
  and timezone-aware timestamps normalized to UTC.
- Require callers to supply durable identifiers and timestamps. Core code does not use
  global ID or clock state.
- Number durable agent events from one; message and checkpoint counters may begin at
  zero.
- Revalidate every `model_copy` operation. Reject status-changing `Run.model_copy`
  calls with `run_status_update_requires_transition`; every run state change goes
  through `transition_run`. `model_construct` is reserved for trusted persistence
  adapters and is prohibited for untrusted or unvalidated persisted values.
- Treat `WAITING_APPROVAL` and `RETRY_PENDING` as suspended states. Entering either
  state releases the worker and lease. Both resume through `QUEUED → LEASED → RUNNING`,
  so capacity is reacquired rather than retained while work is paused.
- Treat `LEASED → QUEUED` as lease release. `WAITING_APPROVAL → QUEUED` preserves the
  attempt after a durable approval decision. `RETRY_PENDING → QUEUED` and
  `LOST → QUEUED` increment the attempt.
- Use session statuses `active`, `completed`, and `cancelled`.
- Use approval modes `require_all`, `require_sensitive`, and `auto_approve`.
  `auto_approve` skips only human confirmation; tool-argument validation, command
  safety, workspace isolation, and every other safety control remain mandatory.
- Hash canonical UTF-8 JSON tool arguments with SHA-256, and recursively freeze JSON
  values after validation so the arguments cannot diverge from their recorded hash.
- Distinguish mutable `JsonObject` dictionaries used at wire boundaries from
  `FrozenJsonObject`, a truthful immutable `Mapping` used by domain fields. Domain
  serialization and `to_json_object()` always produce defensive ordinary
  dictionaries and lists.
- Use one validated `ErrorDetail` contract for domain exceptions and event failures.
- Represent the design's event names as a discriminated union of concrete envelopes.
  Payloads contain only fields needed to correlate durable operations and render agent
  progress, and their serialized UTF-8 representation may not exceed 1 MiB.
- Defer total transition ordering and `(run_id, sequence)` uniqueness to the durable
  event store. Domain event keys are stable, but Sequence 2 has no cross-event storage
  boundary at which to enforce uniqueness.

## Consequences

- Malformed persisted state, non-finite or oversized event payloads, timestamps, and
  tool argument hashes fail at the core boundary.
- Invalid run transitions produce stable codes and JSON-safe details for future API and
  worker adapters.
- Approval and retry waits do not consume a worker lease. Resumption requires a normal
  queue and lease acquisition cycle.
- The `(run_id, sequence)` event key and stable tool/model call IDs survive later
  serialization and recovery.
- Sequence 2 does not implement an event store, agent loop, model gateway, or tools.
  Those remain assigned to later implementation sequences.
