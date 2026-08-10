# ADR 0022: Priority backpressure and bounded aging

- Status: Accepted
- Date: 2026-07-31
- Sequence: 22

## Context

One numeric priority cannot express workload intent, and unconstrained queue growth can
consume database and API capacity. Strict class ordering can permanently starve
background work.

## Decision

- Classify runs as interactive, background, or evaluation, with a bounded secondary
  priority from -100 through 100.
- Compute an effective SQL rank with bounded age promotion, then use priority, creation
  time, and run ID as deterministic tie breakers.
- Serialize global admission through one locked PostgreSQL policy row while also
  enforcing tenant queued-run quotas.
- Reconcile the platform-owned global limit and retry interval while holding that lock;
  historical run timestamps are accepted when no configuration mutation is required.
- Return structured HTTP 429 overload responses with a bounded `Retry-After` value.
- Expose queue depth by class and oldest wait through a provider-neutral snapshot.

## Consequences

Interactive work starts promptly while old lower-class work eventually advances.
Admission is deterministic but the singleton global-admission row intentionally
serializes run creation.

## Migration

Revision `0004` adds the run priority class and global queue-admission row.
