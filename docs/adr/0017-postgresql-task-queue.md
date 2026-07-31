# ADR 0017: PostgreSQL run queue

- Status: Accepted
- Date: 2026-07-30
- Sequence: 17

## Context

Run creation already commits a tenant-scoped `QUEUED` row. Execution must move out of
the API process, and concurrent workers must not receive the same run or block each
other while inspecting busy rows. The platform promises at-least-once execution, not
exactly-once delivery.

## Decision

- Use `runs` as the durable queue rather than introducing a second acknowledgement
  record. A committed `QUEUED` run is accepted work.
- Claim in one PostgreSQL transaction:
  - lock the worker registration and require active advertised capacity;
  - select the highest-priority, oldest eligible run with `FOR UPDATE SKIP LOCKED`;
  - exclude cancelled runs and workspaces with an active writer;
  - transition `QUEUED -> LEASED` through the core state policy;
  - increment the durable run lease generation;
  - insert the unique run-lease row;
  - reserve the workspace writer row and decrement worker capacity.
- Order baseline claims by `priority DESC`, `created_at`, and run ID. Queue classes,
  tenant fairness, admission control, and overload policy remain Sequences 21–22.
- Bound one claim operation to 32 workspace-lock-contention skips. Persistently owned
  workspaces are filtered in SQL, so this bound applies only to transient concurrent
  locks and does not permanently hide later eligible work.
- Fence every subsequent operation with the run ID, tenant ID, worker ID, random lease
  token, and monotonic generation. Database uniqueness permits only one active lease
  row for a run.
- Keep the queue behind the provider-neutral `RunQueue` protocol. The API never imports
  the worker, scheduler, or PostgreSQL queue adapter.

## Consequences

- Independent workers can claim different rows without a queue-wide lock.
- A transaction rollback returns the run, writer reservation, and worker slot together;
  no partial claim is acknowledged.
- PostgreSQL is the queue system of record. Redis is not required for correctness.
- A queued run can wait behind another run for the same workspace by design because
  Sequence 20 initially serializes all workspace access.

## Verification

- Deterministic adapter tests cover worker registration, capacity, claim, start,
  contention, cancellation, invalid clocks/durations, and fencing failures.
- The real PostgreSQL suite starts three registered worker identities and concurrently
  claims three distinct runs, proving `SKIP LOCKED` behavior and non-overlap.
- The worker-loss acceptance test proves that an expired claim becomes eligible for a
  different worker without losing the durable task.

## Operational impact

Run creation needs no separate enqueue call. Operators run worker processes against the
same PostgreSQL database and monitor queued run age and worker capacity; queue-age
metrics are added in the observability phase.

## Security impact

Every run lookup and lease relationship is tenant-scoped. Claim construction accepts
no SQL fragments or executable input. Random UUID lease tokens and generations prevent
a stale process from finishing a successor's run.

## Migration

Alembic revision `0002` adds the queue claim index and lease generation. Upgrade with
`make migrate`; downgrade removes only Sequence 17–20 distributed-execution state.
