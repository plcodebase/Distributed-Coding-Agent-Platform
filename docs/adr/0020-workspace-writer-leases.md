# ADR 0020: Fenced workspace writer leases

- Status: Accepted
- Date: 2026-07-30
- Sequence: 20

## Context

Two runs may target the same repository workspace. Run ownership alone does not prevent
different run IDs from mutating that workspace concurrently, especially during worker
failure and reassignment.

## Decision

- Persist one `workspace_writer_leases` row keyed by `(tenant_id, workspace_id)`.
- Reserve the writer in the same transaction that claims the run. A run is not reported
  leased unless both ownership records and worker capacity commit together.
- Initially serialize every run targeting a workspace, including read-only runs. This
  is conservative and leaves read/write lease splitting to a later measured need.
- Fence the writer with:
  - tenant, workspace, run, and worker identities;
  - the owning run-lease token;
  - a distinct random writer token;
  - a monotonically increasing writer generation;
  - bounded acquisition and expiry timestamps.
- Cap writer expiry at the owning run lease. Renewal validates both lease records under
  row locks.
- Replaying acquisition for the exact same owner renews the writer up to the active run
  expiry instead of returning a stale local expiry.
- Release only the complete matching identity and token/generation. An exact repeated
  release is idempotent only while the retained unowned row has the same generation.
  Missing rows and stale/successor generations fail closed. Scheduler recovery and
  normal run completion target the writer associated with the old run-lease token.
- Pass the exact writer token/generation through the worker restore, execution, and
  loop-composition boundaries.
- Keep an unowned row after release so its generation remains monotonic.
- Enforce relational ownership with a composite foreign key from
  `(tenant_id, run_id, workspace_id)` to the matching run. An application bug cannot
  attach a writer for workspace A to a run for workspace B.

## Consequences

- At most one claimed run can own a tenant workspace at a time.
- A crashed owner's row remains unavailable until lease recovery releases it, avoiding
  unsafe time-only takeover by an uncoordinated worker.
- Writer generations survive release and provide a durable stale-owner fence.
- Throughput for multiple read-only runs on one workspace is intentionally reduced in
  favor of simple initial correctness.

## Verification

- Unit tests cover acquire/renewed same-owner replay, contention, heartbeat expiry
  capping, complete-identity stale heartbeat/release fencing, repeated release, token
  validation, and scheduler cleanup.
- Real PostgreSQL tests prove a second run for the same workspace is not claimable,
  replacement ownership receives a higher generation, and a mismatched run/workspace
  relation violates the composite foreign key.
- The worker-loss acceptance test confirms the active writer is cleared after
  replacement completion.

## Operational impact

No separate writer daemon or Redis lock is required. Operators recover expired run
leases through the scheduler; manual deletion of writer rows would discard fencing
history and is not a supported recovery procedure.

## Security impact

Writer ownership is tenant-scoped and database-enforced. Random tokens are never
derived from user input, and stale releases fail closed without exposing the current
owner's token.

## Migration

Revision `0002` creates the writer table, active-state checks, token uniqueness, expiry
index, worker foreign key, and the composite run/workspace foreign key. Downgrade drops
the writer table before removing its referenced run uniqueness constraint.
