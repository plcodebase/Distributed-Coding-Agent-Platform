# ADR 0018: Worker leases, heartbeats, cancellation, and draining

- Status: Accepted
- Date: 2026-07-30
- Sequence: 18

## Context

Workers are independent failure domains. A process may disappear after claiming a run,
and a control-plane cancellation or shutdown may arrive while model, tool, or sandbox
work is active. Process presence alone cannot establish ownership.

## Decision

- Persist worker registrations with worker ID, supported sandbox types, total and
  available slots, lifecycle status, registration time, and last heartbeat.
- Persist one expiring `run_leases` row per run. Renewal locks and validates the exact
  lease token/generation and updates the run's mirrored expiry in the same transaction.
- Require heartbeat cadence to be less than half the configured lease duration.
  Durations, slot counts, clocks, and identifiers are validated and bounded.
- Run model/tool execution only in `agent-worker`; the FastAPI application only creates
  and controls durable runs.
- Have each `WorkerService`:
  - register before polling;
  - stop claiming at its local slot limit;
  - start and heartbeat owned runs and their writer leases;
  - observe the durable cancellation flag on each heartbeat;
  - cancel active executor work and commit `CANCELLED`;
  - complete a cancellation already visible at start without creating an execution
    context;
  - surface unrecoverable background-task failures instead of silently discarding task
    exceptions.
- Mark a draining worker durably before waiting for owned tasks. Draining workers keep
  heartbeating active work but the queue rejects new claims. Resume is explicit.
- Provide a trusted `module:attribute` composition factory and a local fleet launcher
  whose validated default is three independent spawned processes. Each factory must
  derive a unique worker ID from its process index. The parent observes every child
  rather than blocking on one process indefinitely; a nonzero exit stops the remaining
  fleet so the external supervisor can restart a known state.
- Keep the worker and scheduler packages dependent only on `agent-core`. Application
  composition injects PostgreSQL, gateway-client, checkpoint, and sandbox adapters.

## Consequences

- Loss of process memory does not transfer ownership; only a new database lease can.
- Cancellation latency is bounded by the heartbeat interval, except cancellation
  already present at start, which is immediate.
- Graceful draining does not abandon already accepted work.
- A worker heartbeat records liveness and capacity, while run-lease expiry remains the
  correctness signal for recovery. Automatic `OFFLINE` labelling is an observability
  enhancement, not an ownership mechanism.

## Verification

- Unit tests cover registration, slot accounting, heartbeats, cancellation before and
  during execution, draining/resume, lease loss, background failure propagation, and
  unsafe configuration.
- The default local process count is contract-tested as three.
- Real PostgreSQL tests exercise concurrent worker registrations and claims, stale
  owner rejection, slot return, and a draining worker that cannot claim.

## Operational impact

Start a fleet with:

```text
python -m agent_worker --factory your_app.workers:create_worker
```

The factory reference is trusted deployment configuration, not user input. Signals
cause each child service to stop polling and drain its active run.

## Security impact

Worker code imports neither provider SDKs nor container control libraries. Model access
must use the typed gateway client. Tokens are opaque UUIDs, stale timestamps fail
closed, and unexpected execution errors are represented by bounded opaque details.

## Migration

Revision `0002` creates `workers` and `run_leases`, their checks, uniqueness constraints,
foreign keys, and expiry/worker indexes.
