# ADR 0019: Failure recovery and idempotent replay

- Status: Accepted
- Date: 2026-07-30
- Sequence: 19

## Context

At-least-once delivery creates crash windows around workspace mutation, tool-result
persistence, and event publication. Recovery must not lose accepted tasks or repeat a
completed logical mutation merely because its event was not delivered.

## Decision

- Have the scheduler lock expired lease rows with `SKIP LOCKED`, then transition each
  matching run `LEASED/RUNNING -> LOST -> QUEUED`. Requeue increments `attempt`, clears
  ownership, releases the writer, returns worker capacity, and deletes the old lease.
  A cancellation observed during recovery commits `CANCELLED` instead.
- Fence recovery-state reads to the active replacement lease.
- Load at most 4,096 ordered messages and 100 terminal tool outcomes. Invalid durable
  messages, reserved metadata overrides, missing selected checkpoints, and exceeded
  bounds fail closed. When a checkpoint is selected, exclude tool outcomes completed
  before it so an older workspace revision cannot override the newer snapshot.
- Restore the selected checkpoint's conversation, task plan, and summary. Restore its
  pre-tool workspace snapshot at the latest durable post-tool workspace revision when
  one or more terminal mutations completed after that checkpoint.
- Pass both the checkpoint and selected revision to an injected `WorkspaceRestorer`.
  Snapshot materialization remains deployment-specific because object-store access and
  worktree construction belong in the application composition root.
- Persist each tool state before its corresponding event. Tool persistence is
  monotonic:
  - replay of `RECEIVED`, `WAITING_APPROVAL`, or `RUNNING` returns a later durable
    state;
  - replay of the same terminal status/result returns the durable record even when
    delivery timestamps differ;
  - the same call ID with different name, arguments, hash, or turn fails closed;
  - a different terminal status or result fails closed.
- Persist approval waits as `WAITING_APPROVAL`.
- Inject terminal durable outcomes into `AgentLoop`. The loop reuses a matching stable
  tool-call ID and argument hash instead of invoking its handler again.
- Append worker events with a stable attempt/generation/local-sequence delivery key.
  The event store returns an identical prior event and rejects key/data conflicts.

The relevant crash windows therefore resolve as follows:

| Durable state at crash | Recovery behavior |
|---|---|
| Workspace mutation not finalized | Restore pre-tool checkpoint; retry is allowed |
| Workspace revision finalized, no terminal tool row | Restore pre-tool checkpoint; the unacknowledged mutation is rolled back |
| Terminal tool row committed, event missing | Restore the recorded post-tool revision and reuse the result |
| Terminal tool row and event committed | Replay both identities without another mutation |

## Consequences

- Recovery is at-least-once at the run level and idempotent at stable tool/event
  identities; it does not claim exactly-once provider billing or task delivery.
- Replacement workers need access to the checkpoint snapshot URI and recorded revision.
- A production worker factory must compose a durable checkpoint coordinator and
  workspace restorer. The protocol boundary intentionally does not invent an object
  storage implementation or silently fall back to process memory.
- Replay safety still depends on stable logical tool-call IDs, validated arguments,
  workspace revision checks, and concrete tool idempotency working together.

## Verification

- Unit tests cover predecessor replay, terminal timestamp replay, conflicting outcomes,
  approval states, missing checkpoints, recovery bounds, post-tool revision selection,
  executor reuse, scheduler batches, and fatal worker bookkeeping failures.
- The real PostgreSQL acceptance suite abandons Worker A's live lease, expires it,
  reclaims it with Worker B, invokes the actual `WorkerService` restore boundary with
  the post-tool revision, completes at attempt two, and proves only one mutating
  tool-call row exists.

## Operational impact

Run the scheduler as a separate process:

```text
python -m agent_scheduler --factory your_app.scheduler:create_scheduler
```

Recovery polling and batch size are bounded. An unavailable database fails the
scheduler rather than acknowledging ambiguous recovery.

## Security impact

All recovery queries are tenant/run scoped and require the current lease fence.
Persisted tool arguments and error details remain validated immutable JSON. Rejected
raw provider arguments and unexpected exception text are never reconstructed during
replay.

## Migration

Revision `0002` adds tool error/outcome constraints and event delivery-key uniqueness.
Existing Sequence 14 rows require no data rewrite because no distributed execution data
precedes this revision.
