# ADR 0034: Branch-safe durable rewind

## Status

Accepted

## Context

A selected workspace checkpoint could be restored while messages, tool outcomes, approvals, model
requests, memories, and artifacts created after that checkpoint remained addressable under the same
run-local identities. Re-executing a logical call after rewind could therefore reuse an abandoned
future or collide with its idempotency record.

## Decision

- Give every run a positive, monotonic `execution_epoch`. Rewind increments both the attempt and
  epoch in the same transaction that requeues the run.
- Bind messages, task plans, tool calls, approvals, checkpoints, model calls, memories, extraction
  jobs, events, and run artifacts to their execution epoch. Worker-owned writes require the epoch
  carried by the active run lease.
- Preserve old branch rows for audit. Rewind copies only the selected checkpoint's bounded message
  prefix and task plan into the new epoch. Active conversation, plan, memory, artifact, approval,
  tool, and recovery reads join to the run's current epoch.
- Keep event sequence numbers global to the run, but scope idempotent event delivery keys to the
  epoch so audit replay remains one contiguous timeline.
- Namespace post-rewind model-call and gateway request IDs with the epoch. The initial epoch keeps
  its existing identifiers for compatibility.
- Give sessions a monotonic `context_generation`. Rewind increments it and supersedes pending
  compaction and memory-extraction work so summaries derived from an abandoned future cannot enter
  active context.
- Keep checkpoint object keys immutable by checkpoint UUID. Give final patches an epoch-specific
  object key after the initial branch.
- Reject stale-epoch sandbox authorization and all stale-epoch fenced writes.
- Retain a downgrade path only before a rewind has created branch data. Downgrade fails explicitly
  after that point because flattening branches would discard audit history or violate old unique
  constraints.

## Consequences

Legitimate tool and model calls may execute again after rewind without colliding with abandoned
records, while historical branches remain queryable for audit. Recovery can resume work appended to
the new branch even while the selected workspace checkpoint belongs to an older epoch. The schema
adds one migration and changes internal uniqueness contracts; no external event names or tool names
change.
