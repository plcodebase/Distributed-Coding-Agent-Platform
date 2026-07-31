# ADR 0016: Durable event sequencing and WebSocket replay

- Status: Accepted
- Date: 2026-07-29
- Sequence: 16

## Context

Clients can disconnect at any time and workers may append events concurrently. An
in-process pub/sub stream cannot prove ordering, replay, or recovery.

## Decision

- Put `EventDraft`, `StoredEvent`, and `EventPage` contracts in `agent-core`.
  `EventDraft` validates its discriminated payload against the existing fourteen
  event types before persistence.
- Keep `runs.next_event_sequence` as the allocation cursor. Each append atomically
  increments it and inserts the event in the same PostgreSQL transaction.
- Retain the unique `(run_id, sequence)` database constraint as the final duplicate
  defense.
- Read after an exclusive cursor in ordered pages of at most 1,000 events and at most
  4 MiB of compact UTF-8 JSON. Stream rows from SQLAlchemy with a one-row yield batch,
  retain only entries that fit the byte budget, and stop at the first extra count- or
  byte-limited row. Page contracts independently recompute the exact serialized size.
- Fetch at most one extra count-limited row to report `has_more` exactly. Page contracts
  reject mixed-run entries, non-contiguous sequences, and impossible empty pages that
  claim more data. The count and byte ceilings are present in the core model and
  PostgreSQL adapter rather than only at the HTTP boundary.
- Require the first returned sequence to equal the exclusive cursor plus one and every
  subsequent sequence to be contiguous. A missing/corrupt durable sequence fails
  closed instead of silently presenting a false total order.
- Implement WebSocket reconnect as durable replay followed by bounded polling for new
  committed events. Polling is deliberately simple and cross-process correct; a later
  notification optimization may reduce latency without becoming the source of truth.
- Authenticate and tenant-scope the run before accepting the socket.
- Await every WebSocket send for backpressure. Disconnecting cancels only the socket
  handler and never invokes run cancellation.
- Finish any bounded event-page query under an AnyIO-shielded cancellation boundary so
  disconnect cancellation cannot abandon a checked-out database connection.
- Track active event sockets and drain their sender, receiver, and event-stream cleanup
  before application shutdown disposes the PostgreSQL engine.

## Consequences

- Reconnect after the last received sequence loses no committed event.
- Concurrent writers receive a total per-run order.
- A reconnect page cannot approach the former worst case of 1,000 one-megabyte events;
  serialized retained data is capped at 4 MiB.
- Live delivery latency includes the configured polling interval (250 ms by default).
- PostgreSQL remains authoritative even if future Redis or `LISTEN/NOTIFY` wakeups are
  lost.
- Deterministic adapter tests verify cursor advancement, multi-page replay, empty-page
  polling, live continuation, tenant-scoped latest-sequence lookup, byte-limited
  retention, gap detection, and the append compare-and-set failure path.
