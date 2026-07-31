# ADR 0013: Gateway reliability and idempotency

- Status: Accepted
- Date: 2026-07-29
- Sequence: 13

## Context

The typed Sequence 12 client bounded one normalized stream but did not prevent repeated
billable calls for a completed logical request, retry transient pre-response failures,
or coordinate route health and tenant/model admission. Retrying a response after text
has already reached the agent would duplicate semantic input and can no longer be
treated as a transparent transport retry.

## Decision

- Hash canonical serialized `GatewayRequest` values and claim the pair
  `(tenant_id, request_id)` before gateway admission.
- Treat claim outcomes explicitly:
  - unseen payload: execute;
  - matching in-progress payload: return a retryable conflict;
  - matching completed payload: replay the stored normalized events;
  - matching failed payload: return its opaque stored terminal state;
  - different payload: fail closed.
- Persist the terminal event before yielding it to the caller. Text/tool events retain
  streaming behavior, but a response is not reported terminally successful until the
  complete result is durable.
- Retry only retryable failures that occur before the first normalized event. Use
  bounded exponential backoff and injected jitter. Never retry a partial stream.
- Continue to use LiteLLM's compatible provider fallback configuration. Client retries
  preserve the logical route and request identity rather than inventing provider
  identities or silently changing semantic routes.
- Apply tenant-and-route admission once per logical request. Completed replay bypasses
  rate and circuit checks because it creates no provider call.
- Bind completed stored responses to exactly one final terminal event. A half-open
  circuit probe is exclusive, but its ownership expires after the recovery interval so
  a worker crash cannot leave a durable route permanently blocked.
- Provide deterministic in-memory stores, limiters, and circuits for unit/local use.
  Production composition must inject the PostgreSQL request store and shared
  PostgreSQL rate/circuit policies.
- Keep provider exceptions opaque and close every failed/cancelled delegated stream.
- Treat the durable terminal commit as complete before yielding the terminal event.
  Closing a consumer immediately after that event must not rewrite the completed claim
  as failed.
- Run the terminal completion commit in a shielded task. If caller cancellation arrives
  during that commit, wait for its outcome before propagating cancellation. A successful
  commit records durable completion in the execution coordinator so outer abort
  bookkeeping cannot rewrite it to `failed`.
- Shield request failure/release bookkeeping from caller cancellation and await it
  before propagating cancellation. A bookkeeping failure blocks client reuse rather
  than allowing an ambiguous second provider request.
- Bound local/shared rate counts and circuit thresholds consistently, reject booleans
  disguised as integers, and reject non-finite injected clock values before they can
  corrupt admission state.

## Consequences

- Completed duplicate request IDs do not create another upstream request.
- A worker crash during an in-progress provider call remains conservatively
  `in_progress` until an operational recovery policy resolves it; the platform does not
  claim exactly-once billing.
- Transparent client retry is intentionally unavailable after any streamed content.
- PostgreSQL becomes part of production gateway admission. Its outage fails closed
  before a provider request rather than bypassing idempotency or limits.

## Verification

- Unit tests cover completed replay, running/conflicting claims, tenant scoping,
  pre-output retry delays, partial-stream suppression, half-open probes, rate windows,
  configuration bounds, close-after-terminal behavior, and cancellation during durable
  failure and completion recording.
- PostgreSQL integration tests cover durable replay and shared policy state across
  independently composed adapter instances, including recovery of an abandoned
  half-open probe.
