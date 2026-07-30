# ADR 0012: Typed, attributed, normalized gateway client

- Status: Accepted
- Date: 2026-07-29

## Context

The Agents SDK adapter normalizes provider protocol events, but a worker-facing client
must also enforce route policy, stream shape, resource bounds, lifecycle ownership, and
complete call attribution. These guarantees must not depend on optional telemetry
fields supplied by callers.

## Decision

- Add a separate `gateway-client` workspace package. It composes the existing Agents SDK
  adapter but does not move provider imports into `agent-core`.
- Extend `GatewayRequest` with required tenant ID, session ID, run ID, turn number,
  model-call ID, and stable request ID. Extend `AgentLoopInput` with tenant/session IDs;
  the loop supplies the current turn automatically.
- Attach this attribution to every SDK model call as metadata and explicit upstream
  headers. Configured metadata or headers cannot override platform attribution.
- Keep `request_id` stable for the one logical call and do not generate it inside an
  adapter. Durable duplicate suppression is not claimed in this sequence.
- Allowlist the five logical routes by default. Reject unknown routes before contacting
  LiteLLM.
- Revalidate every delegated event against the provider-neutral discriminated union.
  Require exactly one terminal event, reject post-terminal data, and reject streams
  ending without a terminal event.
- Bound normalized stream event count and cumulative canonical UTF-8 bytes. Close the
  delegated stream on cancellation, limit failure, validation failure, or consumer
  abandonment.
- Preserve normalized text, completed/invalid tool calls, finish reasons, usage, model,
  and opaque provider failures from the Agents SDK adapter. Never fabricate provider
  identity.
- Own adapter cleanup through an async context manager and idempotent,
  cancellation-safe `aclose()`. Retain partial provider/client cleanup state, reject
  model traffic while cleanup is incomplete, and allow cleanup to be retried.

## Consequences

- Every worker model call is attributable to tenant, session, run, turn, model call, and
  request ID before it crosses the network.
- Unit tests cover invalid event shapes, terminal invariants, UTF-8 byte/event limits,
  route denial, error opacity, cancellation, and cleanup. A live Podman LiteLLM test
  covers actual normalized streaming.
- Stored-result idempotency, attach-or-conflict behavior, exponential backoff, client
  fallback, circuit breakers, and tenant/model rate limits remain Sequence 13.
