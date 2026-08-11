# ADR 0025: Authoritative platform telemetry

- Status: Accepted
- Date: 2026-08-10
- Sequence: 25

## Context

Model-provider traces do not describe queueing, leases, context construction, sandbox
startup, checkpointing, or durable retries. Platform telemetry must cross the
API-to-worker boundary without recording prompts, source files, tool arguments, tool
results, credentials, or unbounded identifiers as metric labels.

## Decision

- Own one explicit `PlatformTelemetry` instance at each application composition root.
  It owns an OpenTelemetry SDK provider, optional OTLP/HTTP exporter, W3C propagation,
  and an isolated Prometheus registry with explicit shutdown and flush behavior.
- Persist the W3C `traceparent` and optional `tracestate` accepted when a run is first
  created. Workers extract that carrier before opening the run span. The carrier is
  format- and size-validated in both the domain and PostgreSQL schema.
- Put tenant, session, run, turn, model-call, and tool-call identifiers on spans and
  structured logs. Never use those raw identifiers as Prometheus labels.
- Use bounded route and tool label registries. Cost attribution uses an opaque truncated
  tenant hash and a fixed overflow bucket after the configured tenant-cardinality cap.
- Instrument API requests, queue wait, worker occupancy, context construction, logical
  model requests, provider attempts, first output, token counts, retries, tool
  execution, sandbox startup, and checkpoint creation. Queue pressure is sampled
  through a read-only domain protocol.
- Use stable structured error categories. Span helpers disable automatic exception
  recording; callers record only category and retryability so exception messages and
  source content cannot enter exported telemetry.
- Keep OpenAI Agents SDK tracing disabled at the adapter. The platform model-request
  span surrounding the SDK model layer is authoritative and participates in the same
  distributed trace without requiring a separate provider tracing credential.
- Expose Prometheus text through `/metrics`; composition may require a dedicated bearer
  token. Metrics endpoints never reuse user API credentials as labels or output.

## Consequences

Runs can be followed from API admission into worker, gateway, tool, checkpoint, and
sandbox phases while provider and infrastructure implementations remain dependency
injected. Cardinality and content policy are enforceable in code. Provider-native SDK
details not represented by the platform model layer are intentionally absent; adding
them later requires a content-safe bridge rather than enabling remote SDK tracing by
default.

## Migration

Revision `0007` adds nullable `traceparent` and `tracestate` columns and matching
constraints. Existing queued runs remain valid and begin a new root trace when no
carrier is present.
