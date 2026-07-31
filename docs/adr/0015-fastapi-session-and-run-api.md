# ADR 0015: FastAPI session and run API

- Status: Accepted
- Date: 2026-07-29
- Sequence: 15

## Context

The agent requires a durable control-plane API without importing FastAPI request
objects into the domain or exposing unscoped persistence records.

## Decision

- Add an `agent-api` application package composed entirely through injected
  authentication, repository, event-store, and readiness protocols.
- Expose the design endpoints for sessions, run creation/query/cancellation,
  approvals, rewind selection, event listing, event streaming, and live/ready health.
- Require bearer authentication for every tenant-owned HTTP and WebSocket route.
  `Principal` contains a tenant UUID and audit subject. The authentication provider is
  replaceable; a bounded constant-time static-token implementation exists only for
  local composition and tests.
- Require a header-safe `Idempotency-Key` for run creation. Database uniqueness returns
  the original run for a matching payload and rejects key reuse with a different
  payload.
- Create new runs only while their session is `active`; completed and cancelled
  sessions fail with a closed state-conflict response.
- Put a 64 KiB pre-routing ASGI request-body ceiling in front of FastAPI parsing.
  Validate a single decimal `Content-Length` when present, count the actual streamed
  bytes even when it is absent, reject declared/actual length mismatches, and never
  pass rejected bytes to a route or validation handler.
- Scope every repository call with the authenticated tenant. Cross-tenant resources
  are returned as not found rather than revealing their existence.
- Store approval decisions durably before resuming a suspended run through `QUEUED`.
- Treat rewind as selection of a durable checkpoint; actual workspace restoration is
  performed by the execution plane.
- Convert domain errors to closed JSON responses and unexpected exceptions to an
  opaque retryable `internal_error`. Request-validation responses never echo rejected
  body or header values, and static bearer credentials accept only bounded RFC
  header-safe token characters. Health routes disclose no credentials.
- Reject duplicate JSON object keys at every level of the static credential
  configuration so an operator and parser cannot disagree about the effective tenant.
- Keep WebSocket authentication, tenant lookup, and repository failures distinct:
  authentication closes with 4401, tenant-hidden absence with 4404, and unexpected
  control-plane failure with 1011.
- A client disconnect changes no run state.

## Consequences

- Sequence 15 creates queued durable runs but does not execute them. Sequence 17
  workers claim those rows through the PostgreSQL queue.
- Local development uses `make api` and the bearer map in
  `AGENT_PLATFORM_API_CREDENTIALS_JSON`. Production must replace local static tokens
  with an external authentication implementation.
- API handlers depend on core protocols and values; only the production factory imports
  PostgreSQL adapters.
- The 64 KiB ceiling is intentionally sized for the current control endpoints. A
  future upload API must use a separate streaming object-storage boundary instead of
  raising this global control-plane limit.
