# ADR 0014: PostgreSQL persistence and migrations

- Status: Accepted
- Date: 2026-07-29
- Sequence: 14

## Context

Sessions, run state, execution results, approvals, checkpoints, model accounting, and
event cursors must survive API and worker loss. Process-local state cannot satisfy
recovery or tenant-isolation requirements.

## Decision

- Add a `platform-persistence` adapter package using SQLAlchemy 2.x async sessions,
  asyncpg, and Alembic.
- Create an explicit reversible initial migration for:
  - sessions;
  - runs;
  - messages;
  - task plans;
  - tool calls;
  - approvals;
  - checkpoints;
  - agent events;
  - model calls;
  - gateway requests;
  - gateway rate windows;
  - gateway circuits.
- Duplicate `tenant_id` onto tenant-owned rows and include it in repository predicates
  and relationship constraints. Do not rely on an API lookup followed by an unscoped
  database query.
- Encode cross-entity ownership facts in composite foreign keys:
  - a run's session and workspace must identify the same session row;
  - messages and checkpoints must identify the same run/session pair;
  - an approval's optional tool call must belong to that approval's run;
  - a run's selected checkpoint must belong to that run.
- Enforce database uniqueness for run-creation idempotency, logical tool calls, model
  request IDs, message ordering, checkpoint positions, and `(run_id, sequence)`.
- Store structured values as JSONB only after domain validation. Use aware UTC
  timestamps and closed enum/check constraints, including the complete fourteen-value
  event-type set.
- Revalidate run idempotency keys and require the supplied creation hash to equal the
  canonical hash of the run payload before constructing SQL. Revalidate gateway
  request IDs and hashes at the PostgreSQL adapter boundary rather than trusting
  annotations alone.
- Enforce the gateway-request state/payload relationship in PostgreSQL and retain a
  timestamped half-open circuit probe so abandoned probe ownership can be recovered.
- Treat tool names, argument payloads and hashes, turn numbers, model routes, request
  IDs, and start times as immutable parts of their logical persistence identities.
- Allocate sessions through injected `async_sessionmaker` instances. Engines have
  bounded pools, pre-ping, statement timeouts, UTC sessions, explicit readiness, and
  async cleanup.
- Keep declarative and migration check constraints identical. A unit contract extracts
  every named check from the initial revision and compares its normalized SQL with the
  SQLAlchemy metadata.
- Serialize engine disposal, make it idempotent, and shield it from caller
  cancellation. Cleanup failure leaves disposal retryable and readiness false only
  after disposal succeeds.
- Keep migrations credential-free. `AGENT_PLATFORM_DATABASE_URL` is loaded at runtime
  and is never embedded in migration files or logs.

## Consequences

- API restart no longer loses persisted sessions or run state.
- Persistence adapters remain outside `agent-core`; the core owns contracts and
  transition policy.
- PostgreSQL is required for production control-plane startup and readiness.
- Revision `0002` (Sequences 17–20) adds queue-specific lease tables and claim indexes
  without changing Sequence 14 repository ownership.
- Repository decision branches are covered with deterministic transaction doubles;
  the Podman PostgreSQL suite remains the authoritative integration check for database
  locking, constraints, migrations, and restart durability.
- Real PostgreSQL negative tests deliberately cross workspace/session, run/session,
  approval/tool, and run/checkpoint identities and require referential-integrity
  failures.

## Migration

Run `make migrate` against a reachable PostgreSQL database. `make migration-check`
detects model changes that lack a revision. Downgrade removes only Sequence 14-owned
tables in dependency-safe order.
