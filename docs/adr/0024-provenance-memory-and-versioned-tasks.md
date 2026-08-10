# ADR 0024: Provenance memory and versioned task tracking

- Status: Accepted
- Date: 2026-07-31
- Sequence: 24

## Context

Task state and useful cross-turn knowledge must survive process loss. Model-generated
memory is untrusted and must not become unattributed tenant-global data.

## Decision

- Store every task-plan replacement as a new version and require an expected-version
  compare-and-set under the locked run row.
- Queue one idempotent memory extraction job after each completed run when both tenant
  and session policy permit it. PostgreSQL run completion and job insertion share one
  transaction; no event-time or pre-commit enqueue path exists.
- Claim extraction work with an expiring worker ID, random token, and monotonically
  increasing generation. Recover expired jobs with `SKIP LOCKED`, increment the
  attempt, and reject stale reads or terminal writes.
- Stream and bound the source at a durable message watermark, then validate the gateway's
  closed JSON envelope from the shared `summarization` route before persistence. The
  extraction deadline is strictly shorter than its fenced lease.
- Require every memory to include source tenant, source session, source run, extraction
  time, kind, a canonical content hash, and closed optional metadata.
- Deduplicate by tenant, session, kind, and content hash in the same transaction that
  completes the fenced extraction job. Archival is a tenant/session-scoped soft delete.
- Recheck memory enablement at claim, completion, and retrieval so later disablement
  suppresses both extraction and context use.
- Preserve old arbitrary task-plan rows by converting their legacy `steps` shape into a
  read-only typed view. New writes always use the closed task contract.

## Consequences

Task updates detect concurrent writers and survive restarts. Memory is attributable and
can be disabled or archived without deleting audit history. Extraction is asynchronous
and may lag run completion; its bounded deadline completes or fails the job while the
worker still owns the fenced lease.

## Migration

Revision `0006` adds per-session memory policy, memories, and extraction jobs. Existing
`task_plans` storage remains the versioned system of record. The revision includes
content-identity uniqueness and extraction lease lifecycle constraints so ORM and SQL
metadata enforce the same invariants.
