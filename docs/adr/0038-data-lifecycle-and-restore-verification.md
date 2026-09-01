# ADR 0038: Data lifecycle and restore verification

## Status

Accepted.

## Context

The production-readiness review found that immutable artifacts had optional expiry metadata and the
platform had an append-only audit log, but no executable retention worker, legal hold, export-first
tenant deletion, or cross-store restore verifier. Deleting object metadata before object storage,
accepting application traffic during deletion, or trusting a database restore without re-hashing
its objects could lose evidence or expose partially deleted tenant state.

Managed PostgreSQL backup schedules, object-store versioning/replication, KMS keys, and certificate
authorities are site-owned controls. The application cannot truthfully claim those controls merely
by shipping backup shell commands.

## Decision

- Add migration `0017` with immutable audit-export evidence, fail-closed tenant lifecycle
  tombstones, tenant-wide legal holds, and a fenced object-deletion outbox.
- Treat absence of a lifecycle row as active for existing tenants. Once deletion is requested, the
  control API, event API, and event WebSocket deny all application access after authentication.
  Lifecycle-store failure denies the operation rather than treating the tenant as active.
- Export audit rows in canonical, totally ordered, bounded JSONL before accepting a tenant deletion
  request. Uploads are checksum-bound and downloaded again for structural verification. A retry
  reuses the original export cutoff and request identity.
- Require a configurable cooling-off interval, no active legal hold, no nonterminal run, and an
  exact completed export before deletion begins. Destructive administrative commands require the
  tenant UUID twice.
- Queue object deletion before metadata deletion. Workers claim bounded leases with generation and
  token fencing, delete idempotently, and only then complete metadata removal. Legal holds block
  retention scans and object claims. Expired retention initially applies only to final patches,
  command logs, and evaluation reports; source and checkpoint retention needs an explicit policy.
- Preserve audit logs, audit exports, legal-hold history, deletion jobs, and the tenant tombstone
  after deleting application rows. Repository metadata and the compliance namespace are separate.
- Verify an isolated restore through read-only repeatable-read PostgreSQL queries plus streamed
  object downloads. Verification requires the exact Alembic head, no application rows for deleted
  tenants, contiguous run event sequences, valid checkpoint references, database/object size and
  checksum agreement, and valid audit-export structure. It emits a create-once `0600` evidence
  report and fails on the first divergence.
- Keep managed backup creation and rotation provider-neutral and operator-owned. The runbook
  requires encrypted PITR, immutable/versioned object backups, an isolated restore, dual-trust
  credential/certificate rollout, and forward-fix-first migrations.
- Use Podman only for local infrastructure verification. No Docker CLI, socket, daemon, image, or
  compatibility layer is introduced.

## Consequences

Tenant deletion is deliberately asynchronous and may require multiple prepare and cleanup batches.
An object-store delete can succeed before a worker loses its database lease; idempotent object
deletion plus the durable outbox makes the retry safe. A failed or pending cleanup job prevents
finalization and requires operator repair rather than silent metadata loss.

The restore report proves consistency only for the quiescent restored dependency pair it inspected.
It does not prove that a provider enabled PITR, met an RPO/RTO, replicated every version, or can
rotate credentials. Those claims require retained evidence from the target environment.

Audit and compliance evidence is intentionally retained after tenant application deletion. Legal
requirements determine its independent expiry and physical destruction policy.
