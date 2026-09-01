# Data lifecycle and recovery runbook

This runbook covers privileged lifecycle operations and isolated restore verification. It is not a
substitute for the managed PostgreSQL, object-storage, KMS, identity, or certificate-provider
procedures selected by a production site.

Every command uses the same validated `AGENT_PLATFORM_DATABASE_URL` and
`AGENT_PLATFORM_S3_*` settings as production. Run it from a reviewed release image with a dedicated
administrative identity. Never give these credentials to workers, models, tools, or sandboxes.

## Safety rules

1. Quiesce tenant writes before deletion and all platform writes before taking a coordinated manual
   recovery point or verifying a restore.
2. Place a legal hold before investigation or preservation. Holds block retention, deletion start,
   and deletion-object claims.
3. Never delete PostgreSQL artifact metadata before its immutable object delete is durably complete.
4. Treat every checksum, migration, event-sequence, tenant-residue, or audit-structure mismatch as a
   failed restore. Do not repair the evidence in place.
5. Retain command output, backup/provider identifiers, source revision, image digests, settings
   hashes, start/end timestamps, and the create-once verification report in the compliance system.
6. Use Podman for repository-provided local infrastructure tests. No Docker tooling is supported.

## Legal holds and audit exports

Use canonical lowercase UUIDs and a stable operator identity:

```shell
make lifecycle-admin LIFECYCLE_ARGS='place-hold --tenant-id <tenant-uuid> --actor <operator> --hold-id <hold-uuid> --reason <reviewed-reason>'
make lifecycle-admin LIFECYCLE_ARGS='export-audit --tenant-id <tenant-uuid> --actor <operator> --export-id <export-uuid>'
make lifecycle-admin LIFECYCLE_ARGS='verify-audit --tenant-id <tenant-uuid> --actor <operator> --export-id <export-uuid>'
make lifecycle-admin LIFECYCLE_ARGS='release-hold --tenant-id <tenant-uuid> --actor <operator> --hold-id <hold-uuid>'
```

An export is canonical JSONL with a header, totally ordered audit entries, and a count/timestamp
trailer. Its object key, byte size, SHA-256, range, and count are committed in PostgreSQL. Reusing an
export UUID with different tenant, actor, or cutoff fails closed.

## Tenant deletion

Deletion is export-first and intentionally multi-stage:

```shell
make lifecycle-admin LIFECYCLE_ARGS='request-deletion --tenant-id <tenant-uuid> --confirm-tenant-id <tenant-uuid> --actor <operator> --request-id <request-uuid> --export-id <export-uuid>'
make lifecycle-admin LIFECYCLE_ARGS='status --tenant-id <tenant-uuid>'
make lifecycle-admin LIFECYCLE_ARGS='prepare-deletion --tenant-id <tenant-uuid> --confirm-tenant-id <tenant-uuid> --actor <operator>'
make lifecycle-admin LIFECYCLE_ARGS='cleanup-objects --actor <operator> --worker-id <cleanup-worker> --confirm cleanup'
make lifecycle-admin LIFECYCLE_ARGS='finalize-deletion --tenant-id <tenant-uuid> --confirm-tenant-id <tenant-uuid> --actor <operator>'
```

`request-deletion` creates, downloads, and verifies the audit export before recording the cooling-off
deadline. From that point normal HTTP and event access is forbidden. `prepare-deletion` is valid only
after cooling-off, with no hold or nonterminal run, and enqueues at most one configured batch. Repeat
prepare and cleanup until no tenant object remains. `finalize-deletion` refuses pending, running,
failed, or unqueued objects and preserves compliance evidence plus the tenant tombstone.

To cancel a deletion during cooling-off, use a separately reviewed database/operator procedure only
after product and legal policy defines cancellation semantics. This release intentionally provides
no unaudited reset command.

## Retention worker

Run bounded scans and cleanup batches from a singleton or safely concurrent scheduled job:

```shell
make lifecycle-admin LIFECYCLE_ARGS='retention-scan --actor <operator> --confirm retention'
make lifecycle-admin LIFECYCLE_ARGS='cleanup-objects --actor <operator> --worker-id <stable-worker-id> --confirm cleanup'
```

The database uses `SKIP LOCKED`, unique object keys, lease tokens, generations, attempts, and
idempotent terminal outcomes. Alert on terminal failed jobs, repeated lease expiry, legal holds near
expiry, pending jobs older than policy, and deletion requests that do not progress. Do not manually
remove an outbox row to make finalization pass.

## Backup policy prerequisites

Before accepting traffic, the site owner must document and enable:

- encrypted managed PostgreSQL continuous archiving/PITR, a tested retention window, cross-account
  or otherwise isolated backup administration, deletion protection, and immutable backup logs;
- object-store versioning, encryption with reviewed KMS ownership, access logging, lifecycle rules
  that do not expire live or legally held evidence, and replication or immutable backup appropriate
  to the required failure domain;
- a method to select mutually consistent PostgreSQL and object-store recovery points. If the object
  store is restored later than PostgreSQL, extra unreferenced immutable objects are safe but must be
  inventoried; missing or changed referenced objects fail verification;
- site-approved RPO and RTO values. The repository does not invent them and no target is met until a
  timed rehearsal demonstrates it.

## Isolated restore rehearsal

1. Record the release source revision, all image digests, Alembic head, provider backup identifiers,
   object-version recovery point, and credential/certificate versions.
2. Restore PostgreSQL and object storage into a private account/project/namespace with no production
   ingress, workers, lifecycle jobs, or model-provider egress.
3. Use read-only database credentials where the provider permits them. Configure object credentials
   with read-only access to only the restored bucket.
4. Run the verifier; the output path must not exist:

```shell
make recovery-verify RECOVERY_ARGS='--backup-id <provider-backup-id> --source-revision <40-hex-revision> --output /private/evidence/recovery.json --temporary-parent /private/tmp'
```

5. Require `result=passed`, the expected migration head, every table count, contiguous events, no
   deleted-tenant residue, and the complete checksum scan. Store the `0600` JSON report immutably.
6. Run authenticated tenant-isolation, event replay, checkpoint restore, final artifact download,
   and one non-production coding journey against the isolated restore.
7. Destroy only the isolated restored environment under the provider's reviewed procedure. Retain
   sanitized evidence and timings.

The verifier downloads every referenced artifact, validated source upload, checkpoint snapshot, and
completed audit export within explicit per-object, count, and aggregate limits. Increase a limit
only through reviewed configuration; a limit failure is not a pass.

## Certificate and credential rotation

### Sandbox-node mTLS

1. Issue the new CA/intermediate and leaf certificates with the same constrained identities and
   private-key protections. Never copy private keys into images.
2. Deploy a CA bundle that trusts old and new issuers to node servers and worker clients. A PEM file
   may contain the dual trust chain.
3. Roll node servers with new server leaves while both issuers are trusted; prove TLS 1.3 identity
   checks and sandbox security on each node pool.
4. Roll workers with new client leaves, drain old workers, and prove replacement-worker checkpoint
   recovery.
5. Remove old trust only after all old leaves are expired/revoked and telemetry shows no old
   identity. Retain issuance, rollout, and negative old-certificate test evidence.

The processes load certificates at startup, so rotation is a controlled rolling restart, not a hot
reload. Emergency revocation skips the overlap only under the incident procedure and may reduce
availability.

### OIDC, storage, database, Redis, gateway, and provider credentials

- Publish overlapping OIDC signing keys for longer than JWKS cache plus clock skew, validate both,
  then retire the old key and rehearse issuer/JWKS outage behavior.
- Create a new least-privilege credential/version in the external secret system, deploy consumers,
  verify readiness and actual operations, then revoke the old version. Prefer workload identity or
  short-lived credentials over static secrets.
- Rotate LiteLLM provider credentials only in LiteLLM. Workers must remain free of provider keys.
- Rotate database credentials without changing backup ownership; verify PITR and the recovery reader
  separately. Rotate object KMS keys under provider guidance without deleting decrypt access needed
  by retained backups or legal holds.

## Schema migration and recovery

Use expand/contract migrations for production changes. Back up and rehearse on production-shaped
data before applying. Stop on lock-time or statement-time limits; do not disable them to force a
migration. After upgrade, run `alembic check`, application readiness, tenant isolation, queue/event
invariants, and the isolated restore verifier.

Forward-fix is the default after production data has been written under a new schema. Use an Alembic
downgrade only when that exact downgrade was rehearsed against a restored copy, no irreversible data
shape was introduced, and rollback of every application image is coordinated. Never edit an applied
migration or stamp past a failure to make the version table look healthy.
