"""PostgreSQL tenant lifecycle, legal-hold, audit-export, and cleanup adapter."""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import TYPE_CHECKING, cast

from sqlalchemy import delete, exists, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from agent_core.artifacts import ArtifactKind, StoredObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.lifecycle import (
    MAX_LIFECYCLE_ATTEMPTS,
    MAX_LIFECYCLE_BATCH_SIZE,
    AuditExport,
    AuditExportEntry,
    AuditExportStatus,
    LegalHold,
    ObjectDeletionLease,
    ObjectDeletionReason,
    TenantLifecycle,
    TenantLifecycleStatus,
)
from platform_persistence.models import (
    ArtifactRecord,
    AuditExportRecord,
    AuditRecord,
    GatewayCapacityLeaseRecord,
    GatewayRateLimitRecord,
    GatewayRequestRecord,
    LegalHoldRecord,
    ObjectDeletionJobRecord,
    RunRecord,
    SessionRecord,
    SourceSnapshotRecord,
    TenantLifecycleRecord,
    TenantQuotaRecord,
    WorkspaceLeaseRecord,
    WorkspaceRecord,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable
    from datetime import datetime

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from agent_core.domain.base import AwareTimestamp
    from agent_core.lifecycle import LifecycleActor

_NONTERMINAL_RUN_STATUSES = (
    "queued",
    "leased",
    "running",
    "waiting_approval",
    "retry_pending",
    "lost",
)
_RETENTION_SAFE_KINDS = (
    ArtifactKind.FINAL_PATCH.value,
    ArtifactKind.COMMAND_LOG.value,
    ArtifactKind.EVALUATION_REPORT.value,
)
_MAX_LEASE_SECONDS = 300
_MAX_WORKER_ID_BYTES = 255
_TENANT_LOCK_SEED = 0x4147454E54


class PostgresLifecycleRepository:
    """Fenced lifecycle operations with deletion outbox coordination."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def require_active(self, tenant_id: uuid.UUID) -> None:
        lifecycle = await self.get_tenant_lifecycle(tenant_id)
        if lifecycle is not None and lifecycle.status is not TenantLifecycleStatus.ACTIVE:
            raise DomainOperationError(
                code="tenant_not_active",
                message="the tenant is not accepting application operations",
            )

    async def get_tenant_lifecycle(self, tenant_id: uuid.UUID) -> TenantLifecycle | None:
        async with self._sessions() as database:
            row = await database.get(TenantLifecycleRecord, tenant_id)
        return _tenant_lifecycle(row) if row is not None else None

    async def get_audit_export(
        self,
        tenant_id: uuid.UUID,
        export_id: uuid.UUID,
    ) -> AuditExport | None:
        async with self._sessions() as database:
            row = await database.get(AuditExportRecord, export_id)
        if row is None or row.tenant_id != tenant_id:
            return None
        return _audit_export(row)

    async def get_legal_hold(
        self,
        tenant_id: uuid.UUID,
        hold_id: uuid.UUID,
    ) -> LegalHold | None:
        async with self._sessions() as database:
            row = await database.get(LegalHoldRecord, hold_id)
        if row is None or row.tenant_id != tenant_id:
            return None
        return _legal_hold(row)

    async def has_active_legal_hold(
        self,
        tenant_id: uuid.UUID,
        *,
        occurred_at: AwareTimestamp,
    ) -> bool:
        async with self._sessions() as database:
            return bool(
                await database.scalar(
                    select(
                        exists().where(
                            LegalHoldRecord.tenant_id == tenant_id,
                            LegalHoldRecord.released_at.is_(None),
                            or_(
                                LegalHoldRecord.expires_at.is_(None),
                                LegalHoldRecord.expires_at > occurred_at,
                            ),
                        )
                    )
                )
            )

    async def iter_audit_entries(
        self,
        tenant_id: uuid.UUID,
        *,
        cutoff_at: AwareTimestamp,
        batch_size: int,
    ) -> AsyncIterator[tuple[AuditExportEntry, ...]]:
        _batch_size(batch_size)
        cursor: tuple[datetime, uuid.UUID] | None = None
        while True:
            statement = select(AuditRecord).where(
                AuditRecord.tenant_id == tenant_id,
                AuditRecord.occurred_at <= cutoff_at,
            )
            if cursor is not None:
                cursor_time, cursor_id = cursor
                statement = statement.where(
                    or_(
                        AuditRecord.occurred_at > cursor_time,
                        (AuditRecord.occurred_at == cursor_time) & (AuditRecord.id > cursor_id),
                    )
                )
            statement = statement.order_by(AuditRecord.occurred_at, AuditRecord.id).limit(
                batch_size
            )
            async with self._sessions() as database:
                rows = tuple((await database.scalars(statement)).all())
            if not rows:
                return
            yield tuple(_audit_entry(row) for row in rows)
            last = rows[-1]
            cursor = (last.occurred_at, last.id)

    async def create_audit_export(self, export: AuditExport) -> AuditExport:
        if export.status is not AuditExportStatus.PENDING:
            raise ValueError("new audit exports must be pending")
        async with self._sessions() as database, database.begin():
            row = await database.get(AuditExportRecord, export.id, with_for_update=True)
            if row is None:
                row = AuditExportRecord(
                    id=export.id,
                    tenant_id=export.tenant_id,
                    status=export.status.value,
                    requested_by=export.requested_by,
                    cutoff_at=export.cutoff_at,
                    created_at=export.created_at,
                )
                database.add(row)
                await database.flush()
            elif (
                row.tenant_id != export.tenant_id
                or row.requested_by != export.requested_by
                or row.cutoff_at != export.cutoff_at
                or row.created_at != export.created_at
            ):
                raise _conflict("audit_export_conflict", "audit export identity is already in use")
            return _audit_export(row)

    async def complete_audit_export(self, export: AuditExport) -> AuditExport:
        if export.status is not AuditExportStatus.COMPLETED:
            raise ValueError("audit export completion requires a completed model")
        async with self._sessions() as database, database.begin():
            row = await database.get(AuditExportRecord, export.id, with_for_update=True)
            if row is None or row.tenant_id != export.tenant_id:
                raise _conflict("audit_export_not_found", "audit export does not exist")
            if row.status == AuditExportStatus.COMPLETED.value:
                current = _audit_export(row)
                if current != export:
                    raise _conflict(
                        "audit_export_conflict",
                        "audit export already has a different outcome",
                    )
                return current
            if row.status != AuditExportStatus.PENDING.value:
                raise _conflict("audit_export_conflict", "audit export is already terminal")
            if (
                row.requested_by != export.requested_by
                or row.cutoff_at != export.cutoff_at
                or row.created_at != export.created_at
            ):
                raise _conflict("audit_export_conflict", "audit export request changed")
            stored = export.object
            if stored is None:  # defensive against unchecked construction
                raise ValueError("completed audit export requires object metadata")
            row.status = export.status.value
            row.object_key = stored.object_key
            row.sha256 = stored.sha256
            row.size_bytes = stored.size_bytes
            row.content_type = stored.content_type
            row.etag = stored.etag
            row.record_count = export.record_count
            row.first_occurred_at = export.first_occurred_at
            row.last_occurred_at = export.last_occurred_at
            row.completed_at = export.completed_at
            row.error = None
            await database.flush()
            return _audit_export(row)

    async def fail_audit_export(
        self,
        export_id: uuid.UUID,
        tenant_id: uuid.UUID,
        *,
        error: ErrorDetail,
    ) -> AuditExport:
        async with self._sessions() as database, database.begin():
            row = await database.get(AuditExportRecord, export_id, with_for_update=True)
            if row is None or row.tenant_id != tenant_id:
                raise _conflict("audit_export_not_found", "audit export does not exist")
            if row.status == AuditExportStatus.COMPLETED.value:
                raise _conflict("audit_export_conflict", "completed audit export cannot fail")
            row.status = AuditExportStatus.FAILED.value
            row.object_key = None
            row.sha256 = None
            row.size_bytes = None
            row.content_type = None
            row.etag = None
            row.record_count = None
            row.first_occurred_at = None
            row.last_occurred_at = None
            row.completed_at = None
            row.error = error.model_dump(mode="json")
            await database.flush()
            return _audit_export(row)

    async def place_legal_hold(self, hold: LegalHold) -> LegalHold:
        async with self._sessions() as database, database.begin():
            await _tenant_lock(database, hold.tenant_id)
            await _require_no_running_deletions(database, hold.tenant_id)
            lifecycle = await database.get(TenantLifecycleRecord, hold.tenant_id)
            if lifecycle is not None and lifecycle.status == TenantLifecycleStatus.DELETED.value:
                raise _conflict("tenant_deleted", "a deleted tenant cannot receive a legal hold")
            row = await database.get(LegalHoldRecord, hold.id, with_for_update=True)
            if row is None:
                row = LegalHoldRecord(
                    id=hold.id,
                    tenant_id=hold.tenant_id,
                    reason=hold.reason,
                    placed_by=hold.placed_by,
                    placed_at=hold.placed_at,
                    expires_at=hold.expires_at,
                    released_by=hold.released_by,
                    released_at=hold.released_at,
                )
                database.add(row)
                await database.flush()
            elif _legal_hold(row) != hold:
                raise _conflict("legal_hold_conflict", "legal hold identity is already in use")
            return _legal_hold(row)

    async def release_legal_hold(
        self,
        tenant_id: uuid.UUID,
        hold_id: uuid.UUID,
        *,
        released_by: LifecycleActor,
        released_at: AwareTimestamp,
    ) -> LegalHold:
        async with self._sessions() as database, database.begin():
            await _tenant_lock(database, tenant_id)
            row = await database.get(LegalHoldRecord, hold_id, with_for_update=True)
            if row is None or row.tenant_id != tenant_id:
                raise _conflict("legal_hold_not_found", "legal hold does not exist")
            if row.released_at is not None:
                if row.released_by != released_by:
                    raise _conflict("legal_hold_conflict", "legal hold was already released")
                return _legal_hold(row)
            if released_at < row.placed_at:
                raise ValueError("legal-hold release may not precede placement")
            row.released_by = released_by
            row.released_at = released_at
            await database.flush()
            return _legal_hold(row)

    async def request_tenant_deletion(
        self,
        tenant_id: uuid.UUID,
        *,
        request_id: uuid.UUID,
        requested_by: LifecycleActor,
        requested_at: AwareTimestamp,
        delete_after: AwareTimestamp,
        audit_export_id: uuid.UUID,
    ) -> TenantLifecycle:
        if delete_after < requested_at:
            raise ValueError("delete_after may not precede requested_at")
        async with self._sessions() as database, database.begin():
            await _tenant_lock(database, tenant_id)
            row = await database.get(TenantLifecycleRecord, tenant_id, with_for_update=True)
            if row is not None and row.status != TenantLifecycleStatus.ACTIVE.value:
                current = _tenant_lifecycle(row)
                if (
                    current.request_id == request_id
                    and current.requested_by == requested_by
                    and current.requested_at == requested_at
                    and current.delete_after == delete_after
                    and current.audit_export_id == audit_export_id
                ):
                    return current
                raise _conflict("tenant_deletion_conflict", "tenant deletion is already requested")
            await _require_no_hold(database, tenant_id, requested_at)
            await _require_no_active_runs(database, tenant_id)
            export = await database.get(AuditExportRecord, audit_export_id)
            if (
                export is None
                or export.tenant_id != tenant_id
                or export.status != AuditExportStatus.COMPLETED.value
                or export.cutoff_at != requested_at
            ):
                raise _conflict(
                    "audit_export_required",
                    "tenant deletion requires a completed audit export at the request cutoff",
                )
            if row is None:
                row = TenantLifecycleRecord(tenant_id=tenant_id, status="active")
                database.add(row)
                await database.flush()
            row.status = TenantLifecycleStatus.DELETION_REQUESTED.value
            row.request_id = request_id
            row.requested_by = requested_by
            row.requested_at = requested_at
            row.delete_after = delete_after
            row.audit_export_id = audit_export_id
            row.deletion_started_at = None
            row.deleted_at = None
            row.updated_at = requested_at
            database.add(
                AuditRecord(
                    id=uuid.uuid5(uuid.NAMESPACE_URL, f"tenant-deletion:{request_id}"),
                    tenant_id=tenant_id,
                    subject=requested_by,
                    method="DELETE",
                    resource=f"/admin/tenants/{tenant_id}",
                    action="tenant.deletion_requested",
                    request_id=str(request_id),
                    details={
                        "audit_export_id": str(audit_export_id),
                        "delete_after": delete_after.isoformat(),
                    },
                    occurred_at=requested_at,
                )
            )
            await database.flush()
            return _tenant_lifecycle(row)

    async def begin_tenant_deletion(
        self,
        tenant_id: uuid.UUID,
        *,
        occurred_at: AwareTimestamp,
    ) -> TenantLifecycle:
        async with self._sessions() as database, database.begin():
            await _tenant_lock(database, tenant_id)
            row = await database.get(TenantLifecycleRecord, tenant_id, with_for_update=True)
            if row is None:
                raise _conflict("tenant_deletion_not_requested", "tenant deletion is not requested")
            if row.status == TenantLifecycleStatus.DELETING.value:
                return _tenant_lifecycle(row)
            if row.status != TenantLifecycleStatus.DELETION_REQUESTED.value:
                raise _conflict("tenant_deletion_conflict", "tenant cannot begin deletion")
            if row.delete_after is None or occurred_at < row.delete_after:
                raise _conflict(
                    "tenant_deletion_cooling_off",
                    "tenant deletion cooling-off period has not completed",
                )
            await _require_no_hold(database, tenant_id, occurred_at)
            await _require_no_active_runs(database, tenant_id)
            export = await database.get(AuditExportRecord, row.audit_export_id)
            if export is None or export.status != AuditExportStatus.COMPLETED.value:
                raise _conflict("audit_export_required", "tenant audit export is not complete")
            row.status = TenantLifecycleStatus.DELETING.value
            row.deletion_started_at = occurred_at
            row.updated_at = occurred_at
            await database.flush()
            return _tenant_lifecycle(row)

    async def enqueue_expired_artifacts(
        self,
        *,
        occurred_at: AwareTimestamp,
        limit: int,
    ) -> int:
        _batch_size(limit)
        active_hold = exists().where(
            LegalHoldRecord.tenant_id == ArtifactRecord.tenant_id,
            LegalHoldRecord.released_at.is_(None),
            or_(LegalHoldRecord.expires_at.is_(None), LegalHoldRecord.expires_at > occurred_at),
        )
        inactive_tenant = exists().where(
            TenantLifecycleRecord.tenant_id == ArtifactRecord.tenant_id,
            TenantLifecycleRecord.status != TenantLifecycleStatus.ACTIVE.value,
        )
        existing_job = exists().where(
            ObjectDeletionJobRecord.object_key == ArtifactRecord.object_key
        )
        statement = (
            select(ArtifactRecord)
            .where(
                ArtifactRecord.expires_at.is_not(None),
                ArtifactRecord.expires_at <= occurred_at,
                ArtifactRecord.kind.in_(_RETENTION_SAFE_KINDS),
                ~active_hold,
                ~inactive_tenant,
                ~existing_job,
            )
            .order_by(ArtifactRecord.expires_at, ArtifactRecord.id)
            .limit(limit)
        )
        async with self._sessions() as database, database.begin():
            rows = tuple((await database.scalars(statement)).all())
            tenant_ids = sorted({row.tenant_id for row in rows}, key=lambda value: value.int)
            for tenant_id in tenant_ids:
                await _tenant_lock(database, tenant_id)
            allowed_tenants = {
                tenant_id
                for tenant_id in tenant_ids
                if await _tenant_cleanup_allowed(database, tenant_id, occurred_at)
            }
            return await _insert_deletion_jobs(
                database,
                (
                    (row.tenant_id, row.id, row.object_key, ObjectDeletionReason.RETENTION_EXPIRED)
                    for row in rows
                    if row.tenant_id in allowed_tenants
                ),
                occurred_at,
            )

    async def enqueue_tenant_objects(self, tenant_id: uuid.UUID, *, limit: int) -> int:
        _batch_size(limit)
        async with self._sessions() as database, database.begin():
            await _tenant_lock(database, tenant_id)
            lifecycle = await database.get(TenantLifecycleRecord, tenant_id)
            if lifecycle is None or lifecycle.status != TenantLifecycleStatus.DELETING.value:
                raise _conflict("tenant_deletion_not_started", "tenant deletion has not started")
            database_now = cast("datetime", await database.scalar(select(func.now())))
            await _require_no_hold(database, tenant_id, database_now)
            artifact_rows = tuple(
                (
                    await database.scalars(
                        select(ArtifactRecord)
                        .where(
                            ArtifactRecord.tenant_id == tenant_id,
                            ~exists().where(
                                ObjectDeletionJobRecord.object_key == ArtifactRecord.object_key
                            ),
                        )
                        .order_by(ArtifactRecord.id)
                        .limit(limit)
                    )
                ).all()
            )
            inserted = await _insert_deletion_jobs(
                database,
                (
                    (row.tenant_id, row.id, row.object_key, ObjectDeletionReason.TENANT_DELETION)
                    for row in artifact_rows
                ),
                lifecycle.updated_at,
            )
            remaining = limit - len(artifact_rows)
            if remaining <= 0:
                return inserted
            snapshot_rows = tuple(
                (
                    await database.scalars(
                        select(SourceSnapshotRecord)
                        .where(
                            SourceSnapshotRecord.tenant_id == tenant_id,
                            ~exists().where(
                                ObjectDeletionJobRecord.object_key
                                == SourceSnapshotRecord.object_key
                            ),
                        )
                        .order_by(SourceSnapshotRecord.id)
                        .limit(remaining)
                    )
                ).all()
            )
            return inserted + await _insert_deletion_jobs(
                database,
                (
                    (row.tenant_id, None, row.object_key, ObjectDeletionReason.TENANT_DELETION)
                    for row in snapshot_rows
                ),
                lifecycle.updated_at,
            )

    async def claim_object_deletions(
        self,
        worker_id: str,
        *,
        occurred_at: AwareTimestamp,
        lease_seconds: int,
        limit: int,
    ) -> tuple[ObjectDeletionLease, ...]:
        _worker_id(worker_id)
        _lease_seconds(lease_seconds)
        _batch_size(limit)
        async with self._sessions() as database, database.begin():
            await _recover_expired_jobs(database, occurred_at)
            active_hold = exists().where(
                LegalHoldRecord.tenant_id == ObjectDeletionJobRecord.tenant_id,
                LegalHoldRecord.released_at.is_(None),
                or_(LegalHoldRecord.expires_at.is_(None), LegalHoldRecord.expires_at > occurred_at),
            )
            rows = tuple(
                (
                    await database.scalars(
                        select(ObjectDeletionJobRecord)
                        .where(
                            ObjectDeletionJobRecord.status == "pending",
                            ObjectDeletionJobRecord.attempt < MAX_LIFECYCLE_ATTEMPTS,
                            ~active_hold,
                        )
                        .order_by(ObjectDeletionJobRecord.created_at, ObjectDeletionJobRecord.id)
                        .with_for_update(skip_locked=True)
                        .limit(limit)
                    )
                ).all()
            )
            tenant_ids = sorted({row.tenant_id for row in rows}, key=lambda value: value.int)
            for tenant_id in tenant_ids:
                await _tenant_lock(database, tenant_id)
            held_tenants = {
                tenant_id
                for tenant_id in tenant_ids
                if await _has_active_hold(database, tenant_id, occurred_at)
            }
            expires_at = occurred_at + timedelta(seconds=lease_seconds)
            leases: list[ObjectDeletionLease] = []
            for row in rows:
                if row.tenant_id in held_tenants:
                    continue
                token = uuid.uuid4()
                row.status = "running"
                row.worker_id = worker_id
                row.lease_token = token
                row.outcome_lease_token = None
                row.lease_generation += 1
                row.attempt += 1
                row.lease_expires_at = expires_at
                row.error = None
                row.updated_at = occurred_at
                leases.append(_object_deletion_lease(row))
            await database.flush()
            return tuple(leases)

    async def complete_object_deletion(self, lease: ObjectDeletionLease) -> None:
        async with self._sessions() as database, database.begin():
            row = await database.get(ObjectDeletionJobRecord, lease.job_id, with_for_update=True)
            if row is None or row.tenant_id != lease.tenant_id:
                raise _conflict(
                    "object_deletion_lease_lost", "object deletion lease is no longer valid"
                )
            if row.status == "completed" and row.outcome_lease_token == lease.lease_token:
                return
            _require_deletion_lease(row, lease)
            completed_at = cast("datetime", await database.scalar(select(func.now())))
            if row.lease_expires_at is None or completed_at >= row.lease_expires_at:
                raise _conflict("object_deletion_lease_lost", "object deletion lease expired")
            artifact_id = row.artifact_id
            row.status = "completed"
            row.worker_id = None
            row.lease_token = None
            row.outcome_lease_token = lease.lease_token
            row.lease_expires_at = None
            row.error = None
            row.completed_at = completed_at
            row.updated_at = completed_at
            if (
                row.reason == ObjectDeletionReason.RETENTION_EXPIRED.value
                and artifact_id is not None
            ):
                row.artifact_id = None
                await database.flush()
                await database.execute(
                    delete(ArtifactRecord).where(
                        ArtifactRecord.tenant_id == lease.tenant_id,
                        ArtifactRecord.id == artifact_id,
                    )
                )

    async def fail_object_deletion(
        self,
        lease: ObjectDeletionLease,
        *,
        error: ErrorDetail,
        occurred_at: AwareTimestamp,
    ) -> None:
        async with self._sessions() as database, database.begin():
            row = await database.get(ObjectDeletionJobRecord, lease.job_id, with_for_update=True)
            if row is None or row.tenant_id != lease.tenant_id:
                raise _conflict(
                    "object_deletion_lease_lost", "object deletion lease is no longer valid"
                )
            if row.status == "failed" and row.outcome_lease_token == lease.lease_token:
                return
            _require_deletion_lease(row, lease)
            database_now = cast("datetime", await database.scalar(select(func.now())))
            if (
                row.lease_expires_at is None
                or database_now >= row.lease_expires_at
                or occurred_at < row.updated_at
                or occurred_at >= row.lease_expires_at
            ):
                raise _conflict("object_deletion_lease_lost", "object deletion lease expired")
            retry = error.retryable and row.attempt < MAX_LIFECYCLE_ATTEMPTS
            row.status = "pending" if retry else "failed"
            row.worker_id = None
            row.lease_token = None
            row.outcome_lease_token = None if retry else lease.lease_token
            row.lease_expires_at = None
            row.error = error.model_dump(mode="json")
            row.updated_at = database_now

    async def finalize_tenant_deletion(
        self,
        tenant_id: uuid.UUID,
        *,
        occurred_at: AwareTimestamp,
    ) -> TenantLifecycle:
        async with self._sessions() as database, database.begin():
            await _tenant_lock(database, tenant_id)
            await database.execute(text("SET LOCAL agent_platform.lifecycle_admin = 'on'"))
            lifecycle = await database.get(TenantLifecycleRecord, tenant_id, with_for_update=True)
            if lifecycle is None:
                raise _conflict("tenant_deletion_not_started", "tenant deletion has not started")
            if lifecycle.status == TenantLifecycleStatus.DELETED.value:
                return _tenant_lifecycle(lifecycle)
            if lifecycle.status != TenantLifecycleStatus.DELETING.value:
                raise _conflict("tenant_deletion_not_started", "tenant deletion has not started")
            await _require_no_hold(database, tenant_id, occurred_at)
            await _require_no_active_runs(database, tenant_id)
            await _require_objects_deleted(database, tenant_id)

            await database.execute(
                update(ObjectDeletionJobRecord)
                .where(ObjectDeletionJobRecord.tenant_id == tenant_id)
                .values(artifact_id=None)
            )
            await database.execute(
                update(RunRecord)
                .where(RunRecord.tenant_id == tenant_id)
                .values(last_checkpoint_id=None)
            )
            await database.execute(
                update(WorkspaceRecord)
                .where(WorkspaceRecord.tenant_id == tenant_id)
                .values(current_snapshot_id=None, status="archived", updated_at=occurred_at)
            )
            await database.execute(
                delete(SessionRecord).where(SessionRecord.tenant_id == tenant_id)
            )
            await database.execute(
                delete(SourceSnapshotRecord).where(SourceSnapshotRecord.tenant_id == tenant_id)
            )
            await database.execute(
                delete(ArtifactRecord).where(ArtifactRecord.tenant_id == tenant_id)
            )
            await database.execute(
                delete(WorkspaceLeaseRecord).where(WorkspaceLeaseRecord.tenant_id == tenant_id)
            )
            await database.execute(
                delete(WorkspaceRecord).where(WorkspaceRecord.tenant_id == tenant_id)
            )
            await database.execute(
                delete(GatewayCapacityLeaseRecord).where(
                    GatewayCapacityLeaseRecord.tenant_id == tenant_id
                )
            )
            await database.execute(
                delete(GatewayRateLimitRecord).where(GatewayRateLimitRecord.tenant_id == tenant_id)
            )
            await database.execute(
                delete(GatewayRequestRecord).where(GatewayRequestRecord.tenant_id == tenant_id)
            )
            await database.execute(
                delete(TenantQuotaRecord).where(TenantQuotaRecord.tenant_id == tenant_id)
            )
            lifecycle.status = TenantLifecycleStatus.DELETED.value
            lifecycle.deleted_at = occurred_at
            lifecycle.updated_at = occurred_at
            await database.flush()
            return _tenant_lifecycle(lifecycle)


async def _insert_deletion_jobs(
    database: AsyncSession,
    candidates: Iterable[tuple[uuid.UUID, uuid.UUID | None, str, ObjectDeletionReason]],
    occurred_at: datetime,
) -> int:
    values = [
        {
            "id": uuid.uuid5(uuid.NAMESPACE_URL, f"agent-object-deletion:{object_key}"),
            "tenant_id": tenant_id,
            "artifact_id": artifact_id,
            "object_key": object_key,
            "reason": reason.value,
            "status": "pending",
            "created_at": occurred_at,
            "updated_at": occurred_at,
        }
        for tenant_id, artifact_id, object_key, reason in candidates
    ]
    if not values:
        return 0
    statement = (
        pg_insert(ObjectDeletionJobRecord)
        .values(values)
        .on_conflict_do_nothing(index_elements=(ObjectDeletionJobRecord.object_key,))
        .returning(ObjectDeletionJobRecord.id)
    )
    return len((await database.scalars(statement)).all())


async def _recover_expired_jobs(database: AsyncSession, occurred_at: datetime) -> None:
    retry_error = ErrorDetail(
        code="object_deletion_lease_expired",
        message="the prior object deletion lease expired",
        retryable=True,
    ).model_dump(mode="json")
    await database.execute(
        update(ObjectDeletionJobRecord)
        .where(
            ObjectDeletionJobRecord.status == "running",
            ObjectDeletionJobRecord.lease_expires_at <= occurred_at,
            ObjectDeletionJobRecord.attempt < MAX_LIFECYCLE_ATTEMPTS,
        )
        .values(
            status="pending",
            worker_id=None,
            lease_token=None,
            outcome_lease_token=None,
            lease_expires_at=None,
            error=retry_error,
            updated_at=occurred_at,
        )
    )
    terminal_error = ErrorDetail(
        code="object_deletion_attempts_exhausted",
        message="object deletion exhausted its retry limit",
    ).model_dump(mode="json")
    await database.execute(
        update(ObjectDeletionJobRecord)
        .where(
            ObjectDeletionJobRecord.status == "running",
            ObjectDeletionJobRecord.lease_expires_at <= occurred_at,
            ObjectDeletionJobRecord.attempt >= MAX_LIFECYCLE_ATTEMPTS,
        )
        .values(
            status="failed",
            worker_id=None,
            outcome_lease_token=ObjectDeletionJobRecord.lease_token,
            lease_token=None,
            lease_expires_at=None,
            error=terminal_error,
            updated_at=occurred_at,
        )
    )


def _require_deletion_lease(
    row: ObjectDeletionJobRecord,
    lease: ObjectDeletionLease,
) -> None:
    if (
        row.status != "running"
        or row.worker_id != lease.worker_id
        or row.lease_token != lease.lease_token
        or row.lease_generation != lease.lease_generation
        or row.attempt != lease.attempt
        or row.tenant_id != lease.tenant_id
        or row.artifact_id != lease.artifact_id
        or row.object_key != lease.object_key
        or row.reason != lease.reason.value
        or row.lease_expires_at != lease.expires_at
    ):
        raise _conflict("object_deletion_lease_lost", "object deletion lease is no longer valid")


async def _tenant_lock(database: AsyncSession, tenant_id: uuid.UUID) -> None:
    await database.execute(
        select(func.pg_advisory_xact_lock(func.hashtextextended(str(tenant_id), _TENANT_LOCK_SEED)))
    )


async def _require_no_running_deletions(
    database: AsyncSession,
    tenant_id: uuid.UUID,
) -> None:
    running = await database.scalar(
        select(
            exists().where(
                ObjectDeletionJobRecord.tenant_id == tenant_id,
                ObjectDeletionJobRecord.status == "running",
            )
        )
    )
    if running:
        raise _conflict(
            "legal_hold_cleanup_in_progress",
            "a legal hold cannot begin while object deletion is in progress",
        )


async def _has_active_hold(
    database: AsyncSession,
    tenant_id: uuid.UUID,
    occurred_at: datetime,
) -> bool:
    return bool(
        await database.scalar(
            select(
                exists().where(
                    LegalHoldRecord.tenant_id == tenant_id,
                    LegalHoldRecord.released_at.is_(None),
                    or_(
                        LegalHoldRecord.expires_at.is_(None),
                        LegalHoldRecord.expires_at > occurred_at,
                    ),
                )
            )
        )
    )


async def _tenant_cleanup_allowed(
    database: AsyncSession,
    tenant_id: uuid.UUID,
    occurred_at: datetime,
) -> bool:
    if await _has_active_hold(database, tenant_id, occurred_at):
        return False
    lifecycle = await database.get(TenantLifecycleRecord, tenant_id)
    return lifecycle is None or lifecycle.status == TenantLifecycleStatus.ACTIVE.value


async def _require_no_hold(
    database: AsyncSession,
    tenant_id: uuid.UUID,
    occurred_at: datetime,
) -> None:
    if await _has_active_hold(database, tenant_id, occurred_at):
        raise _conflict("tenant_legal_hold_active", "tenant data is under an active legal hold")


async def _require_no_active_runs(database: AsyncSession, tenant_id: uuid.UUID) -> None:
    active = await database.scalar(
        select(
            exists().where(
                RunRecord.tenant_id == tenant_id,
                RunRecord.status.in_(_NONTERMINAL_RUN_STATUSES),
            )
        )
    )
    if active:
        raise _conflict("tenant_runs_active", "tenant deletion requires every run to be terminal")


async def _require_objects_deleted(database: AsyncSession, tenant_id: uuid.UUID) -> None:
    incomplete = await database.scalar(
        select(
            exists().where(
                ObjectDeletionJobRecord.tenant_id == tenant_id,
                ObjectDeletionJobRecord.status != "completed",
            )
        )
    )
    unqueued_artifact = await database.scalar(
        select(
            exists().where(
                ArtifactRecord.tenant_id == tenant_id,
                ~exists().where(ObjectDeletionJobRecord.object_key == ArtifactRecord.object_key),
            )
        )
    )
    unqueued_snapshot = await database.scalar(
        select(
            exists().where(
                SourceSnapshotRecord.tenant_id == tenant_id,
                ~exists().where(
                    ObjectDeletionJobRecord.object_key == SourceSnapshotRecord.object_key
                ),
            )
        )
    )
    if incomplete or unqueued_artifact or unqueued_snapshot:
        raise _conflict(
            "tenant_objects_not_deleted",
            "tenant object deletion is incomplete",
        )


def _tenant_lifecycle(row: TenantLifecycleRecord) -> TenantLifecycle:
    return TenantLifecycle(
        tenant_id=row.tenant_id,
        status=TenantLifecycleStatus(row.status),
        request_id=row.request_id,
        requested_by=row.requested_by,
        requested_at=row.requested_at,
        delete_after=row.delete_after,
        audit_export_id=row.audit_export_id,
        deletion_started_at=row.deletion_started_at,
        deleted_at=row.deleted_at,
        updated_at=row.updated_at,
    )


def _legal_hold(row: LegalHoldRecord) -> LegalHold:
    return LegalHold(
        id=row.id,
        tenant_id=row.tenant_id,
        reason=row.reason,
        placed_by=row.placed_by,
        placed_at=row.placed_at,
        expires_at=row.expires_at,
        released_by=row.released_by,
        released_at=row.released_at,
    )


def _audit_entry(row: AuditRecord) -> AuditExportEntry:
    return AuditExportEntry(
        id=row.id,
        tenant_id=row.tenant_id,
        subject=row.subject,
        method=row.method,
        resource=row.resource,
        action=row.action,
        request_id=row.request_id,
        details=row.details,
        occurred_at=row.occurred_at,
    )


def _audit_export(row: AuditExportRecord) -> AuditExport:
    stored = (
        StoredObject(
            object_key=cast("str", row.object_key),
            sha256=cast("str", row.sha256),
            size_bytes=cast("int", row.size_bytes),
            content_type=cast("str", row.content_type),
            etag=row.etag,
        )
        if row.status == AuditExportStatus.COMPLETED.value
        else None
    )
    return AuditExport(
        id=row.id,
        tenant_id=row.tenant_id,
        status=AuditExportStatus(row.status),
        requested_by=row.requested_by,
        cutoff_at=row.cutoff_at,
        created_at=row.created_at,
        object=stored,
        record_count=row.record_count,
        first_occurred_at=row.first_occurred_at,
        last_occurred_at=row.last_occurred_at,
        completed_at=row.completed_at,
        error=ErrorDetail.model_validate(row.error) if row.error is not None else None,
    )


def _object_deletion_lease(row: ObjectDeletionJobRecord) -> ObjectDeletionLease:
    return ObjectDeletionLease(
        job_id=row.id,
        tenant_id=row.tenant_id,
        artifact_id=row.artifact_id,
        object_key=row.object_key,
        reason=ObjectDeletionReason(row.reason),
        worker_id=cast("str", row.worker_id),
        lease_token=cast("uuid.UUID", row.lease_token),
        lease_generation=row.lease_generation,
        attempt=row.attempt,
        expires_at=cast("datetime", row.lease_expires_at),
    )


def _batch_size(value: int) -> None:
    if type(value) is not int or value < 1 or value > MAX_LIFECYCLE_BATCH_SIZE:
        raise ValueError("batch size must be between 1 and 1000")


def _lease_seconds(value: int) -> None:
    if type(value) is not int or value < 1 or value > _MAX_LEASE_SECONDS:
        raise ValueError("lease seconds must be between 1 and 300")


def _worker_id(value: str) -> None:
    if not value.strip() or len(value.encode("utf-8")) > _MAX_WORKER_ID_BYTES or "\x00" in value:
        raise ValueError("worker_id must be nonempty and at most 255 UTF-8 bytes")


def _conflict(code: str, message: str) -> DomainOperationError:
    return DomainOperationError(code=code, message=message)


__all__ = ["PostgresLifecycleRepository"]
