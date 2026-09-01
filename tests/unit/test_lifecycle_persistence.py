from __future__ import annotations

import uuid
from collections import deque
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Self, cast

import pytest

from agent_core.artifacts import StoredObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.lifecycle import (
    AuditExport,
    AuditExportStatus,
    LegalHold,
    ObjectDeletionLease,
    ObjectDeletionReason,
    TenantLifecycleStatus,
)
from platform_persistence.lifecycle import (
    PostgresLifecycleRepository,
    _audit_entry,
    _audit_export,
    _batch_size,
    _has_active_hold,
    _insert_deletion_jobs,
    _lease_seconds,
    _legal_hold,
    _object_deletion_lease,
    _recover_expired_jobs,
    _require_deletion_lease,
    _require_no_active_runs,
    _require_no_hold,
    _require_no_running_deletions,
    _require_objects_deleted,
    _tenant_cleanup_allowed,
    _tenant_lifecycle,
    _worker_id,
)
from platform_persistence.models import (
    AuditExportRecord,
    AuditRecord,
    LegalHoldRecord,
    ObjectDeletionJobRecord,
    TenantLifecycleRecord,
)

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
OTHER_TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000002")
EXPORT_ID = uuid.UUID("20000000-0000-0000-0000-000000000001")
HOLD_ID = uuid.UUID("30000000-0000-0000-0000-000000000001")
REQUEST_ID = uuid.UUID("40000000-0000-0000-0000-000000000001")
JOB_ID = uuid.UUID("50000000-0000-0000-0000-000000000001")
ARTIFACT_ID = uuid.UUID("60000000-0000-0000-0000-000000000001")
LEASE_TOKEN = uuid.UUID("70000000-0000-0000-0000-000000000001")
SHA256 = "a" * 64


class _ScalarRows:
    def __init__(self, values: list[object]) -> None:
        self._values = values

    def all(self) -> list[object]:
        return self._values


class _Database:
    def __init__(
        self,
        *,
        gets: list[object | None] | None = None,
        scalars: list[object] | None = None,
        scalar_sets: list[list[object]] | None = None,
    ) -> None:
        self.get_values = deque(gets or [])
        self.scalar_values = deque(scalars or [])
        self.scalar_sets = deque(scalar_sets or [])
        self.added: list[object] = []
        self.executed: list[object] = []
        self.flushes = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def begin(self) -> Self:
        return self

    async def get(self, *_args: object, **_kwargs: object) -> object | None:
        if not self.get_values:
            raise AssertionError("unexpected get query")
        return self.get_values.popleft()

    async def scalar(self, statement: object) -> object:
        self.executed.append(statement)
        if not self.scalar_values:
            raise AssertionError("unexpected scalar query")
        return self.scalar_values.popleft()

    async def scalars(self, statement: object) -> _ScalarRows:
        self.executed.append(statement)
        if not self.scalar_sets:
            raise AssertionError("unexpected scalar-set query")
        return _ScalarRows(self.scalar_sets.popleft())

    async def execute(self, statement: object) -> None:
        self.executed.append(statement)

    def add(self, value: object) -> None:
        self.added.append(value)

    async def flush(self) -> None:
        self.flushes += 1


class _Sessions:
    def __init__(self, *databases: _Database) -> None:
        self._databases = deque(databases)

    def __call__(self) -> _Database:
        if not self._databases:
            raise AssertionError("unexpected database session")
        return self._databases.popleft()


def _repository(*databases: _Database) -> PostgresLifecycleRepository:
    return PostgresLifecycleRepository(cast("Any", _Sessions(*databases)))


def _as_session(database: _Database) -> Any:
    return cast("Any", database)


def _active_lifecycle_row() -> TenantLifecycleRecord:
    return TenantLifecycleRecord(
        tenant_id=TENANT_ID,
        status="active",
        request_id=None,
        requested_by=None,
        requested_at=None,
        delete_after=None,
        audit_export_id=None,
        deletion_started_at=None,
        deleted_at=None,
        updated_at=NOW,
    )


def _lifecycle_row(
    status: TenantLifecycleStatus = TenantLifecycleStatus.DELETION_REQUESTED,
) -> TenantLifecycleRecord:
    started_at = (
        NOW + timedelta(hours=2)
        if status in {TenantLifecycleStatus.DELETING, TenantLifecycleStatus.DELETED}
        else None
    )
    deleted_at = NOW + timedelta(hours=3) if status is TenantLifecycleStatus.DELETED else None
    return TenantLifecycleRecord(
        tenant_id=TENANT_ID,
        status=status.value,
        request_id=REQUEST_ID,
        requested_by="operator",
        requested_at=NOW,
        delete_after=NOW + timedelta(hours=1),
        audit_export_id=EXPORT_ID,
        deletion_started_at=started_at,
        deleted_at=deleted_at,
        updated_at=deleted_at or started_at or NOW,
    )


def _pending_export() -> AuditExport:
    return AuditExport(
        id=EXPORT_ID,
        tenant_id=TENANT_ID,
        status=AuditExportStatus.PENDING,
        requested_by="operator",
        cutoff_at=NOW,
        created_at=NOW,
    )


def _completed_export() -> AuditExport:
    return _pending_export().model_copy(
        update={
            "status": AuditExportStatus.COMPLETED,
            "object": StoredObject(
                object_key=f"audit/{TENANT_ID}/{EXPORT_ID}.jsonl",
                sha256=SHA256,
                size_bytes=12,
                content_type="application/x-ndjson",
                etag="etag",
            ),
            "record_count": 1,
            "first_occurred_at": NOW,
            "last_occurred_at": NOW,
            "completed_at": NOW + timedelta(seconds=1),
        }
    )


def _export_row(
    status: AuditExportStatus = AuditExportStatus.PENDING,
) -> AuditExportRecord:
    completed = status is AuditExportStatus.COMPLETED
    failed = status is AuditExportStatus.FAILED
    return AuditExportRecord(
        id=EXPORT_ID,
        tenant_id=TENANT_ID,
        status=status.value,
        requested_by="operator",
        cutoff_at=NOW,
        object_key=f"audit/{TENANT_ID}/{EXPORT_ID}.jsonl" if completed else None,
        sha256=SHA256 if completed else None,
        size_bytes=12 if completed else None,
        content_type="application/x-ndjson" if completed else None,
        etag="etag" if completed else None,
        record_count=1 if completed else None,
        first_occurred_at=NOW if completed else None,
        last_occurred_at=NOW if completed else None,
        created_at=NOW,
        completed_at=NOW + timedelta(seconds=1) if completed else None,
        error={"code": "export_failed", "message": "failed"} if failed else None,
    )


def _hold_row(*, released: bool = False) -> LegalHoldRecord:
    return LegalHoldRecord(
        id=HOLD_ID,
        tenant_id=TENANT_ID,
        reason="litigation",
        placed_by="operator",
        placed_at=NOW,
        expires_at=None,
        released_by="operator" if released else None,
        released_at=NOW + timedelta(seconds=1) if released else None,
    )


def _hold() -> LegalHold:
    return _legal_hold(_hold_row())


def _job_row(
    *,
    status: str = "running",
    reason: ObjectDeletionReason = ObjectDeletionReason.TENANT_DELETION,
) -> ObjectDeletionJobRecord:
    return ObjectDeletionJobRecord(
        id=JOB_ID,
        tenant_id=TENANT_ID,
        artifact_id=ARTIFACT_ID,
        object_key=f"artifacts/{TENANT_ID}/{ARTIFACT_ID}",
        reason=reason.value,
        status=status,
        worker_id="worker-1" if status == "running" else None,
        lease_token=LEASE_TOKEN if status == "running" else None,
        outcome_lease_token=LEASE_TOKEN if status in {"completed", "failed"} else None,
        lease_generation=1,
        attempt=1,
        lease_expires_at=NOW + timedelta(minutes=1) if status == "running" else None,
        error=None,
        created_at=NOW,
        updated_at=NOW,
        completed_at=NOW if status == "completed" else None,
    )


def _lease(row: ObjectDeletionJobRecord | None = None) -> ObjectDeletionLease:
    return _object_deletion_lease(row or _job_row())


def _assert_code(error: pytest.ExceptionInfo[DomainOperationError], code: str) -> None:
    assert error.value.code == code


@pytest.mark.asyncio
async def test_access_reads_and_audit_iteration() -> None:
    assert await _repository(_Database(gets=[None])).get_tenant_lifecycle(TENANT_ID) is None
    await _repository(_Database(gets=[_active_lifecycle_row()])).require_active(TENANT_ID)
    with pytest.raises(DomainOperationError) as inactive:
        await _repository(_Database(gets=[_lifecycle_row()])).require_active(TENANT_ID)
    _assert_code(inactive, "tenant_not_active")

    assert await _repository(_Database(gets=[None])).get_audit_export(TENANT_ID, EXPORT_ID) is None
    other = _export_row()
    other.tenant_id = OTHER_TENANT_ID
    assert await _repository(_Database(gets=[other])).get_audit_export(TENANT_ID, EXPORT_ID) is None
    assert (
        await _repository(_Database(gets=[_export_row()])).get_audit_export(TENANT_ID, EXPORT_ID)
        == _pending_export()
    )
    assert await _repository(_Database(gets=[None])).get_legal_hold(TENANT_ID, HOLD_ID) is None
    assert (
        await _repository(_Database(gets=[_hold_row()])).get_legal_hold(TENANT_ID, HOLD_ID)
        == _hold()
    )
    assert await _repository(_Database(scalars=[1])).has_active_legal_hold(
        TENANT_ID, occurred_at=NOW
    )

    audit = AuditRecord(
        id=uuid.uuid4(),
        tenant_id=TENANT_ID,
        subject="operator",
        method="DELETE",
        resource="/tenant",
        action="tenant.delete",
        request_id="request",
        details={"safe": True},
        occurred_at=NOW,
    )
    batches = [
        batch
        async for batch in _repository(
            _Database(scalar_sets=[[audit]]), _Database(scalar_sets=[[]])
        ).iter_audit_entries(TENANT_ID, cutoff_at=NOW, batch_size=1)
    ]
    assert batches == [(_audit_entry(audit),)]
    with pytest.raises(ValueError):
        async for _ in _repository().iter_audit_entries(TENANT_ID, cutoff_at=NOW, batch_size=0):
            pass


@pytest.mark.asyncio
async def test_audit_export_lifecycle_and_conflicts() -> None:
    pending = _pending_export()
    created_db = _Database(gets=[None])
    assert await _repository(created_db).create_audit_export(pending) == pending
    assert isinstance(created_db.added[0], AuditExportRecord)
    assert (
        await _repository(_Database(gets=[_export_row()])).create_audit_export(pending) == pending
    )
    changed = _export_row()
    changed.requested_by = "other"
    with pytest.raises(DomainOperationError) as conflict:
        await _repository(_Database(gets=[changed])).create_audit_export(pending)
    _assert_code(conflict, "audit_export_conflict")
    with pytest.raises(ValueError):
        await _repository().create_audit_export(_completed_export())

    completed = _completed_export()
    row = _export_row()
    assert await _repository(_Database(gets=[row])).complete_audit_export(completed) == completed
    assert row.status == "completed"
    assert (
        await _repository(
            _Database(gets=[_export_row(AuditExportStatus.COMPLETED)])
        ).complete_audit_export(completed)
        == completed
    )
    with pytest.raises(DomainOperationError) as missing:
        await _repository(_Database(gets=[None])).complete_audit_export(completed)
    _assert_code(missing, "audit_export_not_found")
    with pytest.raises(ValueError):
        await _repository().complete_audit_export(pending)

    failed_row = _export_row()
    failed = await _repository(_Database(gets=[failed_row])).fail_audit_export(
        EXPORT_ID, TENANT_ID, error=ErrorDetail(code="export_failed", message="failed")
    )
    assert failed.status is AuditExportStatus.FAILED
    assert failed.error is not None
    with pytest.raises(DomainOperationError) as terminal:
        await _repository(
            _Database(gets=[_export_row(AuditExportStatus.COMPLETED)])
        ).fail_audit_export(
            EXPORT_ID, TENANT_ID, error=ErrorDetail(code="export_failed", message="failed")
        )
    _assert_code(terminal, "audit_export_conflict")


@pytest.mark.asyncio
async def test_legal_hold_place_release_and_rejections() -> None:
    hold = _hold()
    created_db = _Database(gets=[None, None], scalars=[False])
    assert await _repository(created_db).place_legal_hold(hold) == hold
    assert isinstance(created_db.added[0], LegalHoldRecord)
    with pytest.raises(DomainOperationError) as running:
        await _repository(_Database(scalars=[True])).place_legal_hold(hold)
    _assert_code(running, "legal_hold_cleanup_in_progress")

    released_row = _hold_row(released=True)
    assert await _repository(_Database(gets=[released_row])).release_legal_hold(
        TENANT_ID, HOLD_ID, released_by="operator", released_at=NOW + timedelta(seconds=1)
    ) == _legal_hold(released_row)
    with pytest.raises(DomainOperationError) as missing:
        await _repository(_Database(gets=[None])).release_legal_hold(
            TENANT_ID, HOLD_ID, released_by="operator", released_at=NOW
        )
    _assert_code(missing, "legal_hold_not_found")
    row = _hold_row()
    released = await _repository(_Database(gets=[row])).release_legal_hold(
        TENANT_ID, HOLD_ID, released_by="operator", released_at=NOW + timedelta(seconds=1)
    )
    assert released.released_by == "operator"
    with pytest.raises(ValueError):
        await _repository(_Database(gets=[_hold_row()])).release_legal_hold(
            TENANT_ID, HOLD_ID, released_by="operator", released_at=NOW - timedelta(seconds=1)
        )


@pytest.mark.asyncio
async def test_request_and_begin_tenant_deletion() -> None:
    export = _export_row(AuditExportStatus.COMPLETED)
    requested_db = _Database(gets=[None, export], scalars=[False, False])
    requested = await _repository(requested_db).request_tenant_deletion(
        TENANT_ID,
        request_id=REQUEST_ID,
        requested_by="operator",
        requested_at=NOW,
        delete_after=NOW + timedelta(hours=1),
        audit_export_id=EXPORT_ID,
    )
    assert requested.status is TenantLifecycleStatus.DELETION_REQUESTED
    assert any(isinstance(row, AuditRecord) for row in requested_db.added)

    existing = _lifecycle_row()
    assert await _repository(_Database(gets=[existing])).request_tenant_deletion(
        TENANT_ID,
        request_id=REQUEST_ID,
        requested_by="operator",
        requested_at=NOW,
        delete_after=NOW + timedelta(hours=1),
        audit_export_id=EXPORT_ID,
    ) == _tenant_lifecycle(existing)
    with pytest.raises(ValueError):
        await _repository().request_tenant_deletion(
            TENANT_ID,
            request_id=REQUEST_ID,
            requested_by="operator",
            requested_at=NOW,
            delete_after=NOW - timedelta(seconds=1),
            audit_export_id=EXPORT_ID,
        )
    with pytest.raises(DomainOperationError) as no_export:
        await _repository(
            _Database(gets=[None, None], scalars=[False, False])
        ).request_tenant_deletion(
            TENANT_ID,
            request_id=REQUEST_ID,
            requested_by="operator",
            requested_at=NOW,
            delete_after=NOW,
            audit_export_id=EXPORT_ID,
        )
    _assert_code(no_export, "audit_export_required")

    row = _lifecycle_row()
    begun = await _repository(
        _Database(gets=[row, export], scalars=[False, False])
    ).begin_tenant_deletion(TENANT_ID, occurred_at=NOW + timedelta(hours=2))
    assert begun.status is TenantLifecycleStatus.DELETING
    with pytest.raises(DomainOperationError) as cooling:
        await _repository(_Database(gets=[_lifecycle_row()])).begin_tenant_deletion(
            TENANT_ID, occurred_at=NOW
        )
    _assert_code(cooling, "tenant_deletion_cooling_off")
    deleting = _lifecycle_row(TenantLifecycleStatus.DELETING)
    assert await _repository(_Database(gets=[deleting])).begin_tenant_deletion(
        TENANT_ID, occurred_at=NOW + timedelta(hours=3)
    ) == _tenant_lifecycle(deleting)


@pytest.mark.asyncio
async def test_enqueue_retention_and_tenant_objects() -> None:
    artifact = SimpleNamespace(
        tenant_id=TENANT_ID, id=ARTIFACT_ID, object_key=f"artifacts/{TENANT_ID}/{ARTIFACT_ID}"
    )
    retention_db = _Database(gets=[None], scalars=[False], scalar_sets=[[artifact], [JOB_ID]])
    assert await _repository(retention_db).enqueue_expired_artifacts(occurred_at=NOW, limit=10) == 1

    snapshot = SimpleNamespace(
        tenant_id=TENANT_ID, object_key=f"snapshots/{TENANT_ID}/{uuid.uuid4()}"
    )
    deleting = _lifecycle_row(TenantLifecycleStatus.DELETING)
    tenant_db = _Database(
        gets=[deleting],
        scalars=[NOW + timedelta(hours=2), False],
        scalar_sets=[[artifact], [JOB_ID], [snapshot], [uuid.uuid4()]],
    )
    assert await _repository(tenant_db).enqueue_tenant_objects(TENANT_ID, limit=10) == 2
    full_db = _Database(
        gets=[deleting],
        scalars=[NOW + timedelta(hours=2), False],
        scalar_sets=[[artifact] * 2, [JOB_ID]],
    )
    assert await _repository(full_db).enqueue_tenant_objects(TENANT_ID, limit=2) == 1
    with pytest.raises(DomainOperationError) as not_started:
        await _repository(_Database(gets=[_active_lifecycle_row()])).enqueue_tenant_objects(
            TENANT_ID, limit=1
        )
    _assert_code(not_started, "tenant_deletion_not_started")


@pytest.mark.asyncio
async def test_claim_complete_and_fail_object_deletions() -> None:
    pending = _job_row(status="pending")
    pending.attempt = 0
    pending.lease_generation = 0
    claim_db = _Database(scalars=[False], scalar_sets=[[pending]])
    leases = await _repository(claim_db).claim_object_deletions(
        "worker-1", occurred_at=NOW, lease_seconds=30, limit=1
    )
    assert len(leases) == 1
    assert pending.status == "running"

    row = _job_row(reason=ObjectDeletionReason.RETENTION_EXPIRED)
    lease = _lease(row)
    complete_db = _Database(gets=[row], scalars=[NOW + timedelta(seconds=1)])
    await _repository(complete_db).complete_object_deletion(lease)
    assert row.status == "completed"
    assert row.artifact_id is None
    await _repository(_Database(gets=[row])).complete_object_deletion(lease)

    failed_row = _job_row()
    failed_lease = _lease(failed_row)
    await _repository(
        _Database(gets=[failed_row], scalars=[NOW + timedelta(seconds=1)])
    ).fail_object_deletion(
        failed_lease,
        error=ErrorDetail(code="object_delete_failed", message="retry", retryable=True),
        occurred_at=NOW + timedelta(seconds=1),
    )
    assert failed_row.status == "pending"
    terminal_row = _job_row()
    terminal_row.attempt = 100
    terminal_lease = _lease(terminal_row)
    await _repository(
        _Database(gets=[terminal_row], scalars=[NOW + timedelta(seconds=1)])
    ).fail_object_deletion(
        terminal_lease,
        error=ErrorDetail(code="object_delete_failed", message="failed", retryable=True),
        occurred_at=NOW + timedelta(seconds=1),
    )
    assert terminal_row.status == "failed"
    await _repository(_Database(gets=[terminal_row])).fail_object_deletion(
        terminal_lease,
        error=ErrorDetail(code="object_delete_failed", message="failed"),
        occurred_at=NOW + timedelta(seconds=2),
    )
    with pytest.raises(DomainOperationError) as lost:
        await _repository(_Database(gets=[None])).complete_object_deletion(lease)
    _assert_code(lost, "object_deletion_lease_lost")


@pytest.mark.asyncio
async def test_finalize_tenant_deletion_and_object_guard() -> None:
    lifecycle = _lifecycle_row(TenantLifecycleStatus.DELETING)
    database = _Database(gets=[lifecycle], scalars=[False, False, False, False, False])
    deleted = await _repository(database).finalize_tenant_deletion(
        TENANT_ID, occurred_at=NOW + timedelta(hours=3)
    )
    assert deleted.status is TenantLifecycleStatus.DELETED
    assert len(database.executed) >= 15
    already = _lifecycle_row(TenantLifecycleStatus.DELETED)
    assert await _repository(_Database(gets=[already])).finalize_tenant_deletion(
        TENANT_ID, occurred_at=NOW + timedelta(hours=4)
    ) == _tenant_lifecycle(already)
    with pytest.raises(DomainOperationError) as missing:
        await _repository(_Database(gets=[None])).finalize_tenant_deletion(
            TENANT_ID, occurred_at=NOW
        )
    _assert_code(missing, "tenant_deletion_not_started")
    with pytest.raises(DomainOperationError) as objects:
        await _require_objects_deleted(
            _as_session(_Database(scalars=[True, False, False])), TENANT_ID
        )
    _assert_code(objects, "tenant_objects_not_deleted")


@pytest.mark.asyncio
async def test_internal_guards_mappings_and_validation() -> None:
    assert not await _has_active_hold(_as_session(_Database(scalars=[False])), TENANT_ID, NOW)
    assert await _tenant_cleanup_allowed(
        _as_session(_Database(gets=[None], scalars=[False])), TENANT_ID, NOW
    )
    assert not await _tenant_cleanup_allowed(_as_session(_Database(scalars=[True])), TENANT_ID, NOW)
    with pytest.raises(DomainOperationError) as held:
        await _require_no_hold(_as_session(_Database(scalars=[True])), TENANT_ID, NOW)
    _assert_code(held, "tenant_legal_hold_active")
    with pytest.raises(DomainOperationError) as active:
        await _require_no_active_runs(_as_session(_Database(scalars=[True])), TENANT_ID)
    _assert_code(active, "tenant_runs_active")
    with pytest.raises(DomainOperationError) as cleanup:
        await _require_no_running_deletions(_as_session(_Database(scalars=[True])), TENANT_ID)
    _assert_code(cleanup, "legal_hold_cleanup_in_progress")

    assert await _insert_deletion_jobs(_as_session(_Database()), [], NOW) == 0
    recovered = _Database()
    await _recover_expired_jobs(_as_session(recovered), NOW)
    assert len(recovered.executed) == 2
    row = _job_row()
    _require_deletion_lease(row, _lease(row))
    row.worker_id = "other"
    with pytest.raises(DomainOperationError) as lost:
        _require_deletion_lease(row, _lease())
    _assert_code(lost, "object_deletion_lease_lost")
    assert _tenant_lifecycle(_active_lifecycle_row()).status is TenantLifecycleStatus.ACTIVE
    assert _legal_hold(_hold_row()).reason == "litigation"
    assert _audit_export(_export_row(AuditExportStatus.COMPLETED)).object is not None

    for invalid in (0, 1001, True):
        with pytest.raises(ValueError):
            _batch_size(invalid)
    for invalid in (0, 301, True):
        with pytest.raises(ValueError):
            _lease_seconds(invalid)
    for invalid_worker in ("", "\x00", "x" * 256):
        with pytest.raises(ValueError):
            _worker_id(invalid_worker)
