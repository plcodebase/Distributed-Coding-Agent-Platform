from __future__ import annotations

import asyncio
import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import ValidationError

from agent_core.artifacts import (
    ObjectStat,
    PresignedDownload,
    PresignedUpload,
    StoredObject,
)
from agent_core.audit import MAX_AUDIT_DETAILS_BYTES, MAX_AUDIT_ENTRY_BYTES, AuditEntry
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.lifecycle import (
    AuditExport,
    AuditExportEntry,
    AuditExportStatus,
    LegalHold,
    LifecycleAdministrationRepository,
    ObjectDeletionLease,
    ObjectDeletionReason,
    TenantLifecycle,
    TenantLifecycleStatus,
    tenant_audit_export_object_key,
)
from agent_core.lifecycle_service import (
    AuditExportService,
    LifecycleServiceConfig,
    ObjectDeletionWorker,
    TenantDeletionService,
    verify_audit_export_file,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from agent_core.artifacts import MediaType, ObjectKey
    from agent_core.domain.base import AwareTimestamp
    from agent_core.domain.models import Sha256Hex

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000101")
EXPORT_ID = uuid.UUID("00000000-0000-0000-0000-000000000102")
REQUEST_ID = uuid.UUID("00000000-0000-0000-0000-000000000103")


def _entry(index: int, *, tenant_id: uuid.UUID = TENANT_ID) -> AuditExportEntry:
    return AuditExportEntry(
        id=uuid.UUID(int=index + 1),
        tenant_id=tenant_id,
        subject="operator@example.invalid",
        method="POST",
        resource=f"/v1/runs/{index}",
        action="run.create",
        request_id=f"request-{index}",
        details={"index": index, "nested": [True, None]},
        occurred_at=NOW - timedelta(seconds=1) + timedelta(microseconds=index),
    )


class _ObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []
        self.delete_error: Exception | None = None

    async def ready(self) -> bool:
        return True

    async def create_upload(
        self,
        *,
        object_key: ObjectKey,
        content_type: MediaType,
        max_bytes: int,
        expires_at: AwareTimestamp,
    ) -> PresignedUpload:
        del object_key, content_type, max_bytes, expires_at
        raise NotImplementedError

    async def create_download(
        self,
        *,
        object_key: ObjectKey,
        expires_at: AwareTimestamp,
    ) -> PresignedDownload:
        del object_key, expires_at
        raise NotImplementedError

    async def head(self, object_key: ObjectKey) -> ObjectStat | None:
        content = self.objects.get(object_key)
        if content is None:
            return None
        return ObjectStat(
            object_key=object_key,
            size_bytes=len(content),
            content_type="application/x-ndjson",
        )

    async def download_to_path(
        self,
        object_key: ObjectKey,
        destination: Path,
        *,
        max_bytes: int,
        expected_sha256: Sha256Hex,
    ) -> StoredObject:
        content = self.objects[object_key]
        assert len(content) <= max_bytes
        assert hashlib.sha256(content).hexdigest() == expected_sha256
        await asyncio.to_thread(destination.write_bytes, content)
        return _stored(object_key, content)

    async def upload_from_path(
        self,
        object_key: ObjectKey,
        source: Path,
        *,
        content_type: MediaType,
        max_bytes: int,
    ) -> StoredObject:
        content = await asyncio.to_thread(source.read_bytes)
        assert len(content) <= max_bytes
        assert content_type == "application/x-ndjson"
        existing = self.objects.setdefault(object_key, content)
        if existing != content:
            raise DomainOperationError(
                code="object_conflict",
                message="object key has different content",
            )
        return _stored(object_key, content)

    async def delete(self, object_key: ObjectKey) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(object_key)
        self.objects.pop(object_key, None)

    async def aclose(self) -> None:
        return None


class _Repository:
    def __init__(self, entries: tuple[AuditExportEntry, ...] = ()) -> None:
        self.entries = entries
        self.exports: dict[uuid.UUID, AuditExport] = {}
        self.failed_exports: list[ErrorDetail] = []
        self.lifecycle: TenantLifecycle | None = None
        self.leases: tuple[ObjectDeletionLease, ...] = ()
        self.completed_leases: list[ObjectDeletionLease] = []
        self.failed_leases: list[tuple[ObjectDeletionLease, ErrorDetail]] = []
        self.calls: list[str] = []

    async def require_active(self, tenant_id: uuid.UUID) -> None:
        assert tenant_id == TENANT_ID
        if self.lifecycle is not None and self.lifecycle.status is not TenantLifecycleStatus.ACTIVE:
            raise DomainOperationError(
                code="tenant_not_active",
                message="tenant is not active",
            )

    async def get_tenant_lifecycle(self, tenant_id: uuid.UUID) -> TenantLifecycle | None:
        assert tenant_id == TENANT_ID
        return self.lifecycle

    async def get_audit_export(
        self,
        tenant_id: uuid.UUID,
        export_id: uuid.UUID,
    ) -> AuditExport | None:
        export = self.exports.get(export_id)
        return export if export is not None and export.tenant_id == tenant_id else None

    async def get_legal_hold(
        self,
        tenant_id: uuid.UUID,
        hold_id: uuid.UUID,
    ) -> LegalHold | None:
        del tenant_id, hold_id
        return None

    async def has_active_legal_hold(
        self,
        tenant_id: uuid.UUID,
        *,
        occurred_at: AwareTimestamp,
    ) -> bool:
        del occurred_at
        assert tenant_id == TENANT_ID
        return False

    async def iter_audit_entries(
        self,
        tenant_id: uuid.UUID,
        *,
        cutoff_at: AwareTimestamp,
        batch_size: int,
    ) -> AsyncIterator[tuple[AuditExportEntry, ...]]:
        assert tenant_id == TENANT_ID
        assert cutoff_at == NOW
        for start in range(0, len(self.entries), batch_size):
            yield self.entries[start : start + batch_size]

    async def create_audit_export(self, export: AuditExport) -> AuditExport:
        current = self.exports.setdefault(export.id, export)
        if current.tenant_id != export.tenant_id or current.cutoff_at != export.cutoff_at:
            raise DomainOperationError(
                code="audit_export_conflict",
                message="audit export conflicts",
            )
        return current

    async def complete_audit_export(self, export: AuditExport) -> AuditExport:
        current = self.exports[export.id]
        if current.status is AuditExportStatus.COMPLETED and current != export:
            raise DomainOperationError(
                code="audit_export_conflict",
                message="audit export outcome conflicts",
            )
        self.exports[export.id] = export
        return export

    async def fail_audit_export(
        self,
        export_id: uuid.UUID,
        tenant_id: uuid.UUID,
        *,
        error: ErrorDetail,
    ) -> AuditExport:
        current = self.exports[export_id]
        assert current.tenant_id == tenant_id
        failed = current.model_copy(update={"status": AuditExportStatus.FAILED, "error": error})
        self.exports[export_id] = failed
        self.failed_exports.append(error)
        return failed

    async def place_legal_hold(self, hold: LegalHold) -> LegalHold:
        return hold

    async def release_legal_hold(
        self,
        tenant_id: uuid.UUID,
        hold_id: uuid.UUID,
        *,
        released_by: str,
        released_at: AwareTimestamp,
    ) -> LegalHold:
        del tenant_id, hold_id, released_by, released_at
        raise NotImplementedError

    async def request_tenant_deletion(
        self,
        tenant_id: uuid.UUID,
        *,
        request_id: uuid.UUID,
        requested_by: str,
        requested_at: AwareTimestamp,
        delete_after: AwareTimestamp,
        audit_export_id: uuid.UUID,
    ) -> TenantLifecycle:
        self.calls.append("request")
        self.lifecycle = TenantLifecycle(
            tenant_id=tenant_id,
            status=TenantLifecycleStatus.DELETION_REQUESTED,
            request_id=request_id,
            requested_by=requested_by,
            requested_at=requested_at,
            delete_after=delete_after,
            audit_export_id=audit_export_id,
            updated_at=requested_at,
        )
        return self.lifecycle

    async def begin_tenant_deletion(
        self,
        tenant_id: uuid.UUID,
        *,
        occurred_at: AwareTimestamp,
    ) -> TenantLifecycle:
        self.calls.append("begin")
        assert self.lifecycle is not None
        self.lifecycle = self.lifecycle.model_copy(
            update={
                "status": TenantLifecycleStatus.DELETING,
                "deletion_started_at": occurred_at,
                "updated_at": occurred_at,
            }
        )
        assert self.lifecycle.tenant_id == tenant_id
        return self.lifecycle

    async def enqueue_expired_artifacts(
        self,
        *,
        occurred_at: AwareTimestamp,
        limit: int,
    ) -> int:
        del occurred_at, limit
        self.calls.append("retention")
        return 0

    async def enqueue_tenant_objects(self, tenant_id: uuid.UUID, *, limit: int) -> int:
        del limit
        assert tenant_id == TENANT_ID
        self.calls.append("enqueue")
        return 2

    async def finalize_tenant_deletion(
        self,
        tenant_id: uuid.UUID,
        *,
        occurred_at: AwareTimestamp,
    ) -> TenantLifecycle:
        self.calls.append("finalize")
        assert self.lifecycle is not None and self.lifecycle.deletion_started_at is not None
        self.lifecycle = self.lifecycle.model_copy(
            update={
                "status": TenantLifecycleStatus.DELETED,
                "deleted_at": occurred_at,
                "updated_at": occurred_at,
            }
        )
        assert self.lifecycle.tenant_id == tenant_id
        return self.lifecycle

    async def claim_object_deletions(
        self,
        worker_id: str,
        *,
        occurred_at: AwareTimestamp,
        lease_seconds: int,
        limit: int,
    ) -> tuple[ObjectDeletionLease, ...]:
        del occurred_at, lease_seconds
        assert worker_id == "cleanup-1"
        return self.leases[:limit]

    async def complete_object_deletion(self, lease: ObjectDeletionLease) -> None:
        self.completed_leases.append(lease)

    async def fail_object_deletion(
        self,
        lease: ObjectDeletionLease,
        *,
        error: ErrorDetail,
        occurred_at: AwareTimestamp,
    ) -> None:
        del occurred_at
        self.failed_leases.append((lease, error))


def _stored(object_key: str, content: bytes) -> StoredObject:
    return StoredObject(
        object_key=object_key,
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        content_type="application/x-ndjson",
    )


def _lease(index: int = 1) -> ObjectDeletionLease:
    return ObjectDeletionLease(
        job_id=uuid.UUID(int=100 + index),
        tenant_id=TENANT_ID,
        object_key=f"tenants/{TENANT_ID.hex}/artifact-{index}",
        reason=ObjectDeletionReason.RETENTION_EXPIRED,
        worker_id="cleanup-1",
        lease_token=uuid.UUID(int=200 + index),
        lease_generation=1,
        attempt=1,
        expires_at=NOW + timedelta(minutes=1),
    )


def test_lifecycle_models_are_closed_and_revalidate_copies() -> None:
    lifecycle = TenantLifecycle(
        tenant_id=TENANT_ID,
        status=TenantLifecycleStatus.ACTIVE,
        updated_at=NOW,
    )
    with pytest.raises(ValidationError, match="deletion state"):
        lifecycle.model_copy(update={"request_id": REQUEST_ID})
    with pytest.raises(ValidationError, match="start and completion"):
        TenantLifecycle(
            tenant_id=TENANT_ID,
            status=TenantLifecycleStatus.DELETED,
            request_id=REQUEST_ID,
            requested_by="operator",
            requested_at=NOW,
            delete_after=NOW,
            audit_export_id=EXPORT_ID,
            updated_at=NOW,
        )
    with pytest.raises(ValidationError, match="extra"):
        ObjectDeletionLease.model_validate({**_lease().model_dump(), "extra": True})


def test_legal_hold_and_audit_entry_validate_lifecycle_and_size() -> None:
    with pytest.raises(ValidationError, match="expiry"):
        LegalHold(
            id=uuid.uuid4(),
            tenant_id=TENANT_ID,
            reason="investigation",
            placed_by="operator",
            placed_at=NOW,
            expires_at=NOW,
        )
    with pytest.raises(ValidationError, match="1000000-byte"):
        AuditEntry(
            id=uuid.uuid4(),
            tenant_id=TENANT_ID,
            subject="operator",
            method="POST",
            resource="/admin/test",
            action="test.action",
            request_id="request",
            details={"value": "x" * MAX_AUDIT_DETAILS_BYTES},
            occurred_at=NOW,
        )


@pytest.mark.asyncio
async def test_audit_export_is_canonical_immutable_and_verifiable(tmp_path: Path) -> None:
    repository = _Repository((_entry(0), _entry(1)))
    objects = _ObjectStore()
    service = AuditExportService(
        cast("LifecycleAdministrationRepository", repository),
        objects,
        temporary_parent=tmp_path,
        clock=lambda: NOW,
    )

    export = await service.export(
        TENANT_ID,
        export_id=EXPORT_ID,
        requested_by="operator",
        cutoff_at=NOW,
    )
    await service.verify(export)

    assert export.status is AuditExportStatus.COMPLETED
    assert export.record_count == 2
    assert export.object is not None
    assert export.object.object_key == tenant_audit_export_object_key(TENANT_ID, EXPORT_ID)
    content = objects.objects[export.object.object_key]
    assert content.count(b"\n") == 4
    assert b'"type":"agent-audit-export-v1"' in content
    assert b'"type":"audit-trailer"' in content
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []

    repeated = await service.export(
        TENANT_ID,
        export_id=EXPORT_ID,
        requested_by="operator",
        cutoff_at=NOW,
    )
    assert repeated == export


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_entries", [(_entry(1), _entry(0)), (_entry(0), _entry(0))])
async def test_audit_export_rejects_non_total_order(
    tmp_path: Path,
    invalid_entries: tuple[AuditExportEntry, ...],
) -> None:
    repository = _Repository(invalid_entries)
    service = AuditExportService(
        cast("LifecycleAdministrationRepository", repository),
        _ObjectStore(),
        temporary_parent=tmp_path,
        clock=lambda: NOW,
    )

    with pytest.raises(DomainOperationError, match="totally ordered") as captured:
        await service.export(
            TENANT_ID,
            export_id=EXPORT_ID,
            requested_by="operator",
            cutoff_at=NOW,
        )

    assert captured.value.code == "audit_export_protocol_error"
    assert repository.failed_exports[0].code == "audit_export_protocol_error"
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
async def test_audit_export_enforces_record_and_byte_limits(tmp_path: Path) -> None:
    for config, code in (
        (LifecycleServiceConfig(audit_max_records=1), "audit_export_record_limit"),
        (LifecycleServiceConfig(audit_max_bytes=64), "audit_export_byte_limit"),
    ):
        repository = _Repository((_entry(0), _entry(1)))
        service = AuditExportService(
            cast("LifecycleAdministrationRepository", repository),
            _ObjectStore(),
            config=config,
            temporary_parent=tmp_path,
            clock=lambda: NOW,
        )
        with pytest.raises(DomainOperationError) as captured:
            await service.export(
                TENANT_ID,
                export_id=uuid.uuid4(),
                requested_by="operator",
                cutoff_at=NOW,
            )
        assert captured.value.code == code


@pytest.mark.asyncio
async def test_audit_export_verification_rejects_tampering(tmp_path: Path) -> None:
    repository = _Repository((_entry(0),))
    objects = _ObjectStore()
    service = AuditExportService(
        cast("LifecycleAdministrationRepository", repository),
        objects,
        temporary_parent=tmp_path,
        clock=lambda: NOW,
    )
    export = await service.export(
        TENANT_ID,
        export_id=EXPORT_ID,
        requested_by="operator",
        cutoff_at=NOW,
    )
    assert export.object is not None
    objects.objects[export.object.object_key] += b"{}\n"

    with pytest.raises(AssertionError):
        await service.verify(export)


def test_audit_export_verifier_rejects_an_overlong_line_boundedly(tmp_path: Path) -> None:
    payload = b"x" * (MAX_AUDIT_ENTRY_BYTES + 1_026)
    path = tmp_path / "oversized.jsonl"
    path.write_bytes(payload)
    export = AuditExport(
        id=EXPORT_ID,
        tenant_id=TENANT_ID,
        status=AuditExportStatus.COMPLETED,
        requested_by="operator",
        cutoff_at=NOW,
        created_at=NOW,
        object=StoredObject(
            object_key=tenant_audit_export_object_key(TENANT_ID, EXPORT_ID),
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            content_type="application/x-ndjson",
        ),
        record_count=0,
        completed_at=NOW,
    )

    with pytest.raises(DomainOperationError) as captured:
        verify_audit_export_file(path, export)

    assert captured.value.code == "audit_export_invalid"


@pytest.mark.asyncio
async def test_tenant_deletion_is_export_first_and_cooling_off_is_explicit(
    tmp_path: Path,
) -> None:
    repository = _Repository((_entry(0),))
    audit = AuditExportService(
        cast("LifecycleAdministrationRepository", repository),
        _ObjectStore(),
        temporary_parent=tmp_path,
        clock=lambda: NOW,
    )
    service = TenantDeletionService(
        cast("LifecycleAdministrationRepository", repository),
        audit,
        config=LifecycleServiceConfig(deletion_cooling_off_seconds=0),
        clock=lambda: NOW,
    )

    lifecycle, export = await service.request(
        TENANT_ID,
        request_id=REQUEST_ID,
        export_id=EXPORT_ID,
        requested_by="operator",
    )
    repeated_lifecycle, repeated_export = await service.request(
        TENANT_ID,
        request_id=REQUEST_ID,
        export_id=EXPORT_ID,
        requested_by="operator",
    )
    prepared, enqueued = await service.prepare(TENANT_ID)
    finalized = await service.finalize(TENANT_ID)

    assert export.status is AuditExportStatus.COMPLETED
    assert lifecycle.status is TenantLifecycleStatus.DELETION_REQUESTED
    assert repeated_lifecycle == lifecycle
    assert repeated_export == export
    assert prepared.status is TenantLifecycleStatus.DELETING
    assert enqueued == 2
    assert finalized.status is TenantLifecycleStatus.DELETED
    assert repository.calls == ["request", "begin", "enqueue", "finalize"]


@pytest.mark.asyncio
async def test_object_deletion_worker_records_success_and_structured_failure() -> None:
    repository = _Repository()
    objects = _ObjectStore()
    first = _lease(1)
    repository.leases = (first,)
    worker = ObjectDeletionWorker(
        cast("LifecycleAdministrationRepository", repository),
        objects,
        worker_id="cleanup-1",
        clock=lambda: NOW,
    )

    completed = await worker.run_batch()
    assert completed.model_dump() == {"claimed": 1, "completed": 1, "failed": 0}
    assert objects.deleted == [first.object_key]
    assert repository.completed_leases == [first]

    second = _lease(2)
    repository.leases = (second,)
    objects.delete_error = RuntimeError("storage-secret-must-not-escape")
    failed = await worker.run_batch()
    assert failed.model_dump() == {"claimed": 1, "completed": 0, "failed": 1}
    assert repository.failed_leases[-1][1] == ErrorDetail(
        code="object_deletion_failed",
        message="object storage deletion failed",
        retryable=True,
    )


def test_lifecycle_configuration_and_object_key_are_bounded() -> None:
    with pytest.raises(ValidationError):
        LifecycleServiceConfig(deletion_batch_size=0)
    with pytest.raises(ValueError, match="worker_id"):
        ObjectDeletionWorker(
            cast("LifecycleAdministrationRepository", _Repository()),
            _ObjectStore(),
            worker_id=" ",
        )
    key = tenant_audit_export_object_key(TENANT_ID, EXPORT_ID)
    assert key == f"compliance/tenants/{TENANT_ID.hex}/audit/{EXPORT_ID.hex}.jsonl"
