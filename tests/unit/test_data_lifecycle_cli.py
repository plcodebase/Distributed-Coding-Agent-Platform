from __future__ import annotations

import hashlib
import uuid
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest
from scripts import data_lifecycle

from agent_core.artifacts import StoredObject
from agent_core.domain.errors import DomainOperationError
from agent_core.lifecycle import (
    AuditExport,
    AuditExportStatus,
    LegalHold,
    ObjectDeletionLease,
    ObjectDeletionReason,
    TenantLifecycle,
    TenantLifecycleStatus,
)
from agent_core.lifecycle_service import LifecycleServiceConfig

if TYPE_CHECKING:
    from agent_core.artifacts import ObjectKey
    from agent_core.audit import AuditEntry
    from agent_core.domain.base import JsonObject

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000301")
EXPORT_ID = uuid.UUID("00000000-0000-0000-0000-000000000302")
REQUEST_ID = uuid.UUID("00000000-0000-0000-0000-000000000303")
HOLD_ID = uuid.UUID("00000000-0000-0000-0000-000000000304")


def _completed_export() -> AuditExport:
    content = b"evidence"
    return AuditExport(
        id=EXPORT_ID,
        tenant_id=TENANT_ID,
        status=AuditExportStatus.COMPLETED,
        requested_by="operator",
        cutoff_at=NOW,
        created_at=NOW,
        object=StoredObject(
            object_key=f"compliance/tenants/{TENANT_ID.hex}/audit/{EXPORT_ID.hex}.jsonl",
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
            content_type="application/x-ndjson",
        ),
        record_count=0,
        completed_at=NOW,
    )


def _lifecycle(status: TenantLifecycleStatus) -> TenantLifecycle:
    if status is TenantLifecycleStatus.ACTIVE:
        return TenantLifecycle(tenant_id=TENANT_ID, status=status, updated_at=NOW)
    common: dict[str, Any] = {
        "tenant_id": TENANT_ID,
        "status": status,
        "request_id": REQUEST_ID,
        "requested_by": "operator",
        "requested_at": NOW,
        "delete_after": NOW,
        "audit_export_id": EXPORT_ID,
        "updated_at": NOW,
    }
    if status in {TenantLifecycleStatus.DELETING, TenantLifecycleStatus.DELETED}:
        common["deletion_started_at"] = NOW
    if status is TenantLifecycleStatus.DELETED:
        common["deleted_at"] = NOW
    return TenantLifecycle.model_validate(common)


class _AuditSink:
    def __init__(self) -> None:
        self.entries: list[AuditEntry] = []

    async def append(self, entry: AuditEntry) -> AuditEntry:
        self.entries.append(entry)
        return entry


class _Repository:
    def __init__(self) -> None:
        self.lifecycle: TenantLifecycle | None = _lifecycle(TenantLifecycleStatus.ACTIVE)
        self.export: AuditExport | None = _completed_export()
        self.hold: LegalHold | None = None
        self.enqueued = 3
        self.completed: list[ObjectDeletionLease] = []

    async def get_tenant_lifecycle(self, tenant_id: uuid.UUID) -> TenantLifecycle | None:
        assert tenant_id == TENANT_ID
        return self.lifecycle

    async def get_audit_export(
        self, tenant_id: uuid.UUID, export_id: uuid.UUID
    ) -> AuditExport | None:
        assert tenant_id == TENANT_ID and export_id == EXPORT_ID
        return self.export

    async def get_legal_hold(
        self,
        tenant_id: uuid.UUID,
        hold_id: uuid.UUID,
    ) -> LegalHold | None:
        assert tenant_id == TENANT_ID and hold_id == HOLD_ID
        return self.hold

    async def place_legal_hold(self, hold: LegalHold) -> LegalHold:
        self.hold = hold
        return hold

    async def release_legal_hold(
        self,
        tenant_id: uuid.UUID,
        hold_id: uuid.UUID,
        *,
        released_by: str,
        released_at: datetime,
    ) -> LegalHold:
        assert tenant_id == TENANT_ID and hold_id == HOLD_ID
        assert self.hold is not None
        self.hold = self.hold.model_copy(
            update={"released_by": released_by, "released_at": released_at}
        )
        return self.hold

    async def enqueue_expired_artifacts(self, *, occurred_at: datetime, limit: int) -> int:
        assert occurred_at.tzinfo is not None and limit == 100
        return self.enqueued

    async def claim_object_deletions(
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        lease_seconds: int,
        limit: int,
    ) -> tuple[ObjectDeletionLease, ...]:
        assert worker_id == "cleanup-1" and lease_seconds == 60 and limit == 100
        return (
            ObjectDeletionLease(
                job_id=uuid.uuid4(),
                tenant_id=TENANT_ID,
                object_key=f"tenants/{TENANT_ID.hex}/expired",
                reason=ObjectDeletionReason.RETENTION_EXPIRED,
                worker_id=worker_id,
                lease_token=uuid.uuid4(),
                lease_generation=1,
                attempt=1,
                expires_at=occurred_at + timedelta(seconds=lease_seconds),
            ),
        )

    async def complete_object_deletion(self, lease: ObjectDeletionLease) -> None:
        self.completed.append(lease)

    async def fail_object_deletion(
        self,
        lease: ObjectDeletionLease,
        *,
        error: object,
        occurred_at: datetime,
    ) -> None:
        del lease, error, occurred_at
        raise AssertionError("delete should succeed")


class _Exports:
    def __init__(self, export: AuditExport) -> None:
        self.export_value = export
        self.verified: list[AuditExport] = []

    async def export(
        self,
        tenant_id: uuid.UUID,
        *,
        export_id: uuid.UUID,
        requested_by: str,
    ) -> AuditExport:
        assert (tenant_id, export_id, requested_by) == (TENANT_ID, EXPORT_ID, "operator")
        return self.export_value

    async def verify(self, export: AuditExport) -> None:
        self.verified.append(export)


class _Deletion:
    async def request(
        self,
        tenant_id: uuid.UUID,
        *,
        request_id: uuid.UUID,
        export_id: uuid.UUID,
        requested_by: str,
    ) -> tuple[TenantLifecycle, AuditExport]:
        assert (tenant_id, request_id, export_id, requested_by) == (
            TENANT_ID,
            REQUEST_ID,
            EXPORT_ID,
            "operator",
        )
        return _lifecycle(TenantLifecycleStatus.DELETION_REQUESTED), _completed_export()

    async def prepare(self, tenant_id: uuid.UUID) -> tuple[TenantLifecycle, int]:
        assert tenant_id == TENANT_ID
        return _lifecycle(TenantLifecycleStatus.DELETING), 4

    async def finalize(self, tenant_id: uuid.UUID) -> TenantLifecycle:
        assert tenant_id == TENANT_ID
        return _lifecycle(TenantLifecycleStatus.DELETED)


class _ObjectStore:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def delete(self, object_key: ObjectKey) -> None:
        self.deleted.append(object_key)


def _arguments(command: str, **updates: object) -> Namespace:
    values: dict[str, object] = {
        "command": command,
        "tenant_id": TENANT_ID,
        "confirm_tenant_id": TENANT_ID,
        "actor": "operator",
        "export_id": EXPORT_ID,
        "request_id": REQUEST_ID,
        "hold_id": HOLD_ID,
        "reason": "investigation",
        "expires_at": None,
        "worker_id": "cleanup-1",
    }
    values.update(updates)
    return Namespace(**values)


async def _execute(  # noqa: PLR0917 - compact test composition helper
    command: str,
    repository: _Repository,
    audit: _AuditSink,
    exports: _Exports,
    deletion: _Deletion,
    objects: _ObjectStore,
    **updates: object,
) -> JsonObject:
    return await data_lifecycle._execute(
        _arguments(command, **updates),
        repository=cast("Any", repository),
        audit_sink=cast("Any", audit),
        exports=cast("Any", exports),
        deletion=cast("Any", deletion),
        object_store=cast("Any", objects),
        config=LifecycleServiceConfig(),
    )


def _json_object(value: object) -> dict[str, Any]:
    assert isinstance(value, dict)
    return cast("dict[str, Any]", value)


@pytest.mark.asyncio
async def test_status_export_and_verify_commands() -> None:
    repository = _Repository()
    audit = _AuditSink()
    exports = _Exports(_completed_export())
    deletion = _Deletion()
    objects = _ObjectStore()

    status = await _execute("status", repository, audit, exports, deletion, objects)
    exported = await _execute("export-audit", repository, audit, exports, deletion, objects)
    verified = await _execute("verify-audit", repository, audit, exports, deletion, objects)

    assert _json_object(status["lifecycle"])["status"] == "active"
    assert _json_object(exported["audit_export"])["status"] == "completed"
    assert verified["verified"] is True
    assert len(exports.verified) == 2
    assert [entry.action for entry in audit.entries] == [
        "audit.export_requested",
        "audit.export_verification_requested",
    ]


@pytest.mark.asyncio
async def test_hold_commands_are_audited_and_typed() -> None:
    repository = _Repository()
    repository.hold = LegalHold(
        id=HOLD_ID,
        tenant_id=TENANT_ID,
        reason="investigation",
        placed_by="operator",
        placed_at=NOW - timedelta(days=1),
    )
    audit = _AuditSink()
    exports = _Exports(_completed_export())
    deletion = _Deletion()
    objects = _ObjectStore()

    placed = await _execute("place-hold", repository, audit, exports, deletion, objects)
    released = await _execute("release-hold", repository, audit, exports, deletion, objects)

    assert _json_object(placed["legal_hold"])["id"] == str(HOLD_ID)
    assert _json_object(released["legal_hold"])["released_by"] == "operator"
    assert [entry.action for entry in audit.entries] == [
        "legal_hold.placed",
        "legal_hold.released",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "expected_status"),
    [
        ("request-deletion", "deletion_requested"),
        ("prepare-deletion", "deleting"),
        ("finalize-deletion", "deleted"),
    ],
)
async def test_deletion_transitions_require_exact_confirmation(
    command: str,
    expected_status: str,
) -> None:
    repository = _Repository()
    audit = _AuditSink()
    exports = _Exports(_completed_export())
    deletion = _Deletion()
    objects = _ObjectStore()

    result = await _execute(command, repository, audit, exports, deletion, objects)
    assert _json_object(result["lifecycle"])["status"] == expected_status

    with pytest.raises(DomainOperationError) as mismatch:
        await _execute(
            command,
            repository,
            audit,
            exports,
            deletion,
            objects,
            confirm_tenant_id=uuid.uuid4(),
        )
    assert mismatch.value.code == "tenant_confirmation_mismatch"


@pytest.mark.asyncio
async def test_retention_and_cleanup_commands_are_bounded() -> None:
    repository = _Repository()
    audit = _AuditSink()
    exports = _Exports(_completed_export())
    deletion = _Deletion()
    objects = _ObjectStore()

    retention = await _execute("retention-scan", repository, audit, exports, deletion, objects)
    cleanup = await _execute("cleanup-objects", repository, audit, exports, deletion, objects)

    assert retention == {"command": "retention-scan", "enqueued": 3}
    assert cleanup == {
        "command": "cleanup-objects",
        "result": {"claimed": 1, "completed": 1, "failed": 0},
    }
    assert len(repository.completed) == 1
    assert objects.deleted == [repository.completed[0].object_key]
    assert [entry.action for entry in audit.entries] == [
        "retention.scan_requested",
        "object_cleanup.batch_requested",
    ]
    assert all(
        entry.tenant_id == data_lifecycle.PLATFORM_AUDIT_TENANT_ID for entry in audit.entries
    )


def test_parser_rejects_ambiguous_or_unconfirmed_operations() -> None:
    parser = data_lifecycle.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["request-deletion", "--tenant-id", "invalid"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "retention-scan",
                "--confirm",
                "wrong",
            ]
        )
    parsed = parser.parse_args(
        [
            "request-deletion",
            "--tenant-id",
            str(TENANT_ID),
            "--confirm-tenant-id",
            str(TENANT_ID),
            "--actor",
            "operator",
            "--request-id",
            str(REQUEST_ID),
            "--export-id",
            str(EXPORT_ID),
        ]
    )
    assert parsed.tenant_id == TENANT_ID


def test_main_emits_structured_opaque_errors(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    async def denied(_arguments: Namespace) -> dict[str, object]:
        raise DomainOperationError(
            code="tenant_confirmation_mismatch",
            message="the exact tenant confirmation did not match",
        )

    monkeypatch.setattr(data_lifecycle, "run", denied)
    result = data_lifecycle.main(
        [
            "status",
            "--tenant-id",
            str(TENANT_ID),
        ]
    )
    captured = capsys.readouterr()
    assert result == 2
    assert "tenant_confirmation_mismatch" in captured.err

    async def unexpected(_arguments: Namespace) -> dict[str, object]:
        raise RuntimeError("database-secret-must-not-escape")

    monkeypatch.setattr(data_lifecycle, "run", unexpected)
    assert data_lifecycle.main(["status", "--tenant-id", str(TENANT_ID)]) == 1
    assert "database-secret" not in capsys.readouterr().err
