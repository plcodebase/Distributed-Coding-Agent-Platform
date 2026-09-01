from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest
from pydantic import ValidationError
from scripts import recovery_verification as recovery

from agent_core.artifacts import StoredObject
from agent_core.domain.errors import DomainOperationError
from agent_core.lifecycle import AuditExport, AuditExportStatus
from platform_persistence import Base
from platform_persistence.models import AuditExportRecord

if TYPE_CHECKING:
    from pathlib import Path

    from agent_core.artifacts import ObjectKey
    from agent_core.domain.models import Sha256Hex

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000401")
EXPORT_ID = uuid.UUID("00000000-0000-0000-0000-000000000402")
REVISION = "a" * 40


def _table_counts() -> tuple[recovery.RestoredTableCount, ...]:
    return tuple(
        recovery.RestoredTableCount(name=name, rows=0) for name in sorted(Base.metadata.tables)
    )


def _report() -> recovery.RecoveryVerificationReport:
    return recovery.RecoveryVerificationReport(
        verification_id=uuid.uuid4(),
        backup_id="backup-2026-08-21",
        source_revision=REVISION,
        migration_head="0017",
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=1),
        table_counts=_table_counts(),
        run_count=0,
        event_count=0,
        object_count=0,
        object_bytes=0,
        database_fingerprint_sha256="0" * 64,
        object_evidence_sha256=hashlib.sha256(b"").hexdigest(),
    )


def _empty_audit_bytes() -> bytes:
    lines = (
        {
            "type": "agent-audit-export-v1",
            "export_id": str(EXPORT_ID),
            "tenant_id": str(TENANT_ID),
            "cutoff_at": "2026-08-21T12:00:00Z",
        },
        {
            "type": "audit-trailer",
            "record_count": 0,
            "first_occurred_at": None,
            "last_occurred_at": None,
        },
    )
    return b"".join(
        json.dumps(
            line,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
        for line in lines
    )


def _audit_export(content: bytes) -> AuditExport:
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


class _ObjectStore:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.downloaded: list[str] = []

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
        self.downloaded.append(object_key)
        return StoredObject(
            object_key=object_key,
            sha256=expected_sha256,
            size_bytes=len(content),
            content_type="application/octet-stream",
        )


class _Rows:
    def __init__(self, rows: tuple[Any, ...]) -> None:
        self.rows = rows

    def all(self) -> list[Any]:
        return list(self.rows)


class _Session:
    def __init__(
        self,
        *,
        execute_rows: tuple[tuple[Any, ...], ...] = (),
        scalar_rows: tuple[tuple[Any, ...], ...] = (),
        scalar_values: tuple[int, ...] = (),
    ) -> None:
        self.execute_rows = list(execute_rows)
        self.scalar_rows = list(scalar_rows)
        self.scalar_values = list(scalar_values)

    async def execute(self, _statement: object) -> _Rows:
        return _Rows(self.execute_rows.pop(0))

    async def scalars(self, _statement: object) -> _Rows:
        return _Rows(self.scalar_rows.pop(0))

    async def scalar(self, _statement: object) -> int:
        return self.scalar_values.pop(0)


def test_recovery_models_are_closed_immutable_and_complete() -> None:
    report = _report()
    assert report.result == "passed"
    with pytest.raises(ValidationError):
        report.model_copy(update={"result": "failed"})
    with pytest.raises(ValidationError, match="every database table"):
        report.model_copy(update={"table_counts": ()})
    with pytest.raises(ValidationError):
        recovery.RecoveryVerificationConfig(max_object_bytes=0)
    with pytest.raises(ValidationError, match="backup ID"):
        report.model_copy(update={"backup_id": "bad\x00id"})


def test_reference_accumulation_deduplicates_and_fails_closed() -> None:
    config = recovery.RecoveryVerificationConfig(
        max_object_references=1,
        max_object_bytes=10,
        max_total_object_bytes=10,
    )
    first = recovery._ObjectReference(
        object_key="tenants/one/object",
        sha256="0" * 64,
        size_bytes=5,
    )
    values: dict[str, recovery._ObjectReference] = {}
    recovery._add_reference(values, first, config)
    recovery._add_reference(values, first, config)
    assert values == {first.object_key: first}

    with pytest.raises(DomainOperationError, match="conflicting"):
        recovery._add_reference(
            values,
            recovery._ObjectReference(
                object_key=first.object_key,
                sha256="1" * 64,
                size_bytes=5,
            ),
            config,
        )
    with pytest.raises(DomainOperationError, match="too many"):
        recovery._add_reference(
            values,
            recovery._ObjectReference(
                object_key="tenants/one/other",
                sha256="2" * 64,
                size_bytes=1,
            ),
            config,
        )


@pytest.mark.asyncio
async def test_object_and_audit_export_content_is_fully_verified(tmp_path: Path) -> None:
    content = _empty_audit_bytes()
    export = _audit_export(content)
    assert export.object is not None
    reference = recovery._ObjectReference(
        object_key=export.object.object_key,
        sha256=export.object.sha256,
        size_bytes=export.object.size_bytes,
        audit_export=export,
    )
    store = _ObjectStore({reference.object_key: content})

    digest = await recovery._verify_objects(
        cast("Any", store),
        (reference,),
        directory=tmp_path,
        maximum_object_bytes=len(content),
    )

    expected = hashlib.sha256(
        f"{reference.object_key}\0{reference.sha256}\0{reference.size_bytes}\n".encode()
    ).hexdigest()
    assert digest == expected
    assert store.downloaded == [reference.object_key]
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        ((uuid.uuid4(), 1, 3, 2, 1, 3, 1),),
        ((uuid.uuid4(), 1, 2, 1, 2, 2, 1),),
        ((uuid.uuid4(), 1, 2, 1, 1, 1, 2),),
    ],
)
async def test_event_sequence_verification_rejects_gaps_and_future_epochs(
    rows: tuple[tuple[Any, ...], ...],
) -> None:
    session = _Session(execute_rows=(rows,))
    with pytest.raises(DomainOperationError):
        await recovery._verify_event_sequences(cast("Any", session))


@pytest.mark.asyncio
async def test_event_sequence_verification_accepts_empty_and_contiguous_runs() -> None:
    rows = (
        (uuid.uuid4(), 1, 1, 0, 0, 0, 0),
        (uuid.uuid4(), 2, 3, 2, 1, 2, 2),
    )
    assert await recovery._verify_event_sequences(cast("Any", _Session(execute_rows=(rows,)))) == (
        2,
        2,
    )


@pytest.mark.asyncio
async def test_deleted_tenant_residue_is_rejected() -> None:
    tenant_tables = [
        table
        for table in Base.metadata.sorted_tables
        if "tenant_id" in table.c and table.name not in recovery.RETAINED_DELETED_TENANT_TABLES
    ]
    clean = _Session(scalar_values=(0,) * len(tenant_tables))
    await recovery._verify_deleted_tenant_residue(cast("Any", clean))
    dirty = _Session(scalar_values=(1,))
    with pytest.raises(DomainOperationError, match="application data remains"):
        await recovery._verify_deleted_tenant_residue(cast("Any", dirty))


@pytest.mark.asyncio
async def test_database_object_reference_normalization_and_limits() -> None:
    content = _empty_audit_bytes()
    export = _audit_export(content)
    assert export.object is not None
    row = AuditExportRecord(
        id=export.id,
        tenant_id=export.tenant_id,
        status=export.status.value,
        requested_by=export.requested_by,
        cutoff_at=export.cutoff_at,
        object_key=export.object.object_key,
        sha256=export.object.sha256,
        size_bytes=export.object.size_bytes,
        content_type=export.object.content_type,
        record_count=0,
        created_at=NOW,
        completed_at=NOW,
    )
    session = _Session(
        execute_rows=(
            ((export.object.object_key, export.object.sha256, export.object.size_bytes),),
            (),
        ),
        scalar_rows=((), (row,)),
    )

    references = await recovery._read_object_references(
        cast("Any", session),
        recovery.RecoveryVerificationConfig(max_total_object_bytes=len(content)),
    )
    assert len(references) == 1
    assert references[0].audit_export == export

    oversized = _Session(
        execute_rows=((("tenants/one/large", "0" * 64, 11),), ()),
        scalar_rows=((), ()),
    )
    with pytest.raises(DomainOperationError, match="verification limit"):
        await recovery._read_object_references(
            cast("Any", oversized),
            recovery.RecoveryVerificationConfig(max_object_bytes=10),
        )


def test_report_writer_is_private_atomic_and_non_overwriting(tmp_path: Path) -> None:
    output = tmp_path / "recovery.json"
    recovery.write_report(output, _report())
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "agent-recovery-verification-v1"
    assert stat_mode(output) == 0o600
    with pytest.raises(FileExistsError):
        recovery.write_report(output, _report())


def test_fingerprint_parser_and_repository_head_are_deterministic(tmp_path: Path) -> None:
    reference = recovery._ObjectReference(
        object_key="tenants/one/object",
        sha256="0" * 64,
        size_bytes=1,
    )
    first = recovery._database_fingerprint(
        migration_head="0017",
        table_counts={"runs": 1},
        run_count=1,
        event_count=2,
        references=(reference,),
    )
    second = recovery._database_fingerprint(
        migration_head="0017",
        table_counts={"runs": 1},
        run_count=1,
        event_count=2,
        references=(reference,),
    )
    assert first == second
    assert recovery._repository_migration_head() == "0017"
    parsed = recovery.build_parser().parse_args(
        [
            "--backup-id",
            "backup-1",
            "--source-revision",
            REVISION,
            "--output",
            str(tmp_path / "report.json"),
        ]
    )
    assert parsed.backup_id == "backup-1"


def test_main_never_exposes_unexpected_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
    tmp_path: Path,
) -> None:
    async def unexpected(_arguments: object) -> recovery.RecoveryVerificationReport:
        raise RuntimeError("database-secret-must-not-escape")

    monkeypatch.setattr(recovery, "run", unexpected)
    result = recovery.main(
        [
            "--backup-id",
            "backup-1",
            "--source-revision",
            REVISION,
            "--output",
            str(tmp_path / "report.json"),
        ]
    )
    captured = capsys.readouterr()
    assert result == 1
    assert "recovery_verification_failed" in captured.err
    assert "database-secret" not in captured.err


def stat_mode(path: Path) -> int:
    return path.stat().st_mode & 0o777
