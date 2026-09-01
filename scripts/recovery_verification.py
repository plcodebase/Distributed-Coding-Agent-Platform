"""Read-only verification of an isolated PostgreSQL and object-store restore."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import stat
import sys
import tempfile
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, Never, Self, cast

from alembic.config import Config
from alembic.script import ScriptDirectory
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator
from sqlalchemy import func, select, text

from agent_core.artifacts import DurableSnapshotReference, StoredObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.lifecycle import AuditExport, AuditExportStatus
from agent_core.lifecycle_service import verify_audit_export_file
from artifact_store import S3ObjectStoreSettings, create_s3_object_store
from platform_persistence import Base, Database, DatabaseSettings
from platform_persistence.models import (
    AgentEventRecord,
    ArtifactRecord,
    AuditExportRecord,
    CheckpointRecord,
    RunRecord,
    SourceSnapshotRecord,
    TenantLifecycleRecord,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import TextIO

    from sqlalchemy.ext.asyncio import AsyncSession

    from agent_core.artifacts import ObjectKey, ObjectStore
    from agent_core.domain.models import Sha256Hex

MAX_OBJECT_REFERENCES = 100_000
MAX_OBJECT_BYTES = 1024 * 1024 * 1024
MAX_TOTAL_OBJECT_BYTES = 100 * 1024 * 1024 * 1024
MAX_REPORT_BYTES = 2 * 1024 * 1024
MAX_BACKUP_ID_BYTES = 1_024
RETAINED_DELETED_TENANT_TABLES = frozenset(
    {
        "audit_exports",
        "audit_log",
        "legal_holds",
        "object_deletion_jobs",
        "tenant_lifecycle",
    }
)


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        del deep
        values = self.model_dump(mode="python", round_trip=True)
        if update is not None:
            values.update(update)
        return type(self).model_validate(values)


class RecoveryVerificationConfig(_Model):
    """Hard bounds for one isolated restore verification."""

    max_object_references: int = Field(
        default=MAX_OBJECT_REFERENCES, ge=1, le=MAX_OBJECT_REFERENCES
    )
    max_object_bytes: int = Field(default=MAX_OBJECT_BYTES, ge=1, le=MAX_OBJECT_BYTES)
    max_total_object_bytes: int = Field(
        default=MAX_TOTAL_OBJECT_BYTES,
        ge=1,
        le=MAX_TOTAL_OBJECT_BYTES,
    )


class RestoredTableCount(_Model):
    """One deterministic restored table count."""

    name: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,62}$")]
    rows: int = Field(ge=0)


class RecoveryVerificationReport(_Model):
    """Immutable success evidence for one restored dependency pair."""

    schema_version: Literal["agent-recovery-verification-v1"] = "agent-recovery-verification-v1"
    verification_id: uuid.UUID
    backup_id: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)
    ]
    source_revision: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
    migration_head: Annotated[str, StringConstraints(pattern=r"^[0-9]{4}$")]
    started_at: datetime
    completed_at: datetime
    table_counts: tuple[RestoredTableCount, ...]
    run_count: int = Field(ge=0)
    event_count: int = Field(ge=0)
    object_count: int = Field(ge=0, le=MAX_OBJECT_REFERENCES)
    object_bytes: int = Field(ge=0, le=MAX_TOTAL_OBJECT_BYTES)
    database_fingerprint_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    object_evidence_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    result: Literal["passed"] = "passed"

    @model_validator(mode="after")
    def validate_report(self) -> Self:
        if self.started_at.tzinfo is None or self.completed_at.tzinfo is None:
            raise ValueError("report timestamps must be timezone-aware")
        if self.completed_at < self.started_at:
            raise ValueError("report completion may not precede its start")
        if len(self.backup_id.encode("utf-8")) > MAX_BACKUP_ID_BYTES or "\x00" in self.backup_id:
            raise ValueError("backup ID must be at most 1024 UTF-8 bytes without NUL")
        if {table.name for table in self.table_counts} != set(Base.metadata.tables):
            raise ValueError("report must contain every database table count")
        if len(self.table_counts) != len(Base.metadata.tables):
            raise ValueError("report table names must be unique")
        return self


@dataclass(frozen=True, slots=True)
class _ObjectReference:
    object_key: ObjectKey
    sha256: Sha256Hex
    size_bytes: int
    audit_export: AuditExport | None = None


@dataclass(frozen=True, slots=True)
class _DatabaseEvidence:
    migration_head: str
    table_counts: dict[str, int]
    run_count: int
    event_count: int
    references: tuple[_ObjectReference, ...]
    fingerprint_sha256: str


async def verify_restored_environment(
    database: Database,
    object_store: ObjectStore,
    *,
    backup_id: str,
    source_revision: str,
    config: RecoveryVerificationConfig | None = None,
    temporary_parent: Path | None = None,
    expected_migration_head: str | None = None,
    verification_id: uuid.UUID | None = None,
    clock: Callable[[], datetime] | None = None,
) -> RecoveryVerificationReport:
    """Verify one quiescent restore and return checksum-bound success evidence."""

    policy = config or RecoveryVerificationConfig()
    now = clock or (lambda: datetime.now(UTC))
    started_at = now()
    expected_head = expected_migration_head or _repository_migration_head()
    evidence = await _read_database_evidence(database, policy, expected_head)
    parent = _temporary_parent(temporary_parent)
    directory = Path(
        await asyncio.to_thread(tempfile.mkdtemp, prefix="agent-restore-verify-", dir=parent)
    )
    await asyncio.to_thread(directory.chmod, 0o700)
    try:
        object_digest = await _verify_objects(
            object_store,
            evidence.references,
            directory=directory,
            maximum_object_bytes=policy.max_object_bytes,
        )
    finally:
        await asyncio.to_thread(shutil.rmtree, directory, True)
    return RecoveryVerificationReport(
        verification_id=verification_id or uuid.uuid4(),
        backup_id=backup_id,
        source_revision=source_revision,
        migration_head=evidence.migration_head,
        started_at=started_at,
        completed_at=now(),
        table_counts=tuple(
            RestoredTableCount(name=name, rows=rows)
            for name, rows in sorted(evidence.table_counts.items())
        ),
        run_count=evidence.run_count,
        event_count=evidence.event_count,
        object_count=len(evidence.references),
        object_bytes=sum(reference.size_bytes for reference in evidence.references),
        database_fingerprint_sha256=evidence.fingerprint_sha256,
        object_evidence_sha256=object_digest,
    )


async def _read_database_evidence(
    database: Database,
    config: RecoveryVerificationConfig,
    expected_head: str,
) -> _DatabaseEvidence:
    async with database.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await session.execute(text("SET TRANSACTION READ ONLY"))
        migration_head = await session.scalar(text("SELECT version_num FROM alembic_version"))
        if migration_head != expected_head:
            _restore_invalid("migration head does not match the verifier")
        table_counts = {
            table.name: int(
                cast("int", await session.scalar(select(func.count()).select_from(table)))
            )
            for table in Base.metadata.sorted_tables
        }
        await _verify_deleted_tenant_residue(session)
        run_count, event_count = await _verify_event_sequences(session)
        references = await _read_object_references(session, config)
    fingerprint = _database_fingerprint(
        migration_head=cast("str", migration_head),
        table_counts=table_counts,
        run_count=run_count,
        event_count=event_count,
        references=references,
    )
    return _DatabaseEvidence(
        migration_head=cast("str", migration_head),
        table_counts=table_counts,
        run_count=run_count,
        event_count=event_count,
        references=references,
        fingerprint_sha256=fingerprint,
    )


async def _verify_deleted_tenant_residue(session: AsyncSession) -> None:
    lifecycle = TenantLifecycleRecord.__table__
    for table in Base.metadata.sorted_tables:
        if "tenant_id" not in table.c or table.name in RETAINED_DELETED_TENANT_TABLES:
            continue
        residue = await session.scalar(
            select(func.count())
            .select_from(table.join(lifecycle, table.c.tenant_id == lifecycle.c.tenant_id))
            .where(lifecycle.c.status == "deleted")
        )
        if residue:
            _restore_invalid("deleted tenant application data remains in the restore")


async def _verify_event_sequences(session: AsyncSession) -> tuple[int, int]:
    rows = tuple(
        (
            await session.execute(
                select(
                    RunRecord.id,
                    RunRecord.execution_epoch,
                    RunRecord.next_event_sequence,
                    func.count(AgentEventRecord.id),
                    func.coalesce(func.min(AgentEventRecord.sequence), 0),
                    func.coalesce(func.max(AgentEventRecord.sequence), 0),
                    func.coalesce(func.max(AgentEventRecord.execution_epoch), 0),
                )
                .outerjoin(AgentEventRecord, AgentEventRecord.run_id == RunRecord.id)
                .group_by(RunRecord.id)
                .order_by(RunRecord.id)
            )
        ).all()
    )
    event_count = 0
    for (
        _run_id,
        execution_epoch,
        next_sequence,
        count,
        minimum,
        maximum,
        maximum_epoch,
    ) in rows:
        observed = int(count)
        event_count += observed
        if observed == 0:
            if int(next_sequence) != 1 or int(minimum) != 0 or int(maximum) != 0:
                _restore_invalid("empty run event sequence metadata is inconsistent")
        elif int(minimum) != 1 or int(maximum) != observed or int(next_sequence) != observed + 1:
            _restore_invalid("run event sequence is not contiguous")
        if int(maximum_epoch) > int(execution_epoch):
            _restore_invalid("run event belongs to a future execution epoch")
    return len(rows), event_count


async def _read_object_references(
    session: AsyncSession,
    config: RecoveryVerificationConfig,
) -> tuple[_ObjectReference, ...]:
    references: dict[str, _ObjectReference] = {}
    artifact_rows = tuple(
        (
            await session.execute(
                select(
                    ArtifactRecord.object_key,
                    ArtifactRecord.sha256,
                    ArtifactRecord.size_bytes,
                )
                .order_by(ArtifactRecord.object_key)
                .limit(config.max_object_references + 1)
            )
        ).all()
    )
    for object_key, sha256, size_bytes in artifact_rows:
        _add_reference(
            references,
            _ObjectReference(object_key=object_key, sha256=sha256, size_bytes=size_bytes),
            config,
        )
    del artifact_rows

    snapshot_rows = tuple(
        (
            await session.execute(
                select(
                    SourceSnapshotRecord.object_key,
                    SourceSnapshotRecord.expected_sha256,
                    SourceSnapshotRecord.compressed_bytes,
                )
                .where(SourceSnapshotRecord.expected_sha256.is_not(None))
                .order_by(SourceSnapshotRecord.object_key)
                .limit(config.max_object_references + 1)
            )
        ).all()
    )
    for object_key, sha256, size_bytes in snapshot_rows:
        if sha256 is None or size_bytes is None:
            _restore_invalid("validated source snapshot evidence is incomplete")
        _add_reference(
            references,
            _ObjectReference(object_key=object_key, sha256=sha256, size_bytes=size_bytes),
            config,
        )
    del snapshot_rows

    checkpoint_rows = tuple(
        (
            await session.scalars(
                select(CheckpointRecord.workspace_snapshot_uri)
                .order_by(CheckpointRecord.workspace_snapshot_uri)
                .limit(config.max_object_references + 1)
            )
        ).all()
    )
    for uri in checkpoint_rows:
        try:
            snapshot = DurableSnapshotReference.from_uri(uri)
        except (TypeError, ValueError):
            _restore_invalid("checkpoint snapshot URI is invalid")
        _add_reference(
            references,
            _ObjectReference(
                object_key=snapshot.object_key,
                sha256=snapshot.sha256,
                size_bytes=snapshot.size_bytes,
            ),
            config,
        )
    del checkpoint_rows

    export_rows = tuple(
        (
            await session.scalars(
                select(AuditExportRecord)
                .where(AuditExportRecord.status == AuditExportStatus.COMPLETED.value)
                .order_by(AuditExportRecord.object_key)
                .limit(config.max_object_references + 1)
            )
        ).all()
    )
    for row in export_rows:
        export = _audit_export(row)
        if export.object is None:
            _restore_invalid("completed audit export evidence is incomplete")
        _add_reference(
            references,
            _ObjectReference(
                object_key=export.object.object_key,
                sha256=export.object.sha256,
                size_bytes=export.object.size_bytes,
                audit_export=export,
            ),
            config,
        )
    del export_rows
    if sum(reference.size_bytes for reference in references.values()) > (
        config.max_total_object_bytes
    ):
        _restore_invalid("restore object evidence exceeds the aggregate byte limit")
    return tuple(references[key] for key in sorted(references))


def _add_reference(
    references: dict[str, _ObjectReference],
    candidate: _ObjectReference,
    config: RecoveryVerificationConfig,
) -> None:
    if candidate.size_bytes < 0 or candidate.size_bytes > config.max_object_bytes:
        _restore_invalid("restored object exceeds the verification limit")
    current = references.get(candidate.object_key)
    if current is not None:
        if current.sha256 != candidate.sha256 or current.size_bytes != candidate.size_bytes:
            _restore_invalid("one object key has conflicting database evidence")
        if current.audit_export is None and candidate.audit_export is not None:
            references[candidate.object_key] = candidate
        return
    if len(references) >= config.max_object_references:
        _restore_invalid("restore contains too many object references")
    references[candidate.object_key] = candidate


async def _verify_objects(
    object_store: ObjectStore,
    references: tuple[_ObjectReference, ...],
    *,
    directory: Path,
    maximum_object_bytes: int,
) -> str:
    digest = hashlib.sha256()
    for index, reference in enumerate(references):
        destination = directory / f"object-{index:06d}"
        stored = await object_store.download_to_path(
            reference.object_key,
            destination,
            max_bytes=max(1, min(maximum_object_bytes, reference.size_bytes)),
            expected_sha256=reference.sha256,
        )
        if stored.size_bytes != reference.size_bytes or stored.sha256 != reference.sha256:
            _restore_invalid("object content does not match database evidence")
        if reference.audit_export is not None:
            await asyncio.to_thread(
                verify_audit_export_file,
                destination,
                reference.audit_export,
            )
        digest.update(reference.object_key.encode("utf-8"))
        digest.update(b"\0")
        digest.update(reference.sha256.encode("ascii"))
        digest.update(b"\0")
        digest.update(str(reference.size_bytes).encode("ascii"))
        digest.update(b"\n")
        await asyncio.to_thread(destination.unlink)
    return digest.hexdigest()


def _audit_export(row: AuditExportRecord) -> AuditExport:
    if row.object_key is None or row.sha256 is None or row.size_bytes is None:
        _restore_invalid("completed audit export object metadata is incomplete")
    if row.content_type is None:
        _restore_invalid("completed audit export content type is missing")
    try:
        return AuditExport(
            id=row.id,
            tenant_id=row.tenant_id,
            status=AuditExportStatus(row.status),
            requested_by=row.requested_by,
            cutoff_at=row.cutoff_at,
            created_at=row.created_at,
            object=StoredObject(
                object_key=row.object_key,
                sha256=row.sha256,
                size_bytes=row.size_bytes,
                content_type=row.content_type,
                etag=row.etag,
            ),
            record_count=row.record_count,
            first_occurred_at=row.first_occurred_at,
            last_occurred_at=row.last_occurred_at,
            completed_at=row.completed_at,
            error=ErrorDetail.model_validate(row.error) if row.error is not None else None,
        )
    except ValueError:
        _restore_invalid("completed audit export metadata is invalid")


def _database_fingerprint(
    *,
    migration_head: str,
    table_counts: dict[str, int],
    run_count: int,
    event_count: int,
    references: tuple[_ObjectReference, ...],
) -> str:
    value = {
        "migration_head": migration_head,
        "table_counts": table_counts,
        "run_count": run_count,
        "event_count": event_count,
        "objects": [
            {
                "object_key": reference.object_key,
                "sha256": reference.sha256,
                "size_bytes": reference.size_bytes,
            }
            for reference in references
        ],
    }
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _repository_migration_head() -> str:
    root = Path(__file__).resolve().parents[1]
    configuration = Config(str(root / "alembic.ini"))
    heads = ScriptDirectory.from_config(configuration).get_heads()
    if len(heads) != 1:
        raise RuntimeError("repository migration graph must have one head")
    return heads[0]


def _temporary_parent(value: Path | None) -> str | None:
    if value is None:
        return None
    resolved = value.resolve(strict=True)
    if resolved.is_symlink() or not resolved.is_dir():
        raise ValueError("temporary parent must be a real directory")
    return str(resolved)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def write_report(path: Path, report: RecoveryVerificationReport) -> None:
    """Create one evidence file without following links or replacing prior evidence."""

    parent = path.parent.resolve(strict=True)
    if parent.is_symlink() or not parent.is_dir():
        raise ValueError("report parent must be a real directory")
    payload = _canonical_json(report.model_dump(mode="json")) + b"\n"
    if len(payload) > MAX_REPORT_BYTES:
        raise ValueError("recovery report exceeds its byte limit")
    descriptor: int | None = None
    try:
        descriptor = os.open(
            parent / path.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        _validate_written_report(descriptor, len(payload))
    except Exception:
        with suppress(OSError):
            (parent / path.name).unlink(missing_ok=True)
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="recovery-verification",
        description="Verify a quiescent isolated PostgreSQL and object-store restore.",
    )
    parser.add_argument("--backup-id", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--temporary-parent", type=Path)
    parser.add_argument("--max-object-references", type=int, default=MAX_OBJECT_REFERENCES)
    parser.add_argument("--max-object-bytes", type=int, default=MAX_OBJECT_BYTES)
    parser.add_argument("--max-total-object-bytes", type=int, default=MAX_TOTAL_OBJECT_BYTES)
    return parser


async def run(arguments: argparse.Namespace) -> RecoveryVerificationReport:
    database = Database(DatabaseSettings())
    object_store = create_s3_object_store(S3ObjectStoreSettings())
    try:
        return await verify_restored_environment(
            database,
            object_store,
            backup_id=arguments.backup_id,
            source_revision=arguments.source_revision,
            config=RecoveryVerificationConfig(
                max_object_references=arguments.max_object_references,
                max_object_bytes=arguments.max_object_bytes,
                max_total_object_bytes=arguments.max_total_object_bytes,
            ),
            temporary_parent=arguments.temporary_parent,
        )
    finally:
        try:
            await object_store.aclose()
        finally:
            await database.aclose()


def _write_json(stream: TextIO, value: object) -> None:
    stream.write(_canonical_json(value).decode("utf-8") + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        arguments = build_parser().parse_args(argv)
        report = asyncio.run(run(arguments))
        write_report(arguments.output, report)
    except DomainOperationError as error:
        _write_json(sys.stderr, {"error": error.as_dict()})
        return 2
    except (ValueError, OSError):
        _write_json(
            sys.stderr,
            {
                "error": {
                    "code": "recovery_verification_invalid",
                    "message": "recovery verification input or evidence is invalid",
                    "retryable": False,
                    "details": {},
                }
            },
        )
        return 2
    except Exception:
        _write_json(
            sys.stderr,
            {
                "error": {
                    "code": "recovery_verification_failed",
                    "message": "recovery verification could not be completed",
                    "retryable": True,
                    "details": {},
                }
            },
        )
        return 1
    _write_json(sys.stdout, report.model_dump(mode="json"))
    return 0


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("report write made no progress")
        offset += written


def _validate_written_report(descriptor: int, expected_bytes: int) -> None:
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != expected_bytes:
        raise OSError("report write did not produce exact regular-file evidence")


def _restore_invalid(message: str) -> Never:
    raise DomainOperationError(
        code="restore_integrity_failed",
        message=message,
    )


if __name__ == "__main__":
    raise SystemExit(main())
