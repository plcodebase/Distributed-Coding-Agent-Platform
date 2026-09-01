"""Bounded audit export and object-deletion lifecycle coordinators."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import stat
import tempfile
import uuid  # noqa: TC003 - runtime identifiers and deterministic object keys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Never

from pydantic import Field

from agent_core.audit import MAX_AUDIT_ENTRY_BYTES
from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.lifecycle import (
    MAX_AUDIT_EXPORT_BYTES,
    MAX_AUDIT_EXPORT_RECORDS,
    AuditExport,
    AuditExportEntry,
    AuditExportStatus,
    LifecycleAdministrationRepository,
    LifecycleBatchResult,
    TenantLifecycle,
    TenantLifecycleStatus,
    tenant_audit_export_object_key,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from agent_core.artifacts import ObjectStore
    from agent_core.domain.base import AwareTimestamp

type LifecycleClock = Callable[[], datetime]

_AUDIT_EXPORT_CONTENT_TYPE = "application/x-ndjson"
_AUDIT_HEADER_TYPE = "agent-audit-export-v1"
_AUDIT_ENTRY_TYPE = "audit-entry"
_AUDIT_TRAILER_TYPE = "audit-trailer"
_MAX_DELETE_BATCH = 1_000
_MAX_DELETE_LEASE_SECONDS = 300
_MAX_COOLING_OFF_SECONDS = 365 * 24 * 60 * 60
_MAX_WORKER_ID_BYTES = 255
_CHUNK_BYTES = 1024 * 1024
_MAX_AUDIT_LINE_BYTES = MAX_AUDIT_ENTRY_BYTES + 1_024


class LifecycleServiceConfig(DomainModel):
    """Closed operational bounds for lifecycle jobs."""

    audit_batch_size: int = Field(default=1_000, ge=1, le=1_000)
    audit_max_records: int = Field(
        default=MAX_AUDIT_EXPORT_RECORDS, ge=1, le=MAX_AUDIT_EXPORT_RECORDS
    )
    audit_max_bytes: int = Field(default=MAX_AUDIT_EXPORT_BYTES, ge=1, le=MAX_AUDIT_EXPORT_BYTES)
    deletion_batch_size: int = Field(default=100, ge=1, le=_MAX_DELETE_BATCH)
    deletion_lease_seconds: int = Field(default=60, ge=1, le=_MAX_DELETE_LEASE_SECONDS)
    deletion_cooling_off_seconds: int = Field(
        default=7 * 24 * 60 * 60,
        ge=0,
        le=_MAX_COOLING_OFF_SECONDS,
    )


class AuditExportService:
    """Create immutable bounded canonical audit exports for deletion and recovery."""

    def __init__(
        self,
        repository: LifecycleAdministrationRepository,
        object_store: ObjectStore,
        *,
        config: LifecycleServiceConfig | None = None,
        temporary_parent: Path | None = None,
        clock: LifecycleClock | None = None,
    ) -> None:
        self._repository = repository
        self._object_store = object_store
        self._config = config or LifecycleServiceConfig()
        self._temporary_parent = _temporary_parent(temporary_parent)
        self._clock = clock or (lambda: datetime.now(UTC))

    async def export(  # noqa: PLR0912 - explicit crash-resume and cleanup state machine
        self,
        tenant_id: uuid.UUID,
        *,
        export_id: uuid.UUID,
        requested_by: str,
        cutoff_at: AwareTimestamp | None = None,
    ) -> AuditExport:
        existing = await self._repository.get_audit_export(tenant_id, export_id)
        if existing is not None:
            if existing.requested_by != requested_by or (
                cutoff_at is not None and existing.cutoff_at != cutoff_at
            ):
                raise DomainOperationError(
                    code="audit_export_conflict",
                    message="the audit export identity is already in use",
                )
            if existing.status is AuditExportStatus.COMPLETED:
                return existing
            if existing.status is AuditExportStatus.FAILED:
                raise DomainOperationError(
                    code="audit_export_terminal",
                    message="the audit export is already terminal",
                )
            cutoff = existing.cutoff_at
        else:
            cutoff = cutoff_at or self._clock()
        pending = AuditExport(
            id=export_id,
            tenant_id=tenant_id,
            status=AuditExportStatus.PENDING,
            requested_by=requested_by,
            cutoff_at=cutoff,
            created_at=cutoff,
        )
        current = await self._repository.create_audit_export(pending)
        if current.status is AuditExportStatus.COMPLETED:
            return current
        if current.status is AuditExportStatus.FAILED:
            raise DomainOperationError(
                code="audit_export_terminal",
                message="the audit export is already terminal",
            )

        directory = Path(
            await asyncio.to_thread(
                tempfile.mkdtemp,
                prefix="agent-audit-export-",
                dir=self._temporary_parent,
            )
        )
        await asyncio.to_thread(directory.chmod, 0o700)
        path = directory / "audit.jsonl"
        descriptor: int | None = None
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            writer = _AuditWriter(
                descriptor,
                tenant_id=tenant_id,
                export_id=export_id,
                cutoff_at=cutoff,
                maximum_bytes=self._config.audit_max_bytes,
                maximum_records=self._config.audit_max_records,
            )
            await asyncio.to_thread(writer.write_header)
            async for batch in self._repository.iter_audit_entries(
                tenant_id,
                cutoff_at=cutoff,
                batch_size=self._config.audit_batch_size,
            ):
                await asyncio.to_thread(writer.write_entries, batch)
            await asyncio.to_thread(writer.finish)
            os.close(descriptor)
            descriptor = None

            stored = await self._object_store.upload_from_path(
                tenant_audit_export_object_key(tenant_id, export_id),
                path,
                content_type=_AUDIT_EXPORT_CONTENT_TYPE,
                max_bytes=self._config.audit_max_bytes,
            )
            completed = AuditExport(
                id=export_id,
                tenant_id=tenant_id,
                status=AuditExportStatus.COMPLETED,
                requested_by=requested_by,
                cutoff_at=cutoff,
                created_at=cutoff,
                object=stored,
                record_count=writer.record_count,
                first_occurred_at=writer.first_occurred_at,
                last_occurred_at=writer.last_occurred_at,
                completed_at=self._clock(),
            )
            return await self._repository.complete_audit_export(completed)
        except asyncio.CancelledError:
            raise
        except DomainOperationError as error:
            if not error.retryable:
                await self._repository.fail_audit_export(
                    export_id,
                    tenant_id,
                    error=error.error,
                )
            raise
        except Exception as error:
            raise DomainOperationError(
                code="audit_export_failed",
                message="the audit export could not be completed",
                retryable=True,
            ) from error
        finally:
            if descriptor is not None:
                os.close(descriptor)
            await asyncio.to_thread(shutil.rmtree, directory, True)

    async def verify(self, export: AuditExport) -> None:
        """Download, checksum, and structurally verify a completed export."""

        if export.status is not AuditExportStatus.COMPLETED or export.object is None:
            raise ValueError("only completed audit exports can be verified")
        directory = Path(
            await asyncio.to_thread(
                tempfile.mkdtemp,
                prefix="agent-audit-verify-",
                dir=self._temporary_parent,
            )
        )
        await asyncio.to_thread(directory.chmod, 0o700)
        path = directory / "audit.jsonl"
        try:
            await self._object_store.download_to_path(
                export.object.object_key,
                path,
                max_bytes=self._config.audit_max_bytes,
                expected_sha256=export.object.sha256,
            )
            await asyncio.to_thread(verify_audit_export_file, path, export)
        finally:
            await asyncio.to_thread(shutil.rmtree, directory, True)


class TenantDeletionService:
    """Coordinate export-first tenant deletion without hiding its durable stages."""

    def __init__(
        self,
        repository: LifecycleAdministrationRepository,
        audit_exports: AuditExportService,
        *,
        config: LifecycleServiceConfig | None = None,
        clock: LifecycleClock | None = None,
    ) -> None:
        self._repository = repository
        self._audit_exports = audit_exports
        self._config = config or LifecycleServiceConfig()
        self._clock = clock or (lambda: datetime.now(UTC))

    async def request(
        self,
        tenant_id: uuid.UUID,
        *,
        request_id: uuid.UUID,
        export_id: uuid.UUID,
        requested_by: str,
    ) -> tuple[TenantLifecycle, AuditExport]:
        existing_lifecycle = await self._repository.get_tenant_lifecycle(tenant_id)
        if (
            existing_lifecycle is not None
            and existing_lifecycle.status is not TenantLifecycleStatus.ACTIVE
        ):
            if (
                existing_lifecycle.request_id != request_id
                or existing_lifecycle.audit_export_id != export_id
                or existing_lifecycle.requested_by != requested_by
            ):
                raise DomainOperationError(
                    code="tenant_deletion_conflict",
                    message="tenant deletion is already requested",
                )
            existing_export = await self._repository.get_audit_export(tenant_id, export_id)
            if existing_export is None:
                raise DomainOperationError(
                    code="audit_export_required",
                    message="tenant deletion audit evidence was not found",
                )
            await self._audit_exports.verify(existing_export)
            return existing_lifecycle, existing_export
        export = await self._audit_exports.export(
            tenant_id,
            export_id=export_id,
            requested_by=requested_by,
        )
        await self._audit_exports.verify(export)
        requested_at = export.cutoff_at
        lifecycle = await self._repository.request_tenant_deletion(
            tenant_id,
            request_id=request_id,
            requested_by=requested_by,
            requested_at=requested_at,
            delete_after=requested_at
            + timedelta(seconds=self._config.deletion_cooling_off_seconds),
            audit_export_id=export_id,
        )
        return lifecycle, export

    async def prepare(self, tenant_id: uuid.UUID) -> tuple[TenantLifecycle, int]:
        occurred_at = self._clock()
        lifecycle = await self._repository.begin_tenant_deletion(
            tenant_id,
            occurred_at=occurred_at,
        )
        enqueued = await self._repository.enqueue_tenant_objects(
            tenant_id,
            limit=self._config.deletion_batch_size,
        )
        return lifecycle, enqueued

    async def finalize(self, tenant_id: uuid.UUID) -> TenantLifecycle:
        return await self._repository.finalize_tenant_deletion(
            tenant_id,
            occurred_at=self._clock(),
        )


class ObjectDeletionWorker:
    """Consume bounded idempotent deletion leases from the durable outbox."""

    def __init__(
        self,
        repository: LifecycleAdministrationRepository,
        object_store: ObjectStore,
        *,
        worker_id: str,
        config: LifecycleServiceConfig | None = None,
        clock: LifecycleClock | None = None,
    ) -> None:
        if (
            not worker_id.strip()
            or len(worker_id.encode("utf-8")) > _MAX_WORKER_ID_BYTES
            or "\x00" in worker_id
        ):
            raise ValueError("worker_id must be nonempty and at most 255 UTF-8 bytes")
        self._repository = repository
        self._object_store = object_store
        self._worker_id = worker_id
        self._config = config or LifecycleServiceConfig()
        self._clock = clock or (lambda: datetime.now(UTC))

    async def run_batch(self) -> LifecycleBatchResult:
        leases = await self._repository.claim_object_deletions(
            self._worker_id,
            occurred_at=self._clock(),
            lease_seconds=self._config.deletion_lease_seconds,
            limit=self._config.deletion_batch_size,
        )
        completed = 0
        failed = 0
        for lease in leases:
            try:
                await self._object_store.delete(lease.object_key)
            except asyncio.CancelledError:
                raise
            except DomainOperationError as error:
                await self._repository.fail_object_deletion(
                    lease,
                    error=error.error,
                    occurred_at=self._clock(),
                )
                failed += 1
                continue
            except Exception:
                await self._repository.fail_object_deletion(
                    lease,
                    error=ErrorDetail(
                        code="object_deletion_failed",
                        message="object storage deletion failed",
                        retryable=True,
                    ),
                    occurred_at=self._clock(),
                )
                failed += 1
                continue
            await self._repository.complete_object_deletion(lease)
            completed += 1
        return LifecycleBatchResult(claimed=len(leases), completed=completed, failed=failed)


class _AuditWriter:
    def __init__(
        self,
        descriptor: int,
        *,
        tenant_id: uuid.UUID,
        export_id: uuid.UUID,
        cutoff_at: datetime,
        maximum_bytes: int,
        maximum_records: int,
    ) -> None:
        self._descriptor = descriptor
        self._tenant_id = tenant_id
        self._export_id = export_id
        self._cutoff_at = cutoff_at
        self._maximum_bytes = maximum_bytes
        self._maximum_records = maximum_records
        self._written_bytes = 0
        self._last_key: tuple[datetime, uuid.UUID] | None = None
        self.record_count = 0
        self.first_occurred_at: datetime | None = None
        self.last_occurred_at: datetime | None = None
        self._finished = False

    def write_header(self) -> None:
        self._write_json(
            {
                "type": _AUDIT_HEADER_TYPE,
                "export_id": str(self._export_id),
                "tenant_id": str(self._tenant_id),
                "cutoff_at": _timestamp(self._cutoff_at),
            }
        )

    def write_entries(self, entries: Iterable[AuditExportEntry]) -> None:
        for entry in entries:
            if entry.tenant_id != self._tenant_id or entry.occurred_at > self._cutoff_at:
                raise DomainOperationError(
                    code="audit_export_protocol_error",
                    message="audit repository returned an out-of-scope entry",
                )
            key = (entry.occurred_at, entry.id)
            if self._last_key is not None and key <= self._last_key:
                raise DomainOperationError(
                    code="audit_export_protocol_error",
                    message="audit repository entries are not totally ordered",
                )
            if self.record_count >= self._maximum_records:
                raise DomainOperationError(
                    code="audit_export_record_limit",
                    message="audit export exceeded its record limit",
                )
            self._write_json(
                {
                    "type": _AUDIT_ENTRY_TYPE,
                    "entry": entry.model_dump(mode="json"),
                }
            )
            self.record_count += 1
            self.first_occurred_at = self.first_occurred_at or entry.occurred_at
            self.last_occurred_at = entry.occurred_at
            self._last_key = key

    def finish(self) -> None:
        if self._finished:
            raise RuntimeError("audit export writer is already finished")
        self._write_json(
            {
                "type": _AUDIT_TRAILER_TYPE,
                "record_count": self.record_count,
                "first_occurred_at": (
                    _timestamp(self.first_occurred_at)
                    if self.first_occurred_at is not None
                    else None
                ),
                "last_occurred_at": (
                    _timestamp(self.last_occurred_at) if self.last_occurred_at is not None else None
                ),
            }
        )
        os.fsync(self._descriptor)
        self._finished = True

    def _write_json(self, value: Any) -> None:
        payload = (
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
        if self._written_bytes + len(payload) > self._maximum_bytes:
            raise DomainOperationError(
                code="audit_export_byte_limit",
                message="audit export exceeded its byte limit",
            )
        _write_all(self._descriptor, payload)
        self._written_bytes += len(payload)


def verify_audit_export_file(  # noqa: PLR0912, PLR0915 - explicit integrity state machine
    path: Path,
    export: AuditExport,
) -> None:
    """Fail closed on malformed, reordered, truncated, or mismatched audit JSONL."""

    if export.status is not AuditExportStatus.COMPLETED or export.object is None:
        raise ValueError("only completed audit exports can be verified")
    if path.is_symlink():
        raise DomainOperationError(
            code="audit_export_invalid",
            message="audit export is not a regular file",
        )
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != export.object.size_bytes:
        raise DomainOperationError(
            code="audit_export_invalid",
            message="audit export size does not match its evidence",
        )
    digest = hashlib.sha256()
    observed = 0
    header_seen = False
    trailer_seen = False
    first: datetime | None = None
    last: datetime | None = None
    last_key: tuple[datetime, uuid.UUID] | None = None
    record_count = 0
    with path.open("rb") as source:
        while raw_line := source.readline(_MAX_AUDIT_LINE_BYTES + 1):
            observed += len(raw_line)
            if (
                len(raw_line) > _MAX_AUDIT_LINE_BYTES
                or observed > MAX_AUDIT_EXPORT_BYTES
                or not raw_line.endswith(b"\n")
            ):
                _invalid_export()
            digest.update(raw_line)
            value = _json_object(raw_line)
            item_type = value.get("type")
            if not header_seen:
                if value != {
                    "type": _AUDIT_HEADER_TYPE,
                    "export_id": str(export.id),
                    "tenant_id": str(export.tenant_id),
                    "cutoff_at": _timestamp(export.cutoff_at),
                }:
                    _invalid_export()
                header_seen = True
                continue
            if trailer_seen:
                _invalid_export()
            if item_type == _AUDIT_TRAILER_TYPE:
                expected = {
                    "type": _AUDIT_TRAILER_TYPE,
                    "record_count": export.record_count,
                    "first_occurred_at": (
                        _timestamp(export.first_occurred_at)
                        if export.first_occurred_at is not None
                        else None
                    ),
                    "last_occurred_at": (
                        _timestamp(export.last_occurred_at)
                        if export.last_occurred_at is not None
                        else None
                    ),
                }
                if value != expected:
                    _invalid_export()
                trailer_seen = True
                continue
            if item_type != _AUDIT_ENTRY_TYPE or set(value) != {"type", "entry"}:
                _invalid_export()
            try:
                entry = AuditExportEntry.model_validate(value["entry"])
            except ValueError:
                _invalid_export()
            if entry.tenant_id != export.tenant_id or entry.occurred_at > export.cutoff_at:
                _invalid_export()
            key = (entry.occurred_at, entry.id)
            if last_key is not None and key <= last_key:
                _invalid_export()
            first = first or entry.occurred_at
            last = entry.occurred_at
            last_key = key
            record_count += 1
    if (
        not header_seen
        or not trailer_seen
        or observed != metadata.st_size
        or digest.hexdigest() != export.object.sha256
        or record_count != export.record_count
        or first != export.first_occurred_at
        or last != export.last_occurred_at
    ):
        _invalid_export()


def _json_object(payload: bytes) -> dict[str, Any]:
    try:
        value = json.loads(payload, parse_constant=_reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        _invalid_export()
    if not isinstance(value, dict):
        _invalid_export()
    return value


def _reject_constant(value: str) -> None:
    raise ValueError(f"nonfinite JSON constant: {value}")


def _invalid_export() -> Never:
    raise DomainOperationError(
        code="audit_export_invalid",
        message="audit export failed integrity validation",
    )


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("audit export write made no progress")
        offset += written


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _temporary_parent(value: Path | None) -> str | None:
    if value is None:
        return None
    resolved = value.resolve(strict=True)
    if not resolved.is_dir() or resolved.is_symlink():
        raise ValueError("temporary_parent must be a real directory")
    return str(resolved)


__all__ = [
    "AuditExportService",
    "LifecycleServiceConfig",
    "ObjectDeletionWorker",
    "TenantDeletionService",
    "verify_audit_export_file",
]
