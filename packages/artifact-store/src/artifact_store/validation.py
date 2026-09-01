"""Fail-closed validation for immutable repository source archives."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import tarfile
import tempfile
import time
import uuid
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Protocol, Self

from pydantic import Field, model_validator

from agent_core.artifacts import (
    MAX_SNAPSHOT_VALIDATION_ATTEMPTS,
    MAX_SNAPSHOT_VALIDATION_LEASE_SECONDS,
    MAX_VALIDATION_WORKER_ID_BYTES,
    Artifact,
    ArtifactKind,
    ObjectStore,
    SnapshotValidationRepository,
    SourceSnapshot,
    SourceSnapshotStatus,
    StoredObject,
)
from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import Sha256Hex  # noqa: TC001 - Pydantic runtime field
from agent_core.workspace_access import WorkspaceAccessPolicy, normalize_workspace_path

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import IO

    from agent_core.domain.base import JsonObject
_READ_CHUNK_BYTES = 1024 * 1024
_MAX_MODE = 0o777
_PRIVATE_KEY_MARKERS = (
    b"-----BEGIN PRIVATE KEY-----",
    b"-----BEGIN RSA PRIVATE KEY-----",
    b"-----BEGIN EC PRIVATE KEY-----",
    b"-----BEGIN OPENSSH PRIVATE KEY-----",
    b"-----BEGIN DSA PRIVATE KEY-----",
)


class SnapshotEntryType(StrEnum):
    """Portable source entries accepted by the platform."""

    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"


class SnapshotManifestEntry(DomainModel):
    """One canonical entry in a validated source snapshot."""

    path: str = Field(min_length=1, max_length=4096)
    type: SnapshotEntryType
    mode: int = Field(ge=0, le=_MAX_MODE)
    size_bytes: int = Field(ge=0)
    sha256: Sha256Hex | None = None
    link_target: str | None = Field(default=None, min_length=1, max_length=4096)

    @model_validator(mode="after")
    def validate_type_fields(self) -> Self:
        if self.type is SnapshotEntryType.FILE:
            if self.sha256 is None or self.link_target is not None:
                raise ValueError("file manifest entries require only a checksum")
        elif self.type is SnapshotEntryType.SYMLINK:
            if self.link_target is None or self.sha256 is not None or self.size_bytes != 0:
                raise ValueError("symlink manifest entries require only a target")
        elif self.sha256 is not None or self.link_target is not None or self.size_bytes != 0:
            raise ValueError("directory manifest entries may not contain file metadata")
        return self


class SnapshotValidationResult(DomainModel):
    """Bounded manifest and verified uploaded-object identity."""

    object: StoredObject
    manifest_sha256: Sha256Hex
    entries: tuple[SnapshotManifestEntry, ...]
    expanded_bytes: int = Field(ge=0)


class SecretDetector(Protocol):
    """Injected high-confidence content scanner."""

    def contains_secret(self, path: PurePosixPath, data: bytes) -> bool: ...


class HighConfidenceSecretDetector:
    """Detect private-key material without broad entropy heuristics."""

    def contains_secret(self, path: PurePosixPath, data: bytes) -> bool:
        del path
        return any(marker in data for marker in _PRIVATE_KEY_MARKERS)


class SnapshotValidator:
    """Download, hash, and inspect one bounded gzip-compressed tar archive."""

    def __init__(
        self,
        object_store: ObjectStore,
        *,
        access_policy: WorkspaceAccessPolicy | None = None,
        secret_detector: SecretDetector | None = None,
        max_compressed_bytes: int = 256 * 1024 * 1024,
        max_expanded_bytes: int = 2 * 1024 * 1024 * 1024,
        max_file_bytes: int = 64 * 1024 * 1024,
        max_entries: int = 20_000,
        timeout_seconds: float = 120.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        for name, value in (
            ("max_compressed_bytes", max_compressed_bytes),
            ("max_expanded_bytes", max_expanded_bytes),
            ("max_file_bytes", max_file_bytes),
            ("max_entries", max_entries),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._object_store = object_store
        self._access_policy = access_policy or WorkspaceAccessPolicy()
        self._secret_detector = secret_detector or HighConfidenceSecretDetector()
        self._max_compressed_bytes = max_compressed_bytes
        self._max_expanded_bytes = max_expanded_bytes
        self._max_file_bytes = max_file_bytes
        self._max_entries = max_entries
        self._timeout_seconds = timeout_seconds
        self._monotonic = monotonic

    async def validate(self, snapshot: SourceSnapshot) -> SnapshotValidationResult:
        """Validate one immutable archive without extracting it."""

        temporary_root = Path(tempfile.mkdtemp(prefix="agent-snapshot-"))
        archive = temporary_root / "source.tar.gz"
        try:
            return await self._download_and_validate(snapshot, archive)
        finally:
            await _await_thread(shutil.rmtree, temporary_root, True)

    async def materialize(
        self,
        snapshot: SourceSnapshot,
        destination: Path,
    ) -> SnapshotValidationResult:
        """Validate and safely expand one archive into a private empty directory."""

        resolved = await _await_thread(_prepare_destination, destination)
        temporary_root = Path(tempfile.mkdtemp(prefix="agent-snapshot-"))
        archive = temporary_root / "source.tar.gz"
        try:
            result = await self._download_and_validate(snapshot, archive)
            await _await_thread(self._extract_archive, archive, resolved, result.entries)
        except BaseException:
            await _await_thread(_clear_directory, resolved)
            raise
        else:
            return result
        finally:
            await _await_thread(shutil.rmtree, temporary_root, True)

    async def _download_and_validate(
        self,
        snapshot: SourceSnapshot,
        archive: Path,
    ) -> SnapshotValidationResult:
        if (
            snapshot.status is not SourceSnapshotStatus.VALIDATING
            or snapshot.expected_sha256 is None
            or snapshot.compressed_bytes is None
        ):
            raise DomainOperationError(
                code="snapshot_validation_invalid",
                message="source snapshot is not ready for validation",
            )
        if snapshot.compressed_bytes > self._max_compressed_bytes:
            raise _snapshot_error("snapshot_compressed_limit", "source archive is too large")

        stored = await self._object_store.download_to_path(
            snapshot.object_key,
            archive,
            max_bytes=self._max_compressed_bytes,
            expected_sha256=snapshot.expected_sha256,
        )
        if stored.size_bytes != snapshot.compressed_bytes:
            raise _snapshot_error(
                "snapshot_size_mismatch",
                "source archive size does not match finalization metadata",
            )
        return await _await_thread(self._validate_archive, archive, stored)

    def _extract_archive(  # noqa: PLR0912 - closed handling for three archive entry kinds
        self,
        archive: Path,
        destination: Path,
        entries: tuple[SnapshotManifestEntry, ...],
    ) -> None:
        expected = {entry.path: entry for entry in entries}
        try:
            with tarfile.open(archive, mode="r:gz") as source:
                members = {
                    normalized.as_posix(): member
                    for member in source.getmembers()
                    if (normalized := _normalize_member_path(member)) is not None
                }
                if set(members) != set(expected):
                    raise _snapshot_error(
                        "snapshot_protocol_error",
                        "source archive changed between validation and extraction",
                    )
                for entry in entries:
                    if entry.type is SnapshotEntryType.DIRECTORY:
                        target = destination.joinpath(*PurePosixPath(entry.path).parts)
                        target.mkdir(mode=0o700, parents=True, exist_ok=True)
                for entry in entries:
                    if entry.type is not SnapshotEntryType.FILE:
                        continue
                    target = destination.joinpath(*PurePosixPath(entry.path).parts)
                    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    file_object = source.extractfile(members[entry.path])
                    if file_object is None:
                        raise _snapshot_error(
                            "snapshot_protocol_error",
                            "validated source content disappeared during extraction",
                        )
                    digest = hashlib.sha256()
                    observed = 0
                    with target.open("xb") as output:
                        while chunk := file_object.read(_READ_CHUNK_BYTES):
                            observed += len(chunk)
                            if observed > entry.size_bytes:
                                raise _snapshot_error(
                                    "snapshot_protocol_error",
                                    "validated source content exceeded its recorded size",
                                )
                            output.write(chunk)
                            digest.update(chunk)
                    if observed != entry.size_bytes or digest.hexdigest() != entry.sha256:
                        raise _snapshot_error(
                            "snapshot_protocol_error",
                            "materialized source content failed integrity validation",
                        )
                    target.chmod(entry.mode)
                for entry in entries:
                    if entry.type is not SnapshotEntryType.SYMLINK:
                        continue
                    target = destination.joinpath(*PurePosixPath(entry.path).parts)
                    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    if entry.link_target is None:
                        raise AssertionError("validated symlink requires a target")
                    target.symlink_to(entry.link_target)
                for entry in reversed(entries):
                    if entry.type is SnapshotEntryType.DIRECTORY:
                        destination.joinpath(*PurePosixPath(entry.path).parts).chmod(entry.mode)
        except DomainOperationError:
            raise
        except (OSError, EOFError, tarfile.TarError) as error:
            raise _snapshot_error(
                "snapshot_materialization_failed",
                "source archive could not be materialized",
            ) from error

    def _validate_archive(
        self,
        archive: Path,
        stored: StoredObject,
    ) -> SnapshotValidationResult:
        deadline = self._monotonic() + self._timeout_seconds
        entries: list[SnapshotManifestEntry] = []
        identities: set[str] = set()
        expanded_bytes = 0
        try:
            with tarfile.open(archive, mode="r:gz") as source:
                for member in source:
                    self._require_time(deadline)
                    if len(entries) >= self._max_entries:
                        raise _snapshot_error(
                            "snapshot_entry_limit",
                            "source archive contains too many entries",
                        )
                    normalized = _normalize_member_path(member)
                    if normalized is None:
                        continue
                    identity = normalized.as_posix().casefold()
                    if identity in identities:
                        raise _snapshot_error(
                            "snapshot_duplicate_path",
                            "source archive contains duplicate canonical paths",
                            path=normalized,
                        )
                    identities.add(identity)
                    self._require_accessible(normalized)
                    entry, expanded_bytes = self._validate_member(
                        source,
                        member,
                        normalized,
                        expanded_bytes,
                        deadline,
                    )
                    entries.append(entry)
        except DomainOperationError:
            raise
        except (OSError, EOFError, tarfile.TarError, UnicodeError):
            raise _snapshot_error(
                "snapshot_archive_invalid",
                "source archive is malformed or unreadable",
            ) from None

        ordered = tuple(sorted(entries, key=lambda entry: entry.path.encode("utf-8")))
        manifest_hash = hashlib.sha256()
        for entry in ordered:
            manifest_hash.update(
                json.dumps(
                    entry.model_dump(mode="json"),
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            )
            manifest_hash.update(b"\n")
        return SnapshotValidationResult(
            object=stored,
            manifest_sha256=manifest_hash.hexdigest(),
            entries=ordered,
            expanded_bytes=expanded_bytes,
        )

    def _validate_member(
        self,
        source: tarfile.TarFile,
        member: tarfile.TarInfo,
        path: PurePosixPath,
        expanded_bytes: int,
        deadline: float,
    ) -> tuple[SnapshotManifestEntry, int]:
        if member.isdir():
            return (
                SnapshotManifestEntry(
                    path=path.as_posix(),
                    type=SnapshotEntryType.DIRECTORY,
                    mode=0o755,
                    size_bytes=0,
                ),
                expanded_bytes,
            )
        if member.issym():
            target = _contained_link_target(path, member.linkname)
            self._require_accessible(target)
            return (
                SnapshotManifestEntry(
                    path=path.as_posix(),
                    type=SnapshotEntryType.SYMLINK,
                    mode=0o777,
                    size_bytes=0,
                    link_target=member.linkname,
                ),
                expanded_bytes,
            )
        if not member.isfile():
            raise _snapshot_error(
                "snapshot_entry_unsupported",
                "source archive contains an unsupported entry type",
                path=path,
            )
        if member.size < 0 or member.size > self._max_file_bytes:
            raise _snapshot_error(
                "snapshot_file_limit",
                "source archive file exceeds its byte limit",
                path=path,
            )
        expanded_bytes += member.size
        if expanded_bytes > self._max_expanded_bytes:
            raise _snapshot_error(
                "snapshot_expanded_limit",
                "source archive exceeds its expanded byte limit",
            )
        file_object = source.extractfile(member)
        if file_object is None:
            raise _snapshot_error(
                "snapshot_protocol_error",
                "source archive file content is missing",
                path=path,
            )
        checksum = self._hash_file(path, file_object, member.size, deadline)
        return (
            SnapshotManifestEntry(
                path=path.as_posix(),
                type=SnapshotEntryType.FILE,
                mode=0o755 if member.mode & 0o111 else 0o644,
                size_bytes=member.size,
                sha256=checksum,
            ),
            expanded_bytes,
        )

    def _hash_file(
        self,
        path: PurePosixPath,
        file_object: IO[bytes],
        declared_size: int,
        deadline: float,
    ) -> str:
        digest = hashlib.sha256()
        observed = 0
        detector_tail = b""
        while chunk := file_object.read(_READ_CHUNK_BYTES):
            self._require_time(deadline)
            observed += len(chunk)
            if observed > declared_size or observed > self._max_file_bytes:
                raise _snapshot_error(
                    "snapshot_file_limit",
                    "source archive file exceeds its declared size",
                    path=path,
                )
            scan = detector_tail + chunk
            if self._secret_detector.contains_secret(path, scan):
                raise _snapshot_error(
                    "snapshot_secret_detected",
                    "source archive contains high-confidence secret material",
                    path=path,
                )
            detector_tail = scan[-4096:]
            digest.update(chunk)
        if observed != declared_size:
            raise _snapshot_error(
                "snapshot_file_size_mismatch",
                "source archive file size does not match its header",
                path=path,
            )
        return digest.hexdigest()

    def _require_accessible(self, path: PurePosixPath) -> None:
        try:
            self._access_policy.require_accessible(path, display_path=path.as_posix())
        except DomainOperationError:
            code = (
                "snapshot_metadata_protected"
                if self._access_policy.is_repository_metadata(path)
                else "snapshot_path_protected"
            )
            raise _snapshot_error(
                code,
                "source archive contains a protected path",
                path=path,
            ) from None

    def _require_time(self, deadline: float) -> None:
        if self._monotonic() > deadline:
            raise _snapshot_error("snapshot_timeout", "source archive validation timed out")


class SnapshotValidationWorker:
    """Claim and resolve one durable validation job at a time."""

    def __init__(
        self,
        repository: SnapshotValidationRepository,
        validator: SnapshotValidator,
        *,
        worker_id: str,
        lease_seconds: int = MAX_SNAPSHOT_VALIDATION_LEASE_SECONDS,
        clock: Callable[[], datetime] | None = None,
        id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        if not worker_id or len(worker_id.encode("utf-8")) > MAX_VALIDATION_WORKER_ID_BYTES:
            raise ValueError("worker_id must be nonempty bounded text")
        if (
            type(lease_seconds) is not int
            or not 1 <= lease_seconds <= MAX_SNAPSHOT_VALIDATION_LEASE_SECONDS
        ):
            raise ValueError("lease_seconds must be an integer in [1, 300]")
        self._repository = repository
        self._validator = validator
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._clock = clock or (lambda: datetime.now(UTC))
        self._id_factory = id_factory

    async def run_once(self) -> bool:
        claimed = await self._repository.claim_validation_job(
            self._worker_id,
            occurred_at=self._clock(),
            lease_seconds=self._lease_seconds,
        )
        if claimed is None:
            return False
        lease, snapshot = claimed
        try:
            result = await self._validator.validate(snapshot)
        except DomainOperationError as error:
            occurred_at = self._clock()
            if error.retryable and lease.attempt < MAX_SNAPSHOT_VALIDATION_ATTEMPTS:
                await self._repository.release_validation(lease, occurred_at=occurred_at)
                return True
            rejection = snapshot.model_copy(
                update={
                    "status": SourceSnapshotStatus.REJECTED,
                    "error": error.error,
                    "updated_at": occurred_at,
                }
            )
            await self._repository.reject_validation(rejection, lease=lease)
            return True
        except Exception:
            occurred_at = self._clock()
            if lease.attempt < MAX_SNAPSHOT_VALIDATION_ATTEMPTS:
                await self._repository.release_validation(lease, occurred_at=occurred_at)
                raise
            opaque_error = DomainOperationError(
                code="snapshot_validation_failed",
                message="source snapshot validation failed",
            )
            rejection = snapshot.model_copy(
                update={
                    "status": SourceSnapshotStatus.REJECTED,
                    "error": opaque_error.error,
                    "updated_at": occurred_at,
                }
            )
            await self._repository.reject_validation(rejection, lease=lease)
            return True

        completed_at = self._clock()
        artifact_id = self._id_factory()
        ready = snapshot.model_copy(
            update={
                "status": SourceSnapshotStatus.READY,
                "artifact_id": artifact_id,
                "manifest_sha256": result.manifest_sha256,
                "entry_count": len(result.entries),
                "expanded_bytes": result.expanded_bytes,
                "updated_at": completed_at,
            }
        )
        artifact = Artifact(
            id=artifact_id,
            tenant_id=snapshot.tenant_id,
            workspace_id=snapshot.workspace_id,
            kind=ArtifactKind.SOURCE_SNAPSHOT,
            object=result.object,
            created_at=completed_at,
        )
        await self._repository.complete_validation(ready, artifact, lease=lease)
        return True


def _normalize_member_path(member: tarfile.TarInfo) -> PurePosixPath | None:
    raw = member.name[:-1] if member.isdir() and member.name.endswith("/") else member.name
    if member.isdir() and raw in {"", "."}:
        return None
    try:
        normalized = normalize_workspace_path(raw)
    except DomainOperationError:
        raise _snapshot_error(
            "snapshot_path_invalid",
            "source archive contains an invalid path",
        ) from None
    if normalized.as_posix() != raw:
        raise _snapshot_error(
            "snapshot_path_invalid",
            "source archive paths must be canonical",
            path=normalized,
        )
    return normalized


def _contained_link_target(path: PurePosixPath, raw_target: str) -> PurePosixPath:
    if not raw_target or "\x00" in raw_target:
        raise _snapshot_error(
            "snapshot_symlink_invalid",
            "source archive symlink target is invalid",
            path=path,
        )
    target = PurePosixPath(raw_target)
    if target.is_absolute():
        raise _snapshot_error(
            "snapshot_symlink_escape",
            "source archive symlink escapes the workspace",
            path=path,
        )
    parts: list[str] = list(path.parent.parts)
    for part in target.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            if not parts:
                raise _snapshot_error(
                    "snapshot_symlink_escape",
                    "source archive symlink escapes the workspace",
                    path=path,
                )
            parts.pop()
        else:
            parts.append(part)
    if not parts:
        raise _snapshot_error(
            "snapshot_symlink_invalid",
            "source archive symlink target is invalid",
            path=path,
        )
    return PurePosixPath(*parts)


def _snapshot_error(
    code: str,
    message: str,
    *,
    path: PurePosixPath | None = None,
) -> DomainOperationError:
    details: JsonObject | None = {"path": path.as_posix()} if path is not None else None
    return DomainOperationError(code=code, message=message, details=details)


async def _await_thread[ResultT](
    function: Callable[..., ResultT],
    *args: object,
) -> ResultT:
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def _clear_directory(path: Path) -> None:
    for child in tuple(path.iterdir()):
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)


def _prepare_destination(destination: Path) -> Path:
    resolved = destination.resolve(strict=True)
    if not resolved.is_dir() or any(resolved.iterdir()):
        raise ValueError("snapshot destination must be an existing empty directory")
    resolved.chmod(0o700)
    return resolved


__all__ = [
    "HighConfidenceSecretDetector",
    "SecretDetector",
    "SnapshotEntryType",
    "SnapshotManifestEntry",
    "SnapshotValidationResult",
    "SnapshotValidationWorker",
    "SnapshotValidator",
]
