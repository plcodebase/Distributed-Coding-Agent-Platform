from __future__ import annotations

import asyncio
import hashlib
import io
import tarfile
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from agent_core.artifacts import (
    Artifact,
    ObjectStat,
    PresignedDownload,
    PresignedUpload,
    SnapshotValidationLease,
    SourceSnapshot,
    SourceSnapshotStatus,
    StoredObject,
    Workspace,
    WorkspaceStatus,
)
from agent_core.domain.errors import DomainOperationError
from agent_core.workspace_access import WorkspaceAccessPolicy
from artifact_store import (
    SnapshotEntryType,
    SnapshotValidationResult,
    SnapshotValidationWorker,
    SnapshotValidator,
)

if TYPE_CHECKING:
    from pathlib import Path

    from agent_core.artifacts import MediaType, ObjectKey
    from agent_core.domain.base import AwareTimestamp
    from agent_core.domain.models import Sha256Hex

NOW = datetime(2026, 8, 20, tzinfo=UTC)
TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
WORKSPACE_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")
SNAPSHOT_ID = uuid.UUID("00000000-0000-0000-0000-000000000003")
OBJECT_KEY = "tenants/one/source.tar.gz"


class _ObjectStore:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.destination_parent: Path | None = None

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
        raise NotImplementedError

    async def head(self, object_key: ObjectKey) -> ObjectStat | None:
        raise NotImplementedError

    async def create_download(
        self,
        *,
        object_key: ObjectKey,
        expires_at: AwareTimestamp,
    ) -> PresignedDownload:
        raise NotImplementedError

    async def download_to_path(
        self,
        object_key: ObjectKey,
        destination: Path,
        *,
        max_bytes: int,
        expected_sha256: Sha256Hex,
    ) -> StoredObject:
        assert object_key == OBJECT_KEY
        assert len(self.content) <= max_bytes
        assert hashlib.sha256(self.content).hexdigest() == expected_sha256
        self.destination_parent = destination.parent
        await asyncio.to_thread(destination.write_bytes, self.content)
        return StoredObject(
            object_key=object_key,
            sha256=expected_sha256,
            size_bytes=len(self.content),
            content_type="application/gzip",
        )

    async def upload_from_path(
        self,
        object_key: ObjectKey,
        source: Path,
        *,
        content_type: MediaType,
        max_bytes: int,
    ) -> StoredObject:
        raise NotImplementedError

    async def delete(self, object_key: ObjectKey) -> None:
        raise NotImplementedError

    async def aclose(self) -> None:
        return None


class _ValidationRepository:
    def __init__(self, snapshot: SourceSnapshot) -> None:
        self.snapshot = snapshot
        self.lease = SnapshotValidationLease(
            job_id=uuid.uuid4(),
            tenant_id=snapshot.tenant_id,
            workspace_id=snapshot.workspace_id,
            snapshot_id=snapshot.id,
            expected_workspace_version=0,
            worker_id="validator-1",
            lease_token=uuid.uuid4(),
            lease_generation=1,
            attempt=1,
            expires_at=NOW + timedelta(minutes=5),
        )
        self.claimed = False
        self.completed: tuple[SourceSnapshot, Artifact] | None = None
        self.rejected: SourceSnapshot | None = None
        self.released = False

    async def claim_validation_job(
        self,
        worker_id: str,
        *,
        occurred_at: AwareTimestamp,
        lease_seconds: int,
    ) -> tuple[SnapshotValidationLease, SourceSnapshot] | None:
        assert worker_id == "validator-1"
        assert occurred_at == NOW
        assert lease_seconds == 300
        if self.claimed:
            return None
        self.claimed = True
        return self.lease, self.snapshot

    async def complete_validation(
        self,
        snapshot: SourceSnapshot,
        artifact: Artifact,
        *,
        lease: SnapshotValidationLease,
    ) -> Workspace:
        assert lease == self.lease
        self.completed = (snapshot, artifact)
        return Workspace(
            id=snapshot.workspace_id,
            tenant_id=snapshot.tenant_id,
            status=WorkspaceStatus.READY,
            display_name="sample",
            current_snapshot_id=snapshot.id,
            version=1,
            created_at=NOW,
            updated_at=snapshot.updated_at,
        )

    async def reject_validation(
        self,
        snapshot: SourceSnapshot,
        *,
        lease: SnapshotValidationLease,
    ) -> SourceSnapshot:
        assert lease == self.lease
        self.rejected = snapshot
        return snapshot

    async def release_validation(
        self,
        lease: SnapshotValidationLease,
        *,
        occurred_at: AwareTimestamp,
    ) -> None:
        assert lease == self.lease
        assert occurred_at == NOW
        self.released = True


def _archive(*entries: tuple[str, str, bytes | str, int]) -> bytes:
    """Build entries as name, type, content/target, mode."""

    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz", format=tarfile.PAX_FORMAT) as archive:
        for name, entry_type, content, mode in entries:
            info = tarfile.TarInfo(name)
            info.mode = mode
            if entry_type == "file":
                assert isinstance(content, bytes)
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))
            elif entry_type == "directory":
                info.type = tarfile.DIRTYPE
                info.size = 0
                archive.addfile(info)
            elif entry_type == "symlink":
                assert isinstance(content, str)
                info.type = tarfile.SYMTYPE
                info.linkname = content
                info.size = 0
                archive.addfile(info)
            elif entry_type == "hardlink":
                assert isinstance(content, str)
                info.type = tarfile.LNKTYPE
                info.linkname = content
                info.size = 0
                archive.addfile(info)
            else:
                raise AssertionError(entry_type)
    return output.getvalue()


def _snapshot(content: bytes) -> SourceSnapshot:
    return SourceSnapshot(
        id=SNAPSHOT_ID,
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        status=SourceSnapshotStatus.VALIDATING,
        object_key=OBJECT_KEY,
        expected_sha256=hashlib.sha256(content).hexdigest(),
        compressed_bytes=len(content),
        created_at=NOW,
        updated_at=NOW,
    )


@pytest.mark.asyncio
async def test_validation_produces_canonical_manifest_and_cleans_temporary_data() -> None:
    content = _archive(
        ("src", "directory", b"", 0o777),
        ("src/main.py", "file", b"print('hello')\n", 0o755),
        ("README.md", "file", b"hello\n", 0o600),
        ("readme-link", "symlink", "README.md", 0o777),
    )
    store = _ObjectStore(content)

    result = await SnapshotValidator(store).validate(_snapshot(content))

    assert [entry.path for entry in result.entries] == [
        "README.md",
        "readme-link",
        "src",
        "src/main.py",
    ]
    assert result.entries[0].mode == 0o644
    assert result.entries[-1].mode == 0o755
    assert result.entries[1].type is SnapshotEntryType.SYMLINK
    assert result.expanded_bytes == len(b"print('hello')\nhello\n")
    assert store.destination_parent is not None
    assert not store.destination_parent.exists()


@pytest.mark.asyncio
async def test_materialization_revalidates_and_extracts_private_tree(tmp_path: Path) -> None:
    content = _archive(
        ("src", "directory", b"", 0o755),
        ("src/main.py", "file", b"print('hello')\n", 0o755),
        ("README.md", "file", b"hello\n", 0o644),
        ("readme-link", "symlink", "README.md", 0o777),
    )
    destination = tmp_path / "source"
    destination.mkdir(mode=0o755)

    result = await SnapshotValidator(_ObjectStore(content)).materialize(
        _snapshot(content),
        destination,
    )

    assert result.expanded_bytes == len(b"print('hello')\nhello\n")
    assert destination.stat().st_mode & 0o777 == 0o700
    assert (destination / "src/main.py").read_bytes() == b"print('hello')\n"
    assert (destination / "src/main.py").stat().st_mode & 0o777 == 0o755
    assert (destination / "readme-link").readlink().as_posix() == "README.md"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "expected_code"),
    [
        (".env", "snapshot_path_protected"),
        (".GIT/config", "snapshot_metadata_protected"),
        ("../escape", "snapshot_path_invalid"),
        ("/absolute", "snapshot_path_invalid"),
        ("not/./canonical", "snapshot_path_invalid"),
    ],
)
async def test_validation_rejects_protected_or_noncanonical_paths(
    path: str,
    expected_code: str,
) -> None:
    content = _archive((path, "file", b"value", 0o644))

    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(content)).validate(_snapshot(content))

    assert failure.value.code == expected_code


@pytest.mark.asyncio
async def test_allowlist_never_bypasses_content_secret_detection() -> None:
    private_key = b"-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n"
    content = _archive(("config/.env", "file", private_key, 0o600))
    policy = WorkspaceAccessPolicy(allowlisted_files=["config/.env"])

    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(content), access_policy=policy).validate(
            _snapshot(content)
        )

    assert failure.value.code == "snapshot_secret_detected"


@pytest.mark.asyncio
async def test_validation_rejects_symlink_escape_and_protected_target() -> None:
    escaping = _archive(("link", "symlink", "../outside", 0o777))
    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(escaping)).validate(_snapshot(escaping))
    assert failure.value.code == "snapshot_symlink_escape"

    protected = _archive(("link", "symlink", ".env", 0o777))
    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(protected)).validate(_snapshot(protected))
    assert failure.value.code == "snapshot_path_protected"


@pytest.mark.asyncio
async def test_validation_rejects_casefold_duplicates_and_hardlinks() -> None:
    duplicate = _archive(
        ("Readme", "file", b"one", 0o644),
        ("README", "file", b"two", 0o644),
    )
    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(duplicate)).validate(_snapshot(duplicate))
    assert failure.value.code == "snapshot_duplicate_path"

    hardlink = _archive(
        ("target", "file", b"one", 0o644),
        ("link", "hardlink", "target", 0o644),
    )
    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(hardlink)).validate(_snapshot(hardlink))
    assert failure.value.code == "snapshot_entry_unsupported"


@pytest.mark.asyncio
async def test_validation_enforces_entry_file_and_expanded_limits() -> None:
    two_files = _archive(
        ("one", "file", b"1", 0o644),
        ("two", "file", b"2", 0o644),
    )
    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(two_files), max_entries=1).validate(
            _snapshot(two_files)
        )
    assert failure.value.code == "snapshot_entry_limit"

    large_file = _archive(("large", "file", b"large", 0o644))
    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(large_file), max_file_bytes=4).validate(
            _snapshot(large_file)
        )
    assert failure.value.code == "snapshot_file_limit"

    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(large_file), max_expanded_bytes=4).validate(
            _snapshot(large_file)
        )
    assert failure.value.code == "snapshot_expanded_limit"


@pytest.mark.asyncio
async def test_validation_rejects_malformed_archive() -> None:
    content = b"not-a-tar-archive"

    with pytest.raises(DomainOperationError) as failure:
        await SnapshotValidator(_ObjectStore(content)).validate(_snapshot(content))

    assert failure.value.code == "snapshot_archive_invalid"


@pytest.mark.asyncio
async def test_validation_worker_completes_durable_snapshot_and_artifact() -> None:
    content = _archive(("README.md", "file", b"hello\n", 0o644))
    snapshot = _snapshot(content)
    repository = _ValidationRepository(snapshot)
    artifact_id = uuid.UUID("00000000-0000-0000-0000-000000000099")
    worker = SnapshotValidationWorker(
        repository,
        SnapshotValidator(_ObjectStore(content)),
        worker_id="validator-1",
        clock=lambda: NOW,
        id_factory=lambda: artifact_id,
    )

    assert await worker.run_once() is True
    assert repository.completed is not None
    ready, artifact = repository.completed
    assert ready.status is SourceSnapshotStatus.READY
    assert ready.artifact_id == artifact_id
    assert ready.entry_count == 1
    assert artifact.id == artifact_id
    assert artifact.object.sha256 == snapshot.expected_sha256
    assert await worker.run_once() is False


@pytest.mark.asyncio
async def test_validation_worker_durably_rejects_policy_failure() -> None:
    content = _archive((".env", "file", b"secret", 0o600))
    repository = _ValidationRepository(_snapshot(content))
    worker = SnapshotValidationWorker(
        repository,
        SnapshotValidator(_ObjectStore(content)),
        worker_id="validator-1",
        clock=lambda: NOW,
    )

    assert await worker.run_once() is True
    assert repository.rejected is not None
    assert repository.rejected.status is SourceSnapshotStatus.REJECTED
    assert repository.rejected.error is not None
    assert repository.rejected.error.code == "snapshot_path_protected"


class _RetryableValidator(SnapshotValidator):
    async def validate(self, snapshot: SourceSnapshot) -> SnapshotValidationResult:
        del snapshot
        raise DomainOperationError(
            code="artifact_download_failed",
            message="artifact storage operation failed",
            retryable=True,
        )


@pytest.mark.asyncio
async def test_validation_worker_releases_retryable_failure() -> None:
    content = _archive(("README.md", "file", b"hello", 0o644))
    repository = _ValidationRepository(_snapshot(content))
    worker = SnapshotValidationWorker(
        repository,
        _RetryableValidator(_ObjectStore(content)),
        worker_id="validator-1",
        clock=lambda: NOW,
    )

    assert await worker.run_once() is True
    assert repository.released is True
    assert repository.rejected is None
