from __future__ import annotations

import uuid
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import Any, Self, cast

import pytest

from agent_core.artifacts import (
    Artifact,
    ArtifactKind,
    SnapshotValidationLease,
    SourceSnapshot,
    SourceSnapshotStatus,
    StoredObject,
    Workspace,
    WorkspaceStatus,
    source_snapshot_object_key,
)
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from platform_persistence.models import SnapshotValidationJobRecord
from platform_persistence.workspaces import (
    PostgresWorkspaceRepository,
    _apply_snapshot,
    _artifact_domain,
    _artifact_record,
    _snapshot_domain,
    _snapshot_record,
    _workspace_domain,
    _workspace_record,
)

NOW = datetime(2026, 8, 20, tzinfo=UTC)
TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
WORKSPACE_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
SNAPSHOT_ID = uuid.UUID("30000000-0000-0000-0000-000000000003")
ARTIFACT_ID = uuid.UUID("40000000-0000-0000-0000-000000000004")
JOB_ID = uuid.UUID("50000000-0000-0000-0000-000000000005")
LEASE_TOKEN = uuid.UUID("60000000-0000-0000-0000-000000000006")
SHA256 = "a" * 64


class _ScalarResult:
    def __init__(self, values: list[object]) -> None:
        self._values = values

    def all(self) -> list[object]:
        return self._values


class _Database:
    def __init__(
        self,
        *,
        scalars: list[object] | None = None,
        scalar_sets: list[list[object]] | None = None,
    ) -> None:
        self.scalar_values = deque(scalars or [])
        self.scalar_sets = deque(scalar_sets or [])
        self.added: list[object] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def begin(self) -> Self:
        return self

    async def scalar(self, _statement: object) -> object:
        if not self.scalar_values:
            raise AssertionError("unexpected scalar query")
        return self.scalar_values.popleft()

    async def scalars(self, _statement: object) -> _ScalarResult:
        if not self.scalar_sets:
            raise AssertionError("unexpected scalar-set query")
        return _ScalarResult(self.scalar_sets.popleft())

    def add(self, value: object) -> None:
        self.added.append(value)


class _Sessions:
    def __init__(self, *databases: _Database) -> None:
        self._databases = deque(databases)

    def __call__(self) -> _Database:
        return self._databases.popleft()


def _repository(*databases: _Database) -> PostgresWorkspaceRepository:
    return PostgresWorkspaceRepository(cast("Any", _Sessions(*databases)))


def _workspace() -> Workspace:
    return Workspace(
        id=WORKSPACE_ID,
        tenant_id=TENANT_ID,
        status=WorkspaceStatus.PENDING,
        display_name="repository",
        created_at=NOW,
        updated_at=NOW,
    )


def _pending_snapshot() -> SourceSnapshot:
    return SourceSnapshot(
        id=SNAPSHOT_ID,
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        status=SourceSnapshotStatus.PENDING,
        object_key=source_snapshot_object_key(TENANT_ID, WORKSPACE_ID, SNAPSHOT_ID),
        created_at=NOW,
        updated_at=NOW,
    )


def _validating_snapshot() -> SourceSnapshot:
    return _pending_snapshot().model_copy(
        update={
            "status": SourceSnapshotStatus.VALIDATING,
            "expected_sha256": SHA256,
            "compressed_bytes": 123,
            "updated_at": NOW + timedelta(seconds=1),
        }
    )


def _ready_snapshot() -> SourceSnapshot:
    return _validating_snapshot().model_copy(
        update={
            "status": SourceSnapshotStatus.READY,
            "artifact_id": ARTIFACT_ID,
            "manifest_sha256": SHA256,
            "entry_count": 2,
            "expanded_bytes": 10,
            "updated_at": NOW + timedelta(seconds=2),
        }
    )


def _artifact() -> Artifact:
    snapshot = _ready_snapshot()
    return Artifact(
        id=ARTIFACT_ID,
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        kind=ArtifactKind.SOURCE_SNAPSHOT,
        object=StoredObject(
            object_key=snapshot.object_key,
            sha256=SHA256,
            size_bytes=123,
            content_type="application/gzip",
            etag="etag",
        ),
        created_at=snapshot.updated_at,
    )


def _job(workspace_version: int = 0) -> SnapshotValidationJobRecord:
    return SnapshotValidationJobRecord(
        id=JOB_ID,
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        snapshot_id=SNAPSHOT_ID,
        status="running",
        expected_workspace_version=workspace_version,
        attempt=1,
        worker_id="validator-1",
        lease_token=LEASE_TOKEN,
        lease_generation=1,
        lease_expires_at=NOW + timedelta(minutes=1),
        created_at=NOW,
        started_at=NOW,
    )


def _lease() -> SnapshotValidationLease:
    return SnapshotValidationLease(
        job_id=JOB_ID,
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        snapshot_id=SNAPSHOT_ID,
        expected_workspace_version=0,
        worker_id="validator-1",
        lease_token=LEASE_TOKEN,
        lease_generation=1,
        attempt=1,
        expires_at=NOW + timedelta(minutes=1),
    )


@pytest.mark.asyncio
async def test_workspace_and_snapshot_crud_round_trip() -> None:
    workspace = _workspace()
    create_db = _Database()
    repository = _repository(
        create_db,
        _Database(scalars=[_workspace_record(workspace)]),
        _Database(scalars=[_workspace_record(workspace)]),
        _Database(scalars=[_snapshot_record(_pending_snapshot())]),
    )

    assert await repository.create(workspace) == workspace
    assert _workspace_domain(cast("Any", create_db.added[0])) == workspace
    assert await repository.get(TENANT_ID, WORKSPACE_ID) == workspace
    assert await repository.create_snapshot(_pending_snapshot()) == _pending_snapshot()
    assert (
        await repository.get_snapshot(TENANT_ID, WORKSPACE_ID, SNAPSHOT_ID) == _pending_snapshot()
    )

    with pytest.raises(DomainOperationError, match="new workspace"):
        await _repository().create(
            workspace.model_copy(
                update={
                    "status": WorkspaceStatus.READY,
                    "current_snapshot_id": SNAPSHOT_ID,
                }
            )
        )
    with pytest.raises(DomainOperationError) as wrong_key:
        await _repository().create_snapshot(
            _pending_snapshot().model_copy(update={"object_key": "tenants/wrong/source.tar.gz"})
        )
    assert wrong_key.value.code == "snapshot_object_key_invalid"


@pytest.mark.asyncio
async def test_validation_lifecycle_advances_workspace_and_job() -> None:
    workspace_row = _workspace_record(_workspace())
    snapshot_row = _snapshot_record(_pending_snapshot())
    begin_db = _Database(scalars=[workspace_row, snapshot_row])
    repository = _repository(begin_db)
    validating = _validating_snapshot()

    assert await repository.begin_validation(validating, job_id=JOB_ID) == validating
    assert snapshot_row.status == SourceSnapshotStatus.VALIDATING.value
    assert isinstance(begin_db.added[0], SnapshotValidationJobRecord)

    job = _job()
    complete_db = _Database(scalars=[workspace_row, snapshot_row, job])
    completed = await _repository(complete_db).complete_validation(
        _ready_snapshot(),
        _artifact(),
        lease=_lease(),
    )
    assert completed.status is WorkspaceStatus.READY
    assert completed.current_snapshot_id == SNAPSHOT_ID
    assert completed.version == 1
    assert job.status == "completed" and job.worker_id is None
    assert _artifact_domain(cast("Any", complete_db.added[0])) == _artifact()


@pytest.mark.asyncio
async def test_validation_claim_release_rejection_and_artifact_reads() -> None:
    snapshot_row = _snapshot_record(_validating_snapshot())
    pending_job = _job()
    pending_job.status = "pending"
    pending_job.worker_id = None
    pending_job.lease_token = None
    pending_job.lease_generation = 0
    pending_job.lease_expires_at = None
    pending_job.started_at = None
    claimed = await _repository(
        _Database(scalars=[pending_job, snapshot_row])
    ).claim_validation_job("validator-1", occurred_at=NOW, lease_seconds=30)
    assert claimed is not None
    lease, snapshot = claimed
    assert snapshot == _validating_snapshot()
    assert lease.lease_generation == 1

    release_db = _Database(scalars=[pending_job])
    await _repository(release_db).release_validation(lease, occurred_at=NOW)
    assert pending_job.status == "pending" and pending_job.worker_id is None

    pending_job.status = "running"
    pending_job.worker_id = "validator-1"
    pending_job.lease_token = lease.lease_token
    pending_job.lease_expires_at = lease.expires_at
    rejected = _validating_snapshot().model_copy(
        update={
            "status": SourceSnapshotStatus.REJECTED,
            "error": ErrorDetail(code="snapshot_invalid", message="invalid"),
            "updated_at": NOW + timedelta(seconds=2),
        }
    )
    reject_db = _Database(scalars=[snapshot_row, pending_job])
    assert await _repository(reject_db).reject_validation(rejected, lease=lease) == rejected
    assert pending_job.status == "failed"

    artifact_row = _artifact_record(_artifact())
    repository = _repository(
        _Database(scalar_sets=[[artifact_row]]),
        _Database(scalars=[artifact_row]),
    )
    assert await repository.list_artifacts(TENANT_ID, WORKSPACE_ID) == (_artifact(),)
    assert await repository.get_artifact(TENANT_ID, ARTIFACT_ID) == _artifact()


@pytest.mark.asyncio
async def test_workspace_repository_fail_closed_paths() -> None:
    assert await _repository(_Database(scalars=[None])).get(TENANT_ID, WORKSPACE_ID) is None
    assert (
        await _repository(_Database(scalars=[None])).get_snapshot(
            TENANT_ID, WORKSPACE_ID, SNAPSHOT_ID
        )
        is None
    )
    assert (
        await _repository(_Database(scalars=[None])).claim_validation_job(
            "validator-1", occurred_at=NOW, lease_seconds=30
        )
        is None
    )
    with pytest.raises(ValueError, match="worker_id"):
        await _repository().claim_validation_job("", occurred_at=NOW, lease_seconds=30)
    with pytest.raises(ValueError, match="lease_seconds"):
        await _repository().claim_validation_job("validator", occurred_at=NOW, lease_seconds=0)
    with pytest.raises(DomainOperationError) as page:
        await _repository().list_artifacts(TENANT_ID, WORKSPACE_ID, limit=0)
    assert page.value.code == "artifact_page_invalid"

    with pytest.raises(DomainOperationError) as missing_workspace:
        await _repository(_Database(scalars=[None])).create_snapshot(_pending_snapshot())
    assert missing_workspace.value.code == "workspace_not_found"


def test_workspace_record_helpers_preserve_closed_domain_contracts() -> None:
    workspace = _workspace()
    assert _workspace_domain(_workspace_record(workspace)) == workspace
    snapshot = _ready_snapshot()
    assert _snapshot_domain(_snapshot_record(snapshot)) == snapshot
    artifact = _artifact()
    assert _artifact_domain(_artifact_record(artifact)) == artifact

    target = _snapshot_record(_pending_snapshot())
    _apply_snapshot(target, snapshot)
    assert _snapshot_domain(target) == snapshot
