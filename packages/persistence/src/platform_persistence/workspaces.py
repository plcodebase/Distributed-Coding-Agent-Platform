"""Tenant-scoped immutable workspace and artifact persistence."""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

from sqlalchemy import and_, or_, select

from agent_core.artifacts import (
    MAX_SNAPSHOT_VALIDATION_ATTEMPTS,
    MAX_SNAPSHOT_VALIDATION_LEASE_SECONDS,
    MAX_VALIDATION_WORKER_ID_BYTES,
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
from platform_persistence.fencing import assert_active_run_lease
from platform_persistence.models import (
    ArtifactRecord,
    RunRecord,
    SnapshotValidationJobRecord,
    SourceSnapshotRecord,
    WorkspaceRecord,
)

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from agent_core.distributed import RunLease

MAX_ARTIFACT_PAGE_SIZE = 500


class PostgresWorkspaceRepository:
    """Durable workspace heads over checksum-verified immutable artifacts."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def create(self, workspace: Workspace) -> Workspace:
        if workspace.status is not WorkspaceStatus.PENDING:
            raise DomainOperationError(
                code="workspace_creation_invalid",
                message="a new workspace must be pending",
            )
        async with self._sessions() as database, database.begin():
            database.add(_workspace_record(workspace))
        return workspace

    async def get(self, tenant_id: uuid.UUID, workspace_id: uuid.UUID) -> Workspace | None:
        async with self._sessions() as database:
            row = await database.scalar(
                select(WorkspaceRecord).where(
                    WorkspaceRecord.tenant_id == tenant_id,
                    WorkspaceRecord.id == workspace_id,
                )
            )
        return _workspace_domain(row) if row is not None else None

    async def create_snapshot(self, snapshot: SourceSnapshot) -> SourceSnapshot:
        if snapshot.status is not SourceSnapshotStatus.PENDING:
            raise DomainOperationError(
                code="snapshot_creation_invalid",
                message="a new source snapshot must be pending",
            )
        expected_key = source_snapshot_object_key(
            snapshot.tenant_id,
            snapshot.workspace_id,
            snapshot.id,
        )
        if snapshot.object_key != expected_key:
            raise DomainOperationError(
                code="snapshot_object_key_invalid",
                message="the source snapshot object key is not platform-owned",
            )
        async with self._sessions() as database, database.begin():
            await self._require_workspace(database, snapshot.tenant_id, snapshot.workspace_id)
            database.add(_snapshot_record(snapshot))
        return snapshot

    async def get_snapshot(
        self,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID,
        snapshot_id: uuid.UUID,
    ) -> SourceSnapshot | None:
        async with self._sessions() as database:
            row = await database.scalar(
                select(SourceSnapshotRecord).where(
                    SourceSnapshotRecord.tenant_id == tenant_id,
                    SourceSnapshotRecord.workspace_id == workspace_id,
                    SourceSnapshotRecord.id == snapshot_id,
                )
            )
        return _snapshot_domain(row) if row is not None else None

    async def begin_validation(
        self,
        snapshot: SourceSnapshot,
        *,
        job_id: uuid.UUID,
    ) -> SourceSnapshot:
        if snapshot.status is not SourceSnapshotStatus.VALIDATING:
            raise DomainOperationError(
                code="snapshot_validation_invalid",
                message="snapshot validation requires validating state",
            )
        async with self._sessions() as database, database.begin():
            workspace = await self._workspace_for_update(
                database,
                snapshot.tenant_id,
                snapshot.workspace_id,
            )
            row = await self._snapshot_for_update(database, snapshot)
            if row.status == SourceSnapshotStatus.VALIDATING.value:
                existing = _snapshot_domain(row)
                if existing == snapshot:
                    return existing
                raise _snapshot_state_conflict(snapshot, row.status)
            if row.status != SourceSnapshotStatus.PENDING.value:
                raise _snapshot_state_conflict(snapshot, row.status)
            _apply_snapshot(row, snapshot)
            database.add(
                SnapshotValidationJobRecord(
                    id=job_id,
                    tenant_id=snapshot.tenant_id,
                    workspace_id=snapshot.workspace_id,
                    snapshot_id=snapshot.id,
                    status="pending",
                    expected_workspace_version=workspace.version,
                    attempt=1,
                    worker_id=None,
                    lease_token=None,
                    lease_generation=0,
                    lease_expires_at=None,
                    created_at=snapshot.updated_at,
                    started_at=None,
                    completed_at=None,
                )
            )
        return snapshot

    async def complete_validation(
        self,
        snapshot: SourceSnapshot,
        artifact: Artifact,
        *,
        lease: SnapshotValidationLease,
    ) -> Workspace:
        _validate_completed_snapshot(snapshot, artifact)
        _validate_lease_identity(lease, snapshot)
        async with self._sessions() as database, database.begin():
            workspace = await self._workspace_for_update(
                database,
                snapshot.tenant_id,
                snapshot.workspace_id,
            )
            if workspace.version != lease.expected_workspace_version:
                raise DomainOperationError(
                    code="workspace_version_conflict",
                    message="the workspace head changed before snapshot validation completed",
                    details={
                        "workspace_id": str(snapshot.workspace_id),
                        "expected_version": lease.expected_workspace_version,
                        "actual_version": workspace.version,
                    },
                )
            row = await self._snapshot_for_update(database, snapshot)
            if row.status != SourceSnapshotStatus.VALIDATING.value:
                raise _snapshot_state_conflict(snapshot, row.status)
            job = await self._validation_job_for_update(database, snapshot)
            _assert_validation_lease(job, lease, occurred_at=snapshot.updated_at)
            database.add(_artifact_record(artifact))
            _apply_snapshot(row, snapshot)
            workspace.status = WorkspaceStatus.READY.value
            workspace.current_snapshot_id = snapshot.id
            workspace.version += 1
            workspace.updated_at = snapshot.updated_at
            _finish_validation_job(job, status="completed", completed_at=snapshot.updated_at)
            return _workspace_domain(workspace)

    async def reject_validation(
        self,
        snapshot: SourceSnapshot,
        *,
        lease: SnapshotValidationLease,
    ) -> SourceSnapshot:
        if snapshot.status is not SourceSnapshotStatus.REJECTED:
            raise DomainOperationError(
                code="snapshot_rejection_invalid",
                message="snapshot rejection requires rejected state",
            )
        _validate_lease_identity(lease, snapshot)
        async with self._sessions() as database, database.begin():
            row = await self._snapshot_for_update(database, snapshot)
            if row.status != SourceSnapshotStatus.VALIDATING.value:
                raise _snapshot_state_conflict(snapshot, row.status)
            job = await self._validation_job_for_update(database, snapshot)
            _assert_validation_lease(job, lease, occurred_at=snapshot.updated_at)
            _apply_snapshot(row, snapshot)
            _finish_validation_job(job, status="failed", completed_at=snapshot.updated_at)
        return snapshot

    async def claim_validation_job(
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        lease_seconds: int,
    ) -> tuple[SnapshotValidationLease, SourceSnapshot] | None:
        if not worker_id or len(worker_id.encode("utf-8")) > MAX_VALIDATION_WORKER_ID_BYTES:
            raise ValueError("worker_id must be nonempty bounded text")
        if (
            type(lease_seconds) is not int
            or not 1 <= lease_seconds <= MAX_SNAPSHOT_VALIDATION_LEASE_SECONDS
        ):
            raise ValueError("lease_seconds must be an integer in [1, 300]")
        async with self._sessions() as database, database.begin():
            job = await database.scalar(
                select(SnapshotValidationJobRecord)
                .where(
                    or_(
                        SnapshotValidationJobRecord.status == "pending",
                        and_(
                            SnapshotValidationJobRecord.status == "running",
                            SnapshotValidationJobRecord.lease_expires_at <= occurred_at,
                        ),
                    ),
                    or_(
                        SnapshotValidationJobRecord.lease_generation == 0,
                        SnapshotValidationJobRecord.attempt < MAX_SNAPSHOT_VALIDATION_ATTEMPTS,
                    ),
                )
                .order_by(SnapshotValidationJobRecord.created_at, SnapshotValidationJobRecord.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if job is None:
                return None
            if job.lease_generation > 0:
                if job.attempt >= MAX_SNAPSHOT_VALIDATION_ATTEMPTS:
                    return None
                job.attempt += 1
            token = uuid.uuid4()
            job.status = "running"
            job.worker_id = worker_id
            job.lease_token = token
            job.lease_generation += 1
            job.started_at = job.started_at or occurred_at
            job.lease_expires_at = occurred_at + timedelta(seconds=lease_seconds)
            snapshot_row = await database.scalar(
                select(SourceSnapshotRecord).where(
                    SourceSnapshotRecord.tenant_id == job.tenant_id,
                    SourceSnapshotRecord.workspace_id == job.workspace_id,
                    SourceSnapshotRecord.id == job.snapshot_id,
                )
            )
            if snapshot_row is None:
                raise DomainOperationError(
                    code="snapshot_not_found",
                    message="source snapshot was not found",
                    details={"snapshot_id": str(job.snapshot_id)},
                )
            lease = SnapshotValidationLease(
                job_id=job.id,
                tenant_id=job.tenant_id,
                workspace_id=job.workspace_id,
                snapshot_id=job.snapshot_id,
                expected_workspace_version=job.expected_workspace_version,
                worker_id=worker_id,
                lease_token=token,
                lease_generation=job.lease_generation,
                attempt=job.attempt,
                expires_at=job.lease_expires_at,
            )
            return lease, _snapshot_domain(snapshot_row)

    async def release_validation(
        self,
        lease: SnapshotValidationLease,
        *,
        occurred_at: datetime,
    ) -> None:
        async with self._sessions() as database, database.begin():
            job = await database.scalar(
                select(SnapshotValidationJobRecord)
                .where(
                    SnapshotValidationJobRecord.tenant_id == lease.tenant_id,
                    SnapshotValidationJobRecord.id == lease.job_id,
                )
                .with_for_update()
            )
            if job is None:
                raise DomainOperationError(
                    code="snapshot_validation_job_missing",
                    message="source snapshot validation job was not found",
                )
            _assert_validation_lease(job, lease, occurred_at=occurred_at)
            job.status = "pending"
            job.worker_id = None
            job.lease_token = None
            job.lease_expires_at = None

    async def list_artifacts(
        self,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID,
        *,
        run_id: uuid.UUID | None = None,
        limit: int = 100,
    ) -> tuple[Artifact, ...]:
        if limit < 1 or limit > MAX_ARTIFACT_PAGE_SIZE:
            raise DomainOperationError(
                code="artifact_page_invalid",
                message="artifact page size must be between 1 and 500",
            )
        statement = select(ArtifactRecord).where(
            ArtifactRecord.tenant_id == tenant_id,
            ArtifactRecord.workspace_id == workspace_id,
        )
        if run_id is not None:
            statement = statement.join(
                RunRecord,
                (RunRecord.tenant_id == ArtifactRecord.tenant_id)
                & (RunRecord.id == ArtifactRecord.run_id),
            ).where(
                ArtifactRecord.run_id == run_id,
                ArtifactRecord.execution_epoch == RunRecord.execution_epoch,
            )
        statement = statement.order_by(ArtifactRecord.created_at.desc(), ArtifactRecord.id).limit(
            limit
        )
        async with self._sessions() as database:
            rows = tuple((await database.scalars(statement)).all())
        return tuple(_artifact_domain(row) for row in rows)

    async def get_artifact(
        self,
        tenant_id: uuid.UUID,
        artifact_id: uuid.UUID,
    ) -> Artifact | None:
        async with self._sessions() as database:
            row = await database.scalar(
                select(ArtifactRecord).where(
                    ArtifactRecord.tenant_id == tenant_id,
                    ArtifactRecord.id == artifact_id,
                )
            )
        return _artifact_domain(row) if row is not None else None

    async def create_artifact_fenced(self, lease: RunLease, artifact: Artifact) -> Artifact:
        """Persist one immutable run artifact under the exact active execution fence."""

        if (
            artifact.kind is not ArtifactKind.FINAL_PATCH
            or artifact.tenant_id != lease.tenant_id
            or artifact.workspace_id != lease.workspace_id
            or artifact.run_id != lease.run_id
            or artifact.execution_epoch != lease.execution_epoch
        ):
            raise DomainOperationError(
                code="artifact_lease_mismatch",
                message="the final artifact does not belong to the active run lease",
            )
        async with self._sessions() as database, database.begin():
            await assert_active_run_lease(database, lease)
            await self._require_workspace(database, lease.tenant_id, lease.workspace_id)
            existing = await database.scalar(
                select(ArtifactRecord)
                .where(
                    ArtifactRecord.tenant_id == artifact.tenant_id,
                    ArtifactRecord.id == artifact.id,
                )
                .with_for_update()
            )
            if existing is None:
                database.add(_artifact_record(artifact))
                return artifact
            persisted = _artifact_domain(existing)
            if (
                persisted.tenant_id != artifact.tenant_id
                or persisted.workspace_id != artifact.workspace_id
                or persisted.run_id != artifact.run_id
                or persisted.kind is not artifact.kind
                or persisted.object != artifact.object
                or persisted.expires_at != artifact.expires_at
            ):
                raise DomainOperationError(
                    code="artifact_id_conflict",
                    message="the artifact ID belongs to different immutable metadata",
                )
            return persisted

    @staticmethod
    async def _require_workspace(
        database: AsyncSession,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> WorkspaceRecord:
        row = await database.scalar(
            select(WorkspaceRecord).where(
                WorkspaceRecord.tenant_id == tenant_id,
                WorkspaceRecord.id == workspace_id,
            )
        )
        if row is None:
            raise DomainOperationError(
                code="workspace_not_found",
                message="workspace was not found",
                details={"workspace_id": str(workspace_id)},
            )
        return row

    @classmethod
    async def _workspace_for_update(
        cls,
        database: AsyncSession,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> WorkspaceRecord:
        row = await database.scalar(
            select(WorkspaceRecord)
            .where(
                WorkspaceRecord.tenant_id == tenant_id,
                WorkspaceRecord.id == workspace_id,
            )
            .with_for_update()
        )
        if row is None:
            raise DomainOperationError(
                code="workspace_not_found",
                message="workspace was not found",
                details={"workspace_id": str(workspace_id)},
            )
        return row

    @staticmethod
    async def _snapshot_for_update(
        database: AsyncSession,
        snapshot: SourceSnapshot,
    ) -> SourceSnapshotRecord:
        row = await database.scalar(
            select(SourceSnapshotRecord)
            .where(
                SourceSnapshotRecord.tenant_id == snapshot.tenant_id,
                SourceSnapshotRecord.workspace_id == snapshot.workspace_id,
                SourceSnapshotRecord.id == snapshot.id,
            )
            .with_for_update()
        )
        if row is None:
            raise DomainOperationError(
                code="snapshot_not_found",
                message="source snapshot was not found",
                details={"snapshot_id": str(snapshot.id)},
            )
        return row

    @staticmethod
    async def _validation_job_for_update(
        database: AsyncSession,
        snapshot: SourceSnapshot,
    ) -> SnapshotValidationJobRecord:
        row = await database.scalar(
            select(SnapshotValidationJobRecord)
            .where(
                SnapshotValidationJobRecord.tenant_id == snapshot.tenant_id,
                SnapshotValidationJobRecord.snapshot_id == snapshot.id,
            )
            .with_for_update()
        )
        if row is None:
            raise DomainOperationError(
                code="snapshot_validation_job_missing",
                message="source snapshot validation job was not found",
                details={"snapshot_id": str(snapshot.id)},
            )
        return row


def _validate_completed_snapshot(snapshot: SourceSnapshot, artifact: Artifact) -> None:
    if snapshot.status is not SourceSnapshotStatus.READY:
        raise DomainOperationError(
            code="snapshot_completion_invalid",
            message="snapshot completion requires ready state",
        )
    if (
        artifact.kind is not ArtifactKind.SOURCE_SNAPSHOT
        or artifact.id != snapshot.artifact_id
        or artifact.tenant_id != snapshot.tenant_id
        or artifact.workspace_id != snapshot.workspace_id
        or artifact.run_id is not None
        or artifact.object.object_key != snapshot.object_key
        or artifact.object.sha256 != snapshot.expected_sha256
        or artifact.object.size_bytes != snapshot.compressed_bytes
    ):
        raise DomainOperationError(
            code="snapshot_artifact_mismatch",
            message="source snapshot artifact metadata does not match validation",
        )


def _snapshot_state_conflict(snapshot: SourceSnapshot, actual_status: str) -> DomainOperationError:
    return DomainOperationError(
        code="snapshot_state_conflict",
        message="source snapshot is not in the required lifecycle state",
        details={
            "snapshot_id": str(snapshot.id),
            "requested_status": snapshot.status.value,
            "actual_status": actual_status,
        },
    )


def _validate_lease_identity(
    lease: SnapshotValidationLease,
    snapshot: SourceSnapshot,
) -> None:
    if (
        lease.tenant_id != snapshot.tenant_id
        or lease.workspace_id != snapshot.workspace_id
        or lease.snapshot_id != snapshot.id
    ):
        raise DomainOperationError(
            code="snapshot_validation_lease_mismatch",
            message="validation lease does not belong to the source snapshot",
        )


def _assert_validation_lease(
    job: SnapshotValidationJobRecord,
    lease: SnapshotValidationLease,
    *,
    occurred_at: datetime,
) -> None:
    if (
        job.status != "running"
        or job.id != lease.job_id
        or job.worker_id != lease.worker_id
        or job.lease_token != lease.lease_token
        or job.lease_generation != lease.lease_generation
        or job.lease_expires_at is None
        or job.lease_expires_at != lease.expires_at
        or job.lease_expires_at < occurred_at
    ):
        raise DomainOperationError(
            code="snapshot_validation_lease_lost",
            message="source snapshot validation lease is no longer active",
            retryable=True,
            details={
                "snapshot_id": str(lease.snapshot_id),
                "lease_generation": lease.lease_generation,
            },
        )


def _finish_validation_job(
    job: SnapshotValidationJobRecord,
    *,
    status: str,
    completed_at: datetime,
) -> None:
    job.status = status
    job.worker_id = None
    job.lease_token = None
    job.lease_expires_at = None
    job.started_at = job.started_at or completed_at
    job.completed_at = completed_at


def _workspace_record(workspace: Workspace) -> WorkspaceRecord:
    return WorkspaceRecord(
        id=workspace.id,
        tenant_id=workspace.tenant_id,
        status=workspace.status.value,
        display_name=workspace.display_name,
        current_snapshot_id=workspace.current_snapshot_id,
        version=workspace.version,
        created_at=workspace.created_at,
        updated_at=workspace.updated_at,
    )


def _workspace_domain(row: WorkspaceRecord) -> Workspace:
    return Workspace(
        id=row.id,
        tenant_id=row.tenant_id,
        status=WorkspaceStatus(row.status),
        display_name=row.display_name,
        current_snapshot_id=row.current_snapshot_id,
        version=row.version,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _snapshot_record(snapshot: SourceSnapshot) -> SourceSnapshotRecord:
    return SourceSnapshotRecord(
        id=snapshot.id,
        tenant_id=snapshot.tenant_id,
        workspace_id=snapshot.workspace_id,
        status=snapshot.status.value,
        object_key=snapshot.object_key,
        expected_sha256=snapshot.expected_sha256,
        compressed_bytes=snapshot.compressed_bytes,
        artifact_id=snapshot.artifact_id,
        manifest_sha256=snapshot.manifest_sha256,
        entry_count=snapshot.entry_count,
        expanded_bytes=snapshot.expanded_bytes,
        error=snapshot.error.model_dump(mode="json") if snapshot.error is not None else None,
        created_at=snapshot.created_at,
        updated_at=snapshot.updated_at,
    )


def _apply_snapshot(row: SourceSnapshotRecord, snapshot: SourceSnapshot) -> None:
    row.status = snapshot.status.value
    row.expected_sha256 = snapshot.expected_sha256
    row.compressed_bytes = snapshot.compressed_bytes
    row.artifact_id = snapshot.artifact_id
    row.manifest_sha256 = snapshot.manifest_sha256
    row.entry_count = snapshot.entry_count
    row.expanded_bytes = snapshot.expanded_bytes
    row.error = snapshot.error.model_dump(mode="json") if snapshot.error is not None else None
    row.updated_at = snapshot.updated_at


def _snapshot_domain(row: SourceSnapshotRecord) -> SourceSnapshot:
    return SourceSnapshot(
        id=row.id,
        tenant_id=row.tenant_id,
        workspace_id=row.workspace_id,
        status=SourceSnapshotStatus(row.status),
        object_key=row.object_key,
        expected_sha256=row.expected_sha256,
        compressed_bytes=row.compressed_bytes,
        artifact_id=row.artifact_id,
        manifest_sha256=row.manifest_sha256,
        entry_count=row.entry_count,
        expanded_bytes=row.expanded_bytes,
        error=ErrorDetail.model_validate(row.error) if row.error is not None else None,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _artifact_record(artifact: Artifact) -> ArtifactRecord:
    return ArtifactRecord(
        id=artifact.id,
        tenant_id=artifact.tenant_id,
        workspace_id=artifact.workspace_id,
        run_id=artifact.run_id,
        execution_epoch=artifact.execution_epoch,
        kind=artifact.kind.value,
        object_key=artifact.object.object_key,
        sha256=artifact.object.sha256,
        size_bytes=artifact.object.size_bytes,
        content_type=artifact.object.content_type,
        etag=artifact.object.etag,
        created_at=artifact.created_at,
        expires_at=artifact.expires_at,
    )


def _artifact_domain(row: ArtifactRecord) -> Artifact:
    return Artifact(
        id=row.id,
        tenant_id=row.tenant_id,
        workspace_id=row.workspace_id,
        run_id=row.run_id,
        execution_epoch=row.execution_epoch,
        kind=ArtifactKind(row.kind),
        object=StoredObject(
            object_key=row.object_key,
            sha256=row.sha256,
            size_bytes=row.size_bytes,
            content_type=row.content_type,
            etag=row.etag,
        ),
        created_at=row.created_at,
        expires_at=row.expires_at,
    )


__all__ = ["PostgresWorkspaceRepository", "source_snapshot_object_key"]
