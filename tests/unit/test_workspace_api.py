from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from fastapi.testclient import TestClient

from agent_api import ApiServices, Principal, StaticTokenAuthenticator, create_app
from agent_core.artifacts import (
    Artifact,
    ArtifactKind,
    ObjectStat,
    PresignedDownload,
    PresignedUpload,
    SourceSnapshot,
    SourceSnapshotStatus,
    StoredObject,
    Workspace,
    WorkspaceStatus,
)
from agent_core.control import (
    ApprovalDecision,
    PersistedApproval,
    RunCreationResult,
    RunSubmission,
)
from agent_core.event_store import EventPage

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from agent_core.artifacts import MediaType, ObjectKey
    from agent_core.domain.base import AwareTimestamp
    from agent_core.domain.models import Run, Session, Sha256Hex
    from agent_core.event_store import StoredEvent

TENANT_A = uuid.UUID("00000000-0000-0000-0000-000000000001")
TENANT_B = uuid.UUID("00000000-0000-0000-0000-000000000002")
TOKEN_A = "workspace-token-a"  # noqa: S105 - inert test credential
TOKEN_B = "workspace-token-b"  # noqa: S105 - inert test credential
NOW = datetime(2026, 8, 20, tzinfo=UTC)


class _Sessions:
    def __init__(self) -> None:
        self.values: dict[tuple[uuid.UUID, uuid.UUID], Session] = {}

    async def create(self, session: Session) -> Session:
        self.values[(session.tenant_id, session.id)] = session
        return session

    async def get(self, tenant_id: uuid.UUID, session_id: uuid.UUID) -> Session | None:
        return self.values.get((tenant_id, session_id))


class _Runs:
    async def create_idempotent(
        self,
        tenant_id: uuid.UUID,
        run: Run,
        *,
        idempotency_key: str,
        creation_hash: str,
    ) -> RunCreationResult:
        del tenant_id, idempotency_key, creation_hash
        return RunCreationResult(run=run, created=True)

    async def create_submission(
        self,
        tenant_id: uuid.UUID,
        submission: RunSubmission,
        *,
        idempotency_key: str,
        creation_hash: str,
    ) -> RunCreationResult:
        return await self.create_idempotent(
            tenant_id,
            submission.run,
            idempotency_key=idempotency_key,
            creation_hash=creation_hash,
        )

    async def get(self, tenant_id: uuid.UUID, run_id: uuid.UUID) -> Run | None:
        del tenant_id, run_id
        return None

    async def request_cancel(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        occurred_at: datetime,
    ) -> Run | None:
        del tenant_id, run_id, occurred_at
        return None

    async def rewind(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        checkpoint_id: uuid.UUID,
    ) -> Run | None:
        del tenant_id, run_id, checkpoint_id
        return None


class _Approvals:
    async def decide(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        approval_id: uuid.UUID,
        decision: ApprovalDecision,
    ) -> PersistedApproval | None:
        del tenant_id, run_id, approval_id, decision
        return None


class _Events:
    async def read_page(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        limit: int = 100,
    ) -> EventPage:
        del tenant_id, run_id, limit
        return EventPage(events=(), next_after=after, has_more=False)

    async def stream(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        after: int = 0,
        page_size: int = 100,
    ) -> AsyncIterator[StoredEvent]:
        del tenant_id, run_id, after, page_size
        events: list[StoredEvent] = []
        for event in events:
            yield event


class _Readiness:
    async def ready(self) -> bool:
        return True


class _Workspaces:
    def __init__(self) -> None:
        self.workspaces: dict[tuple[uuid.UUID, uuid.UUID], Workspace] = {}
        self.snapshots: dict[tuple[uuid.UUID, uuid.UUID, uuid.UUID], SourceSnapshot] = {}
        self.artifacts: dict[tuple[uuid.UUID, uuid.UUID], Artifact] = {}
        self.validation_jobs: list[uuid.UUID] = []

    async def create(self, workspace: Workspace) -> Workspace:
        self.workspaces[(workspace.tenant_id, workspace.id)] = workspace
        return workspace

    async def get(self, tenant_id: uuid.UUID, workspace_id: uuid.UUID) -> Workspace | None:
        return self.workspaces.get((tenant_id, workspace_id))

    async def create_snapshot(self, snapshot: SourceSnapshot) -> SourceSnapshot:
        self.snapshots[(snapshot.tenant_id, snapshot.workspace_id, snapshot.id)] = snapshot
        return snapshot

    async def get_snapshot(
        self,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID,
        snapshot_id: uuid.UUID,
    ) -> SourceSnapshot | None:
        return self.snapshots.get((tenant_id, workspace_id, snapshot_id))

    async def begin_validation(
        self,
        snapshot: SourceSnapshot,
        *,
        job_id: uuid.UUID,
    ) -> SourceSnapshot:
        key = (snapshot.tenant_id, snapshot.workspace_id, snapshot.id)
        current = self.snapshots[key]
        if current.status is SourceSnapshotStatus.VALIDATING:
            return current
        self.snapshots[key] = snapshot
        self.validation_jobs.append(job_id)
        return snapshot

    async def list_artifacts(
        self,
        tenant_id: uuid.UUID,
        workspace_id: uuid.UUID,
        *,
        run_id: uuid.UUID | None = None,
        limit: int = 100,
    ) -> tuple[Artifact, ...]:
        values = tuple(
            artifact
            for (stored_tenant, _), artifact in self.artifacts.items()
            if stored_tenant == tenant_id
            and artifact.workspace_id == workspace_id
            and (run_id is None or artifact.run_id == run_id)
        )
        return values[:limit]

    async def get_artifact(
        self,
        tenant_id: uuid.UUID,
        artifact_id: uuid.UUID,
    ) -> Artifact | None:
        return self.artifacts.get((tenant_id, artifact_id))


class _Objects:
    def __init__(self) -> None:
        self.stats: dict[str, ObjectStat] = {}

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
        return PresignedUpload(
            object_key=object_key,
            url="https://objects.invalid/upload",
            content_type=content_type,
            headers={"x-amz-meta-max-bytes": str(max_bytes)},
            expires_at=expires_at,
        )

    async def create_download(
        self,
        *,
        object_key: ObjectKey,
        expires_at: AwareTimestamp,
    ) -> PresignedDownload:
        return PresignedDownload(
            url=f"https://objects.invalid/download/{object_key}",
            expires_at=expires_at,
        )

    async def head(self, object_key: ObjectKey) -> ObjectStat | None:
        return self.stats.get(object_key)

    async def download_to_path(
        self,
        object_key: ObjectKey,
        destination: Path,
        *,
        max_bytes: int,
        expected_sha256: Sha256Hex,
    ) -> StoredObject:
        raise NotImplementedError

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


def _application() -> tuple[TestClient, _Workspaces, _Objects, _Sessions]:
    workspaces = _Workspaces()
    objects = _Objects()
    sessions = _Sessions()
    services = ApiServices(
        authenticator=StaticTokenAuthenticator(
            {
                TOKEN_A: Principal(tenant_id=TENANT_A, subject="user-a"),
                TOKEN_B: Principal(tenant_id=TENANT_B, subject="user-b"),
            }
        ),
        sessions=sessions,
        runs=_Runs(),
        approvals=_Approvals(),
        events=_Events(),
        readiness=_Readiness(),
        workspaces=workspaces,
        object_store=objects,
    )
    return TestClient(create_app(services)), workspaces, objects, sessions


def _authorization(token: str = TOKEN_A) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_workspace_upload_and_finalize_protocol_is_tenant_scoped() -> None:
    client, workspaces, objects, _ = _application()
    created = client.post(
        "/v1/workspaces",
        headers=_authorization(),
        json={"display_name": "sample"},
    )
    assert created.status_code == 201
    workspace_id = uuid.UUID(created.json()["id"])

    uploaded = client.post(
        f"/v1/workspaces/{workspace_id}/snapshots",
        headers=_authorization(),
    )
    assert uploaded.status_code == 201
    upload_body = uploaded.json()
    snapshot_id = uuid.UUID(upload_body["snapshot"]["id"])
    object_key = upload_body["snapshot"]["object_key"]
    assert upload_body["upload"]["headers"]["x-amz-meta-max-bytes"] == str(256 * 1024 * 1024)
    assert ".git" not in object_key.casefold()

    hidden = client.get(
        f"/v1/workspaces/{workspace_id}",
        headers=_authorization(TOKEN_B),
    )
    assert hidden.status_code == 404

    missing = client.post(
        f"/v1/workspaces/{workspace_id}/snapshots/{snapshot_id}/finalize",
        headers=_authorization(),
        json={"sha256": "a" * 64, "compressed_bytes": 100},
    )
    assert missing.status_code == 409
    assert missing.json()["error"]["code"] == "snapshot_upload_in_progress"

    objects.stats[object_key] = ObjectStat(
        object_key=object_key,
        size_bytes=100,
        content_type="application/gzip",
    )
    finalized = client.post(
        f"/v1/workspaces/{workspace_id}/snapshots/{snapshot_id}/finalize",
        headers=_authorization(),
        json={"sha256": "a" * 64, "compressed_bytes": 100},
    )
    assert finalized.status_code == 202
    assert finalized.json()["status"] == "validating"
    assert len(workspaces.validation_jobs) == 1

    repeated = client.post(
        f"/v1/workspaces/{workspace_id}/snapshots/{snapshot_id}/finalize",
        headers=_authorization(),
        json={"sha256": "a" * 64, "compressed_bytes": 100},
    )
    assert repeated.status_code == 202
    assert len(workspaces.validation_jobs) == 1


def test_sessions_require_ready_tenant_workspace() -> None:
    client, workspaces, _, sessions = _application()
    created = client.post(
        "/v1/workspaces",
        headers=_authorization(),
        json={"display_name": "sample"},
    ).json()
    workspace_id = uuid.UUID(created["id"])

    pending = client.post(
        "/v1/sessions",
        headers=_authorization(),
        json={"workspace_id": str(workspace_id)},
    )
    assert pending.status_code == 400
    assert pending.json()["error"]["code"] == "workspace_not_ready"

    current = workspaces.workspaces[(TENANT_A, workspace_id)]
    workspaces.workspaces[(TENANT_A, workspace_id)] = current.model_copy(
        update={
            "status": WorkspaceStatus.READY,
            "current_snapshot_id": uuid.uuid4(),
            "version": 1,
            "updated_at": current.updated_at,
        }
    )
    ready = client.post(
        "/v1/sessions",
        headers=_authorization(),
        json={"workspace_id": str(workspace_id)},
    )
    assert ready.status_code == 201
    assert len(sessions.values) == 1


def test_artifact_listing_and_download_are_tenant_scoped() -> None:
    client, workspaces, _, _ = _application()
    workspace = Workspace(
        id=uuid.uuid4(),
        tenant_id=TENANT_A,
        status=WorkspaceStatus.READY,
        display_name="sample",
        current_snapshot_id=uuid.uuid4(),
        version=1,
        created_at=NOW,
        updated_at=NOW,
    )
    workspaces.workspaces[(TENANT_A, workspace.id)] = workspace
    artifact = Artifact(
        id=uuid.uuid4(),
        tenant_id=TENANT_A,
        workspace_id=workspace.id,
        kind=ArtifactKind.FINAL_PATCH,
        object=StoredObject(
            object_key=f"tenants/{TENANT_A.hex}/final.patch",
            sha256="b" * 64,
            size_bytes=10,
            content_type="text/plain",
        ),
        created_at=NOW,
    )
    workspaces.artifacts[(TENANT_A, artifact.id)] = artifact

    listed = client.get(
        f"/v1/workspaces/{workspace.id}/artifacts",
        headers=_authorization(),
    )
    assert listed.status_code == 200
    assert listed.json()["artifacts"][0]["id"] == str(artifact.id)

    downloaded = client.get(
        f"/v1/artifacts/{artifact.id}/download",
        headers=_authorization(),
    )
    assert downloaded.status_code == 200
    assert downloaded.json()["artifact"]["object"]["sha256"] == "b" * 64
    assert downloaded.json()["download"]["url"].endswith("final.patch")

    hidden = client.get(
        f"/v1/artifacts/{artifact.id}/download",
        headers=_authorization(TOKEN_B),
    )
    assert hidden.status_code == 404
