from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from agent_core.artifacts import (
    Artifact,
    ArtifactKind,
    PresignedUpload,
    SourceSnapshot,
    SourceSnapshotStatus,
    StoredObject,
    Workspace,
    WorkspaceStatus,
    final_patch_object_key,
)
from agent_core.domain.errors import ErrorDetail

NOW = datetime(2026, 8, 20, tzinfo=UTC)
TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
WORKSPACE_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")
SNAPSHOT_ID = uuid.UUID("00000000-0000-0000-0000-000000000003")
ARTIFACT_ID = uuid.UUID("00000000-0000-0000-0000-000000000004")
SHA256 = "a" * 64


def test_final_patch_keys_are_execution_branch_specific() -> None:
    run_id = uuid.uuid4()
    initial = final_patch_object_key(TENANT_ID, WORKSPACE_ID, run_id)
    rewound = final_patch_object_key(TENANT_ID, WORKSPACE_ID, run_id, 2)

    assert initial.endswith("/artifacts/final.patch")
    assert rewound.endswith("/branches/2/artifacts/final.patch")
    assert initial != rewound
    with pytest.raises(ValueError):
        final_patch_object_key(TENANT_ID, WORKSPACE_ID, run_id, 0)


def test_artifact_models_round_trip_through_ordinary_json() -> None:
    stored = StoredObject(
        object_key="tenants/one/workspaces/two/source.tar.gz",
        sha256=SHA256,
        size_bytes=123,
        content_type="application/gzip",
        etag="etag",
    )
    artifact = Artifact(
        id=ARTIFACT_ID,
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        kind=ArtifactKind.SOURCE_SNAPSHOT,
        object=stored,
        created_at=NOW,
        expires_at=NOW + timedelta(days=1),
    )

    wire = json.loads(artifact.model_dump_json())

    assert isinstance(wire["object"], dict)
    assert Artifact.model_validate(wire) == artifact


@pytest.mark.parametrize(
    "object_key",
    ["/absolute", "upper/Case", "double//separator", "dot/./entry", "parent/../entry"],
)
def test_object_keys_must_be_canonical(object_key: str) -> None:
    with pytest.raises(ValidationError):
        PresignedUpload(
            object_key=object_key,
            url="https://objects.invalid/upload",
            content_type="application/gzip",
            expires_at=NOW,
        )


def test_workspace_lifecycle_requires_ready_snapshot() -> None:
    with pytest.raises(ValidationError, match="ready workspace requires"):
        Workspace(
            id=WORKSPACE_ID,
            tenant_id=TENANT_ID,
            status=WorkspaceStatus.READY,
            display_name="repo",
            created_at=NOW,
            updated_at=NOW,
        )

    workspace = Workspace(
        id=WORKSPACE_ID,
        tenant_id=TENANT_ID,
        status=WorkspaceStatus.READY,
        display_name="repo",
        current_snapshot_id=SNAPSHOT_ID,
        version=1,
        created_at=NOW,
        updated_at=NOW,
    )
    assert workspace.current_snapshot_id == SNAPSHOT_ID


def test_source_snapshot_lifecycle_is_closed() -> None:
    pending = SourceSnapshot(
        id=SNAPSHOT_ID,
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        status=SourceSnapshotStatus.PENDING,
        object_key="tenants/one/source.tar.gz",
        created_at=NOW,
        updated_at=NOW,
    )
    validating = pending.model_copy(
        update={
            "status": SourceSnapshotStatus.VALIDATING,
            "expected_sha256": SHA256,
            "compressed_bytes": 123,
        }
    )
    ready = validating.model_copy(
        update={
            "status": SourceSnapshotStatus.READY,
            "artifact_id": ARTIFACT_ID,
            "manifest_sha256": SHA256,
            "entry_count": 10,
            "expanded_bytes": 456,
        }
    )

    assert ready.status is SourceSnapshotStatus.READY
    with pytest.raises(ValidationError, match="ready snapshot requires"):
        validating.model_copy(update={"status": SourceSnapshotStatus.READY})


def test_rejected_snapshot_requires_upload_metadata_and_error() -> None:
    with pytest.raises(ValidationError, match="rejected snapshot requires"):
        SourceSnapshot(
            id=SNAPSHOT_ID,
            tenant_id=TENANT_ID,
            workspace_id=WORKSPACE_ID,
            status=SourceSnapshotStatus.REJECTED,
            object_key="tenants/one/source.tar.gz",
            created_at=NOW,
            updated_at=NOW,
        )

    rejected = SourceSnapshot(
        id=SNAPSHOT_ID,
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        status=SourceSnapshotStatus.REJECTED,
        object_key="tenants/one/source.tar.gz",
        expected_sha256=SHA256,
        compressed_bytes=123,
        error=ErrorDetail(code="snapshot_invalid", message="snapshot validation failed"),
        created_at=NOW,
        updated_at=NOW,
    )
    assert rejected.error is not None


def test_artifact_expiry_must_follow_creation() -> None:
    with pytest.raises(ValidationError, match="expiry"):
        Artifact(
            id=ARTIFACT_ID,
            tenant_id=TENANT_ID,
            workspace_id=WORKSPACE_ID,
            kind=ArtifactKind.SOURCE_SNAPSHOT,
            object=StoredObject(
                object_key="tenants/one/source.tar.gz",
                sha256=SHA256,
                size_bytes=123,
                content_type="application/gzip",
            ),
            created_at=NOW,
            expires_at=NOW,
        )
