"""Provider-neutral immutable workspace and artifact contracts."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves identifiers at runtime
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Protocol, Self
from urllib.parse import parse_qs, quote, unquote, urlsplit

from pydantic import AfterValidator, Field, StringConstraints, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel, FrozenJsonObject
from agent_core.domain.errors import ErrorDetail  # noqa: TC001 - Pydantic runtime field
from agent_core.domain.models import Sha256Hex  # noqa: TC001 - Pydantic runtime field

MAX_SNAPSHOT_VALIDATION_ATTEMPTS = 100
MAX_SNAPSHOT_VALIDATION_LEASE_SECONDS = 300
MAX_VALIDATION_WORKER_ID_BYTES = 255

if TYPE_CHECKING:
    from pathlib import Path


def _validate_object_key(value: str) -> str:
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError("object key must contain only canonical path components")
    return value


type ObjectKey = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=1024,
        pattern=r"^[a-z0-9][a-z0-9._/-]*$",
    ),
    AfterValidator(_validate_object_key),
]
type MediaType = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=255,
        pattern=r"^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*$",
    ),
]


class WorkspaceStatus(StrEnum):
    """Lifecycle of a logical tenant workspace."""

    PENDING = "pending"
    READY = "ready"
    ARCHIVED = "archived"


class SourceSnapshotStatus(StrEnum):
    """Validation lifecycle for an immutable repository upload."""

    PENDING = "pending"
    VALIDATING = "validating"
    READY = "ready"
    REJECTED = "rejected"


class ArtifactKind(StrEnum):
    """Closed artifact categories used for policy and retention."""

    SOURCE_SNAPSHOT = "source_snapshot"
    WORKSPACE_CHECKPOINT = "workspace_checkpoint"
    FINAL_PATCH = "final_patch"
    COMMAND_LOG = "command_log"
    EVALUATION_REPORT = "evaluation_report"


class PresignedUpload(DomainModel):
    """Short-lived direct upload instruction without storage credentials."""

    object_key: ObjectKey
    url: Annotated[str, StringConstraints(min_length=1, max_length=8192)]
    method: Annotated[str, StringConstraints(pattern=r"^PUT$")] = "PUT"
    content_type: MediaType
    headers: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    expires_at: AwareTimestamp


class PresignedDownload(DomainModel):
    """Short-lived artifact download instruction without storage credentials."""

    url: Annotated[str, StringConstraints(min_length=1, max_length=8192)]
    expires_at: AwareTimestamp


class ObjectStat(DomainModel):
    """Unverified object metadata available before content hashing."""

    object_key: ObjectKey
    size_bytes: int = Field(ge=0)
    content_type: MediaType
    etag: Annotated[str, StringConstraints(min_length=1, max_length=1024)] | None = None


class StoredObject(DomainModel):
    """Verified immutable object metadata."""

    object_key: ObjectKey
    sha256: Sha256Hex
    size_bytes: int = Field(ge=0)
    content_type: MediaType
    etag: Annotated[str, StringConstraints(min_length=1, max_length=1024)] | None = None


class DurableSnapshotReference(DomainModel):
    """Checksum-bound reference to a private workspace snapshot object."""

    object_key: ObjectKey
    sha256: Sha256Hex
    size_bytes: int = Field(ge=1, le=256 * 1024 * 1024)

    def to_uri(self) -> str:
        """Encode the reference without exposing object-store credentials."""

        key = quote(self.object_key, safe="/")
        return f"artifact:///{key}?sha256={self.sha256}&size_bytes={self.size_bytes}"

    @classmethod
    def from_uri(cls, value: str) -> Self:
        """Parse the platform-owned durable snapshot URI or fail closed."""

        parsed = urlsplit(value)
        query = parse_qs(parsed.query, strict_parsing=True)
        if (
            parsed.scheme != "artifact"
            or parsed.netloc
            or not parsed.path.startswith("/")
            or parsed.fragment
            or set(query) != {"sha256", "size_bytes"}
            or any(len(items) != 1 for items in query.values())
        ):
            raise ValueError("invalid durable snapshot URI")
        key = unquote(parsed.path[1:])
        if quote(key, safe="/") != parsed.path[1:]:
            raise ValueError("durable snapshot URI is not canonically encoded")
        try:
            size_bytes = int(query["size_bytes"][0])
        except ValueError:
            raise ValueError("durable snapshot size is invalid") from None
        return cls(
            object_key=key,
            sha256=query["sha256"][0],
            size_bytes=size_bytes,
        )


class Artifact(DomainModel):
    """Tenant-owned immutable artifact persisted outside PostgreSQL."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    workspace_id: uuid.UUID
    run_id: uuid.UUID | None = None
    execution_epoch: int = Field(default=1, ge=1)
    kind: ArtifactKind
    object: StoredObject
    created_at: AwareTimestamp
    expires_at: AwareTimestamp | None = None

    @model_validator(mode="after")
    def validate_retention(self) -> Self:
        if self.run_id is None and self.execution_epoch != 1:
            raise ValueError("only run-owned artifacts may use a later execution epoch")
        if self.expires_at is not None and self.expires_at <= self.created_at:
            raise ValueError("artifact expiry must follow creation")
        return self


class Workspace(DomainModel):
    """Logical mutable head over immutable repository snapshots."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    status: WorkspaceStatus
    display_name: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ]
    current_snapshot_id: uuid.UUID | None = None
    version: int = Field(default=0, ge=0)
    created_at: AwareTimestamp
    updated_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("workspace update may not precede creation")
        if self.status is WorkspaceStatus.PENDING and self.current_snapshot_id is not None:
            raise ValueError("pending workspace may not have a current snapshot")
        if self.status is WorkspaceStatus.READY and self.current_snapshot_id is None:
            raise ValueError("ready workspace requires a current snapshot")
        return self


class SourceSnapshot(DomainModel):
    """Immutable source upload plus its fail-closed validation outcome."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    workspace_id: uuid.UUID
    status: SourceSnapshotStatus
    object_key: ObjectKey
    expected_sha256: Sha256Hex | None = None
    compressed_bytes: int | None = Field(default=None, ge=0)
    artifact_id: uuid.UUID | None = None
    manifest_sha256: Sha256Hex | None = None
    entry_count: int | None = Field(default=None, ge=0)
    expanded_bytes: int | None = Field(default=None, ge=0)
    error: ErrorDetail | None = None
    created_at: AwareTimestamp
    updated_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.updated_at < self.created_at:
            raise ValueError("snapshot update may not precede creation")
        upload = (self.expected_sha256, self.compressed_bytes)
        result = (
            self.artifact_id,
            self.manifest_sha256,
            self.entry_count,
            self.expanded_bytes,
        )
        if self.status is SourceSnapshotStatus.PENDING:
            if any(value is not None for value in (*upload, *result, self.error)):
                raise ValueError("pending snapshot may not contain validation state")
        elif self.status is SourceSnapshotStatus.VALIDATING:
            if any(value is None for value in upload) or any(value is not None for value in result):
                raise ValueError("validating snapshot requires upload metadata only")
            if self.error is not None:
                raise ValueError("validating snapshot may not contain an error")
        elif self.status is SourceSnapshotStatus.READY:
            if any(value is None for value in (*upload, *result)) or self.error is not None:
                raise ValueError("ready snapshot requires complete validated metadata")
        elif (
            any(value is None for value in upload)
            or self.error is None
            or any(value is not None for value in result)
        ):
            raise ValueError("rejected snapshot requires only upload metadata and an error")
        return self


class SnapshotValidationLease(DomainModel):
    """Fenced ownership of one source-snapshot validation job."""

    job_id: uuid.UUID
    tenant_id: uuid.UUID
    workspace_id: uuid.UUID
    snapshot_id: uuid.UUID
    expected_workspace_version: int = Field(ge=0)
    worker_id: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ]
    lease_token: uuid.UUID
    lease_generation: int = Field(ge=1)
    attempt: int = Field(ge=1, le=100)
    expires_at: AwareTimestamp


class SnapshotValidationRepository(Protocol):
    """Durable fenced queue and completion boundary for snapshot validation."""

    async def claim_validation_job(
        self,
        worker_id: str,
        *,
        occurred_at: AwareTimestamp,
        lease_seconds: int,
    ) -> tuple[SnapshotValidationLease, SourceSnapshot] | None: ...

    async def complete_validation(
        self,
        snapshot: SourceSnapshot,
        artifact: Artifact,
        *,
        lease: SnapshotValidationLease,
    ) -> Workspace: ...

    async def reject_validation(
        self,
        snapshot: SourceSnapshot,
        *,
        lease: SnapshotValidationLease,
    ) -> SourceSnapshot: ...

    async def release_validation(
        self,
        lease: SnapshotValidationLease,
        *,
        occurred_at: AwareTimestamp,
    ) -> None: ...


class ObjectStore(Protocol):
    """Storage boundary for immutable, checksum-verified platform objects."""

    async def ready(self) -> bool: ...

    async def create_upload(
        self,
        *,
        object_key: ObjectKey,
        content_type: MediaType,
        max_bytes: int,
        expires_at: AwareTimestamp,
    ) -> PresignedUpload: ...

    async def create_download(
        self,
        *,
        object_key: ObjectKey,
        expires_at: AwareTimestamp,
    ) -> PresignedDownload: ...

    async def head(self, object_key: ObjectKey) -> ObjectStat | None: ...

    async def download_to_path(
        self,
        object_key: ObjectKey,
        destination: Path,
        *,
        max_bytes: int,
        expected_sha256: Sha256Hex,
    ) -> StoredObject: ...

    async def upload_from_path(
        self,
        object_key: ObjectKey,
        source: Path,
        *,
        content_type: MediaType,
        max_bytes: int,
    ) -> StoredObject: ...

    async def delete(self, object_key: ObjectKey) -> None: ...

    async def aclose(self) -> None: ...


__all__ = [
    "MAX_SNAPSHOT_VALIDATION_ATTEMPTS",
    "MAX_SNAPSHOT_VALIDATION_LEASE_SECONDS",
    "MAX_VALIDATION_WORKER_ID_BYTES",
    "Artifact",
    "ArtifactKind",
    "DurableSnapshotReference",
    "MediaType",
    "ObjectKey",
    "ObjectStat",
    "ObjectStore",
    "PresignedDownload",
    "PresignedUpload",
    "SnapshotValidationLease",
    "SnapshotValidationRepository",
    "SourceSnapshot",
    "SourceSnapshotStatus",
    "StoredObject",
    "Workspace",
    "WorkspaceStatus",
    "final_patch_object_key",
    "source_snapshot_object_key",
    "workspace_checkpoint_object_key",
]


def source_snapshot_object_key(
    tenant_id: uuid.UUID,
    workspace_id: uuid.UUID,
    snapshot_id: uuid.UUID,
) -> ObjectKey:
    """Return the canonical platform-owned key for a source upload."""

    return (
        f"tenants/{tenant_id.hex}/workspaces/{workspace_id.hex}/"
        f"snapshots/{snapshot_id.hex}/source.tar.gz"
    )


def workspace_checkpoint_object_key(
    tenant_id: uuid.UUID,
    workspace_id: uuid.UUID,
    run_id: uuid.UUID,
    checkpoint_id: uuid.UUID,
) -> ObjectKey:
    """Return the canonical immutable key for one pre-tool checkpoint archive."""

    return (
        f"tenants/{tenant_id.hex}/workspaces/{workspace_id.hex}/runs/{run_id.hex}/"
        f"checkpoints/{checkpoint_id.hex}/workspace.tar.gz"
    )


def final_patch_object_key(
    tenant_id: uuid.UUID,
    workspace_id: uuid.UUID,
    run_id: uuid.UUID,
    execution_epoch: int = 1,
) -> ObjectKey:
    """Return the immutable final-patch key owned by one execution branch."""

    if type(execution_epoch) is not int or execution_epoch < 1:
        raise ValueError("execution_epoch must be a positive integer")

    prefix = f"tenants/{tenant_id.hex}/workspaces/{workspace_id.hex}/runs/{run_id.hex}/"
    if execution_epoch == 1:
        return f"{prefix}artifacts/final.patch"
    return f"{prefix}branches/{execution_epoch}/artifacts/final.patch"
