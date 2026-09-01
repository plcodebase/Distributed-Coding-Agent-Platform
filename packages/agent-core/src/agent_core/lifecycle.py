"""Provider-neutral tenant lifecycle, retention, and compliance contracts."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves identifiers at runtime
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Protocol, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.artifacts import ObjectKey, StoredObject  # noqa: TC001 - Pydantic runtime fields
from agent_core.audit import AuditEntry
from agent_core.domain.base import AwareTimestamp, DomainModel
from agent_core.domain.errors import ErrorDetail  # noqa: TC001 - Pydantic runtime field

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

MAX_LIFECYCLE_ATTEMPTS = 100
MAX_LIFECYCLE_BATCH_SIZE = 1_000
MAX_AUDIT_EXPORT_RECORDS = 10_000_000
MAX_AUDIT_EXPORT_BYTES = 1024 * 1024 * 1024

type LifecycleActor = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
]
type LifecycleReason = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=2_000),
]


class TenantLifecycleStatus(StrEnum):
    """Fail-closed tenant access and deletion lifecycle."""

    ACTIVE = "active"
    DELETION_REQUESTED = "deletion_requested"
    DELETING = "deleting"
    DELETED = "deleted"


class AuditExportStatus(StrEnum):
    """Immutable audit-export lifecycle."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class ObjectDeletionReason(StrEnum):
    """Closed reasons for deleting an immutable object."""

    RETENTION_EXPIRED = "retention_expired"
    TENANT_DELETION = "tenant_deletion"


class TenantLifecycle(DomainModel):
    """Durable tenant access state; absence in storage means active."""

    tenant_id: uuid.UUID
    status: TenantLifecycleStatus
    request_id: uuid.UUID | None = None
    requested_by: LifecycleActor | None = None
    requested_at: AwareTimestamp | None = None
    delete_after: AwareTimestamp | None = None
    audit_export_id: uuid.UUID | None = None
    deletion_started_at: AwareTimestamp | None = None
    deleted_at: AwareTimestamp | None = None
    updated_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        request = (
            self.request_id,
            self.requested_by,
            self.requested_at,
            self.delete_after,
            self.audit_export_id,
        )
        if self.status is TenantLifecycleStatus.ACTIVE:
            if any(
                value is not None for value in (*request, self.deletion_started_at, self.deleted_at)
            ):
                raise ValueError("active tenant lifecycle may not contain deletion state")
            return self
        if (
            self.request_id is None
            or self.requested_by is None
            or self.requested_at is None
            or self.delete_after is None
            or self.audit_export_id is None
        ):
            raise ValueError("tenant deletion requires request and audit-export state")
        if self.delete_after < self.requested_at:
            raise ValueError("tenant deletion cooling-off time may not precede its request")
        if self.updated_at < self.requested_at:
            raise ValueError("tenant lifecycle update may not precede its request")
        if self.status is TenantLifecycleStatus.DELETION_REQUESTED:
            if self.deletion_started_at is not None or self.deleted_at is not None:
                raise ValueError("requested tenant deletion may not contain completion state")
        elif self.status is TenantLifecycleStatus.DELETING:
            if self.deletion_started_at is None or self.deleted_at is not None:
                raise ValueError("deleting tenant requires only a deletion start time")
            if self.deletion_started_at < self.delete_after:
                raise ValueError("tenant deletion may not start before cooling-off completes")
        elif self.deletion_started_at is None or self.deleted_at is None:
            raise ValueError("deleted tenant requires start and completion times")
        elif self.deleted_at < self.deletion_started_at:
            raise ValueError("tenant deletion completion may not precede its start")
        return self


class LegalHold(DomainModel):
    """Tenant-wide preservation hold that blocks retention and deletion."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    reason: LifecycleReason
    placed_by: LifecycleActor
    placed_at: AwareTimestamp
    expires_at: AwareTimestamp | None = None
    released_by: LifecycleActor | None = None
    released_at: AwareTimestamp | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.expires_at is not None and self.expires_at <= self.placed_at:
            raise ValueError("legal-hold expiry must follow placement")
        if (self.released_by is None) != (self.released_at is None):
            raise ValueError("legal-hold release actor and timestamp must be recorded together")
        if self.released_at is not None and self.released_at < self.placed_at:
            raise ValueError("legal-hold release may not precede placement")
        return self


class AuditExportEntry(AuditEntry):
    """Canonical append-only audit row included in an export."""


class AuditExport(DomainModel):
    """Checksum-bound compliance export retained outside tenant application data."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    status: AuditExportStatus
    requested_by: LifecycleActor
    cutoff_at: AwareTimestamp
    created_at: AwareTimestamp
    object: StoredObject | None = None
    record_count: int | None = Field(default=None, ge=0, le=MAX_AUDIT_EXPORT_RECORDS)
    first_occurred_at: AwareTimestamp | None = None
    last_occurred_at: AwareTimestamp | None = None
    completed_at: AwareTimestamp | None = None
    error: ErrorDetail | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        completed = (self.object, self.record_count, self.completed_at)
        if self.status is AuditExportStatus.PENDING:
            if any(value is not None for value in (*completed, self.error)):
                raise ValueError("pending audit export may not contain an outcome")
        elif self.status is AuditExportStatus.COMPLETED:
            if (
                self.object is None
                or self.record_count is None
                or self.completed_at is None
                or self.error is not None
            ):
                raise ValueError("completed audit export requires immutable object metadata")
            if self.record_count == 0:
                if self.first_occurred_at is not None or self.last_occurred_at is not None:
                    raise ValueError("empty audit export may not contain a timestamp range")
            elif self.first_occurred_at is None or self.last_occurred_at is None:
                raise ValueError("nonempty audit export requires a timestamp range")
        elif any(value is not None for value in completed) or self.error is None:
            raise ValueError("failed audit export requires only a structured error")
        if self.completed_at is not None and self.completed_at < self.created_at:
            raise ValueError("audit export completion may not precede creation")
        if self.last_occurred_at is not None and self.last_occurred_at > self.cutoff_at:
            raise ValueError("audit export may not include records after its cutoff")
        if (
            self.first_occurred_at is not None
            and self.last_occurred_at is not None
            and self.last_occurred_at < self.first_occurred_at
        ):
            raise ValueError("audit export timestamp range is invalid")
        return self


class ObjectDeletionLease(DomainModel):
    """Fenced ownership of one idempotent object deletion."""

    job_id: uuid.UUID
    tenant_id: uuid.UUID
    artifact_id: uuid.UUID | None = None
    object_key: ObjectKey
    reason: ObjectDeletionReason
    worker_id: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ]
    lease_token: uuid.UUID
    lease_generation: int = Field(ge=1)
    attempt: int = Field(ge=1, le=MAX_LIFECYCLE_ATTEMPTS)
    expires_at: AwareTimestamp


class LifecycleBatchResult(DomainModel):
    """Bounded administrative batch outcome without sensitive object contents."""

    claimed: int = Field(ge=0, le=MAX_LIFECYCLE_BATCH_SIZE)
    completed: int = Field(ge=0, le=MAX_LIFECYCLE_BATCH_SIZE)
    failed: int = Field(ge=0, le=MAX_LIFECYCLE_BATCH_SIZE)

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.completed + self.failed > self.claimed:
            raise ValueError("lifecycle outcomes may not exceed claimed jobs")
        return self


class TenantAccessPolicy(Protocol):
    """Fail-closed asynchronous tenant-access policy."""

    async def require_active(self, tenant_id: uuid.UUID) -> None: ...


class LifecycleRepository(TenantAccessPolicy, Protocol):
    """Durable tenant lifecycle and cleanup boundary."""

    async def get_tenant_lifecycle(self, tenant_id: uuid.UUID) -> TenantLifecycle | None: ...

    async def get_audit_export(
        self,
        tenant_id: uuid.UUID,
        export_id: uuid.UUID,
    ) -> AuditExport | None: ...

    async def get_legal_hold(
        self,
        tenant_id: uuid.UUID,
        hold_id: uuid.UUID,
    ) -> LegalHold | None: ...

    async def has_active_legal_hold(
        self,
        tenant_id: uuid.UUID,
        *,
        occurred_at: AwareTimestamp,
    ) -> bool: ...

    def iter_audit_entries(
        self,
        tenant_id: uuid.UUID,
        *,
        cutoff_at: AwareTimestamp,
        batch_size: int,
    ) -> AsyncIterator[tuple[AuditExportEntry, ...]]: ...

    async def claim_object_deletions(
        self,
        worker_id: str,
        *,
        occurred_at: AwareTimestamp,
        lease_seconds: int,
        limit: int,
    ) -> tuple[ObjectDeletionLease, ...]: ...

    async def complete_object_deletion(self, lease: ObjectDeletionLease) -> None: ...

    async def fail_object_deletion(
        self,
        lease: ObjectDeletionLease,
        *,
        error: ErrorDetail,
        occurred_at: AwareTimestamp,
    ) -> None: ...


class LifecycleAdministrationRepository(LifecycleRepository, Protocol):
    """Privileged mutation boundary used only by trusted lifecycle jobs."""

    async def create_audit_export(self, export: AuditExport) -> AuditExport: ...

    async def complete_audit_export(self, export: AuditExport) -> AuditExport: ...

    async def fail_audit_export(
        self,
        export_id: uuid.UUID,
        tenant_id: uuid.UUID,
        *,
        error: ErrorDetail,
    ) -> AuditExport: ...

    async def place_legal_hold(self, hold: LegalHold) -> LegalHold: ...

    async def release_legal_hold(
        self,
        tenant_id: uuid.UUID,
        hold_id: uuid.UUID,
        *,
        released_by: LifecycleActor,
        released_at: AwareTimestamp,
    ) -> LegalHold: ...

    async def request_tenant_deletion(
        self,
        tenant_id: uuid.UUID,
        *,
        request_id: uuid.UUID,
        requested_by: LifecycleActor,
        requested_at: AwareTimestamp,
        delete_after: AwareTimestamp,
        audit_export_id: uuid.UUID,
    ) -> TenantLifecycle: ...

    async def begin_tenant_deletion(
        self,
        tenant_id: uuid.UUID,
        *,
        occurred_at: AwareTimestamp,
    ) -> TenantLifecycle: ...

    async def enqueue_expired_artifacts(
        self,
        *,
        occurred_at: AwareTimestamp,
        limit: int,
    ) -> int: ...

    async def enqueue_tenant_objects(self, tenant_id: uuid.UUID, *, limit: int) -> int: ...

    async def finalize_tenant_deletion(
        self,
        tenant_id: uuid.UUID,
        *,
        occurred_at: AwareTimestamp,
    ) -> TenantLifecycle: ...


def tenant_audit_export_object_key(tenant_id: uuid.UUID, export_id: uuid.UUID) -> ObjectKey:
    """Return a compliance namespace key that survives tenant application-data deletion."""

    return f"compliance/tenants/{tenant_id.hex}/audit/{export_id.hex}.jsonl"


__all__ = [
    "MAX_AUDIT_EXPORT_BYTES",
    "MAX_AUDIT_EXPORT_RECORDS",
    "MAX_LIFECYCLE_ATTEMPTS",
    "MAX_LIFECYCLE_BATCH_SIZE",
    "AuditExport",
    "AuditExportEntry",
    "AuditExportStatus",
    "LegalHold",
    "LifecycleAdministrationRepository",
    "LifecycleBatchResult",
    "LifecycleRepository",
    "ObjectDeletionLease",
    "ObjectDeletionReason",
    "TenantAccessPolicy",
    "TenantLifecycle",
    "TenantLifecycleStatus",
    "tenant_audit_export_object_key",
]
