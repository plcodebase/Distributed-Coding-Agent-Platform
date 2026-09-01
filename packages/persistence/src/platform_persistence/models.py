"""PostgreSQL records for every durable entity in design Phase 5."""

from __future__ import annotations

import datetime  # noqa: TC003 - SQLAlchemy resolves mapped annotations at runtime
import decimal  # noqa: TC003 - SQLAlchemy resolves mapped annotations at runtime
import uuid  # noqa: TC003 - SQLAlchemy resolves mapped annotations at runtime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from platform_persistence.base import Base

_UTC_NOW = text("now()")
_EMPTY_JSON = text("'{}'::jsonb")
_EMPTY_ARRAY = text("'[]'::jsonb")


class WorkspaceRecord(Base):
    """Tenant-owned logical workspace whose head is an immutable snapshot."""

    __tablename__ = "workspaces"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "id", "current_snapshot_id"),
            ("source_snapshots.tenant_id", "source_snapshots.workspace_id", "source_snapshots.id"),
            name="fk_workspaces_current_snapshot",
            use_alter=True,
        ),
        UniqueConstraint("tenant_id", "id"),
        CheckConstraint("status IN ('pending', 'ready', 'archived')", name="status"),
        CheckConstraint(
            "(status = 'pending' AND current_snapshot_id IS NULL AND version = 0) "
            "OR (status = 'ready' AND current_snapshot_id IS NOT NULL AND version >= 1) "
            "OR status = 'archived'",
            name="lifecycle",
        ),
        CheckConstraint("octet_length(display_name) BETWEEN 1 AND 1024", name="display_name_bytes"),
        CheckConstraint("updated_at >= created_at", name="timestamp_order"),
        Index("ix_workspaces_tenant_created", "tenant_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    current_snapshot_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class ArtifactRecord(Base):
    """Immutable tenant artifact stored in an external object store."""

    __tablename__ = "artifacts"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "workspace_id"),
            ("workspaces.tenant_id", "workspaces.id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("tenant_id", "run_id", "workspace_id"),
            ("runs.tenant_id", "runs.id", "runs.workspace_id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "id"),
        UniqueConstraint("tenant_id", "workspace_id", "id"),
        UniqueConstraint("object_key"),
        CheckConstraint(
            "kind IN ('source_snapshot', 'workspace_checkpoint', 'final_patch', "
            "'command_log', 'evaluation_report')",
            name="kind",
        ),
        CheckConstraint("object_key ~ '^[a-z0-9][a-z0-9._/-]*$'", name="object_key"),
        CheckConstraint("sha256 ~ '^[0-9a-f]{64}$'", name="sha256"),
        CheckConstraint("size_bytes >= 0", name="size_bytes"),
        CheckConstraint("execution_epoch >= 1", name="execution_epoch"),
        CheckConstraint("octet_length(content_type) BETWEEN 3 AND 255", name="content_type"),
        CheckConstraint("etag IS NULL OR octet_length(etag) BETWEEN 1 AND 1024", name="etag"),
        CheckConstraint("expires_at IS NULL OR expires_at > created_at", name="retention"),
        Index("ix_artifacts_tenant_workspace_created", "tenant_id", "workspace_id", "created_at"),
        Index("ix_artifacts_tenant_run_created", "tenant_id", "run_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    execution_epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("1"),
    )
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    object_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content_type: Mapped[str] = mapped_column(String(255), nullable=False)
    etag: Mapped[str | None] = mapped_column(String(1024))
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    expires_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class SourceSnapshotRecord(Base):
    """Fail-closed validation state for an uploaded source archive."""

    __tablename__ = "source_snapshots"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "workspace_id"),
            ("workspaces.tenant_id", "workspaces.id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("tenant_id", "workspace_id", "artifact_id"),
            ("artifacts.tenant_id", "artifacts.workspace_id", "artifacts.id"),
        ),
        UniqueConstraint("tenant_id", "workspace_id", "id"),
        UniqueConstraint("object_key"),
        CheckConstraint(
            "status IN ('pending', 'validating', 'ready', 'rejected')",
            name="status",
        ),
        CheckConstraint("object_key ~ '^[a-z0-9][a-z0-9._/-]*$'", name="object_key"),
        CheckConstraint(
            "expected_sha256 IS NULL OR expected_sha256 ~ '^[0-9a-f]{64}$'",
            name="expected_sha256",
        ),
        CheckConstraint(
            "manifest_sha256 IS NULL OR manifest_sha256 ~ '^[0-9a-f]{64}$'",
            name="manifest_sha256",
        ),
        CheckConstraint(
            "compressed_bytes IS NULL OR compressed_bytes >= 0", name="compressed_bytes"
        ),
        CheckConstraint("entry_count IS NULL OR entry_count >= 0", name="entry_count"),
        CheckConstraint("expanded_bytes IS NULL OR expanded_bytes >= 0", name="expanded_bytes"),
        CheckConstraint("error IS NULL OR jsonb_typeof(error) = 'object'", name="error_object"),
        CheckConstraint(
            "(status = 'pending' AND expected_sha256 IS NULL AND compressed_bytes IS NULL "
            "AND artifact_id IS NULL AND manifest_sha256 IS NULL AND entry_count IS NULL "
            "AND expanded_bytes IS NULL AND error IS NULL) "
            "OR (status = 'validating' AND expected_sha256 IS NOT NULL "
            "AND compressed_bytes IS NOT NULL AND artifact_id IS NULL "
            "AND manifest_sha256 IS NULL AND entry_count IS NULL AND expanded_bytes IS NULL "
            "AND error IS NULL) "
            "OR (status = 'ready' AND expected_sha256 IS NOT NULL "
            "AND compressed_bytes IS NOT NULL AND artifact_id IS NOT NULL "
            "AND manifest_sha256 IS NOT NULL AND entry_count IS NOT NULL "
            "AND expanded_bytes IS NOT NULL AND error IS NULL) "
            "OR (status = 'rejected' AND expected_sha256 IS NOT NULL "
            "AND compressed_bytes IS NOT NULL AND artifact_id IS NULL "
            "AND manifest_sha256 IS NULL AND entry_count IS NULL AND expanded_bytes IS NULL "
            "AND error IS NOT NULL)",
            name="lifecycle",
        ),
        CheckConstraint("updated_at >= created_at", name="timestamp_order"),
        Index("ix_source_snapshots_workspace_created", "tenant_id", "workspace_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    object_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    expected_sha256: Mapped[str | None] = mapped_column(String(64))
    compressed_bytes: Mapped[int | None] = mapped_column(BigInteger)
    artifact_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    manifest_sha256: Mapped[str | None] = mapped_column(String(64))
    entry_count: Mapped[int | None] = mapped_column(BigInteger)
    expanded_bytes: Mapped[int | None] = mapped_column(BigInteger)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class SnapshotValidationJobRecord(Base):
    """Retryable validation job for one finalized source upload."""

    __tablename__ = "snapshot_validation_jobs"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "workspace_id", "snapshot_id"),
            ("source_snapshots.tenant_id", "source_snapshots.workspace_id", "source_snapshots.id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "snapshot_id"),
        CheckConstraint("status IN ('pending', 'running', 'completed', 'failed')", name="status"),
        CheckConstraint("attempt >= 1 AND attempt <= 100", name="attempt"),
        CheckConstraint("expected_workspace_version >= 0", name="workspace_version"),
        CheckConstraint("lease_generation >= 0", name="lease_generation"),
        CheckConstraint(
            "(status = 'pending' AND worker_id IS NULL AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND completed_at IS NULL) "
            "OR (status = 'running' AND worker_id IS NOT NULL AND lease_token IS NOT NULL "
            "AND lease_generation >= 1 AND lease_expires_at > started_at "
            "AND started_at IS NOT NULL AND completed_at IS NULL) "
            "OR (status IN ('completed', 'failed') AND worker_id IS NULL AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND started_at IS NOT NULL "
            "AND completed_at >= started_at)",
            name="lifecycle",
        ),
        Index("ix_snapshot_validation_jobs_claim", "status", "lease_expires_at", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    snapshot_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    expected_workspace_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    worker_id: Mapped[str | None] = mapped_column(String(255))
    lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    lease_generation: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    lease_expires_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    started_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class SessionRecord(Base):
    """Durable tenant-owned conversation."""

    __tablename__ = "sessions"
    __table_args__ = (
        UniqueConstraint("tenant_id", "id"),
        UniqueConstraint("tenant_id", "id", "workspace_id"),
        CheckConstraint(
            "status IN ('active', 'completed', 'cancelled')",
            name="status",
        ),
        CheckConstraint(
            "approval_mode IN ('require_all', 'require_sensitive', 'auto_approve')",
            name="approval_mode",
        ),
        CheckConstraint("updated_at >= created_at", name="timestamp_order"),
        Index("ix_sessions_tenant_created", "tenant_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    approval_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    model_route: Mapped[str] = mapped_column(String(255), nullable=False)
    memory_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("true"),
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class RunRecord(Base):
    """Durable run lifecycle plus API idempotency and event allocation state."""

    __tablename__ = "runs"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "session_id", "workspace_id"),
            ("sessions.tenant_id", "sessions.id", "sessions.workspace_id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("tenant_id", "id", "last_checkpoint_id"),
            ("checkpoints.tenant_id", "checkpoints.run_id", "checkpoints.id"),
            name="fk_runs_tenant_id_id_last_checkpoint_id_checkpoints",
            use_alter=True,
        ),
        UniqueConstraint("tenant_id", "id"),
        UniqueConstraint("tenant_id", "id", "session_id"),
        UniqueConstraint("tenant_id", "id", "workspace_id"),
        UniqueConstraint("tenant_id", "session_id", "idempotency_key"),
        CheckConstraint(
            "status IN "
            "('queued', 'leased', 'running', 'waiting_approval', 'retry_pending', "
            "'completed', 'failed', 'cancelled', 'lost')",
            name="status",
        ),
        CheckConstraint("attempt >= 1", name="attempt"),
        CheckConstraint("execution_epoch >= 1", name="execution_epoch"),
        CheckConstraint("execution_epoch <= attempt", name="execution_epoch_attempt"),
        CheckConstraint("priority >= -100 AND priority <= 100", name="priority"),
        CheckConstraint(
            "priority_class IN ('interactive', 'background', 'evaluation')",
            name="priority_class",
        ),
        CheckConstraint("lease_generation >= 0", name="lease_generation"),
        CheckConstraint("next_event_sequence >= 1", name="next_event_sequence"),
        CheckConstraint("creation_hash ~ '^[0-9a-f]{64}$'", name="creation_hash"),
        CheckConstraint(
            "traceparent IS NULL OR (traceparent ~ "
            "'^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$' "
            "AND split_part(traceparent, '-', 2) <> repeat('0', 32) "
            "AND split_part(traceparent, '-', 3) <> repeat('0', 16))",
            name="traceparent",
        ),
        CheckConstraint(
            "tracestate IS NULL OR (traceparent IS NOT NULL AND octet_length(tracestate) "
            "BETWEEN 1 AND 512)",
            name="tracestate",
        ),
        CheckConstraint(
            "started_at IS NULL OR started_at >= created_at",
            name="started_timestamp",
        ),
        CheckConstraint(
            "completed_at IS NULL OR completed_at >= COALESCE(started_at, created_at)",
            name="completed_timestamp",
        ),
        CheckConstraint(
            "status NOT IN "
            "('running', 'waiting_approval', 'retry_pending', 'completed', 'failed') "
            "OR started_at IS NOT NULL",
            name="started_state",
        ),
        CheckConstraint(
            "(status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL) "
            "OR (status NOT IN ('completed', 'failed', 'cancelled') AND completed_at IS NULL)",
            name="terminal_state",
        ),
        CheckConstraint(
            "status NOT IN ('leased', 'running') "
            "OR (assigned_worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="active_lease",
        ),
        CheckConstraint(
            "status NOT IN ('queued', 'waiting_approval', 'retry_pending', 'lost') "
            "OR (assigned_worker_id IS NULL AND lease_expires_at IS NULL)",
            name="suspended_without_lease",
        ),
        CheckConstraint(
            "(status = 'retry_pending') = (retry_ready_at IS NOT NULL)",
            name="retry_ready_state",
        ),
        Index("ix_runs_tenant_session_created", "tenant_id", "session_id", "created_at"),
        Index("ix_runs_status_created", "status", "created_at"),
        Index("ix_runs_retry_ready", "status", "retry_ready_at"),
        Index(
            "ix_runs_queue_claim",
            "status",
            "priority_class",
            text("priority DESC"),
            "created_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    priority_class: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default=text("'interactive'"),
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    execution_epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("1"),
    )
    lease_generation: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    assigned_worker_id: Mapped[str | None] = mapped_column(String(255))
    lease_expires_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    retry_ready_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    last_checkpoint_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    cancellation_requested: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
    traceparent: Mapped[str | None] = mapped_column(String(55))
    tracestate: Mapped[str | None] = mapped_column(String(512))
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    creation_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    next_event_sequence: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("1"),
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    started_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class MessageRecord(Base):
    """Ordered durable conversation message."""

    __tablename__ = "messages"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("tenant_id", "run_id", "session_id"),
            ("runs.tenant_id", "runs.id", "runs.session_id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "session_id", "sequence"),
        CheckConstraint("sequence >= 1", name="sequence"),
        CheckConstraint(
            "role IN ('system', 'user', 'assistant', 'tool')",
            name="role",
        ),
        CheckConstraint("jsonb_typeof(metadata) = 'object'", name="metadata_object"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=_EMPTY_JSON,
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class ContextCompactionRecord(Base):
    """Explicit summary request referencing, but never replacing, source messages."""

    __tablename__ = "context_compactions"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "session_id", "idempotency_key"),
        CheckConstraint("source_message_sequence >= 0", name="source_message_sequence"),
        CheckConstraint(
            "status IN ('pending', 'completed', 'failed')",
            name="status",
        ),
        CheckConstraint(
            "(status = 'pending' AND summary IS NULL AND input_tokens IS NULL "
            "AND output_tokens IS NULL AND error IS NULL AND completed_at IS NULL) "
            "OR (status = 'completed' AND summary IS NOT NULL "
            "AND octet_length(summary) > 0 AND octet_length(summary) <= 262144 "
            "AND input_tokens >= 0 AND output_tokens >= 0 AND error IS NULL "
            "AND completed_at >= requested_at) "
            "OR (status = 'failed' AND summary IS NULL AND input_tokens IS NULL "
            "AND output_tokens IS NULL AND error IS NOT NULL "
            "AND completed_at >= requested_at)",
            name="terminal_outcome",
        ),
        CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="error_object",
        ),
        Index(
            "ix_context_compactions_tenant_session_requested",
            "tenant_id",
            "session_id",
            "requested_at",
        ),
        Index(
            "uq_context_compactions_one_pending",
            "tenant_id",
            "session_id",
            unique=True,
            postgresql_where=text("status = 'pending'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    source_message_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    route_name: Mapped[str] = mapped_column(String(255), nullable=False)
    summary: Mapped[str | None] = mapped_column(Text)
    input_tokens: Mapped[int | None] = mapped_column(BigInteger)
    output_tokens: Mapped[int | None] = mapped_column(BigInteger)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    requested_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class TaskPlanRecord(Base):
    """Versioned durable structured task plan."""

    __tablename__ = "task_plans"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id"),
            ("runs.tenant_id", "runs.id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "run_id", "version"),
        CheckConstraint("version >= 1", name="version"),
        CheckConstraint("jsonb_typeof(plan) = 'object'", name="plan_object"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    plan: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class MemoryRecord(Base):
    """Tenant-owned long-term memory with mandatory source provenance."""

    __tablename__ = "memories"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("tenant_id", "source_run_id", "session_id"),
            ("runs.tenant_id", "runs.id", "runs.session_id"),
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "kind IN ('fact', 'preference', 'decision', 'constraint')",
            name="kind",
        ),
        CheckConstraint(
            "octet_length(content) > 0 AND octet_length(content) <= 65536",
            name="content_bytes",
        ),
        CheckConstraint("content_hash ~ '^[0-9a-f]{64}$'", name="content_hash"),
        CheckConstraint("jsonb_typeof(metadata) = 'object'", name="metadata_object"),
        UniqueConstraint(
            "tenant_id",
            "session_id",
            "kind",
            "content_hash",
            name="uq_memories_memory_identity",
        ),
        CheckConstraint(
            "archived_at IS NULL OR archived_at >= extracted_at",
            name="archive_timestamp",
        ),
        Index(
            "ix_memories_tenant_session_active",
            "tenant_id",
            "session_id",
            "extracted_at",
            postgresql_where=text("archived_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    source_run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    memory_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=_EMPTY_JSON,
    )
    extracted_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    archived_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class MemoryExtractionJobRecord(Base):
    """One idempotent asynchronous extraction job per completed run."""

    __tablename__ = "memory_extraction_jobs"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id", "session_id"),
            ("runs.tenant_id", "runs.id", "runs.session_id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "run_id"),
        CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed')",
            name="status",
        ),
        CheckConstraint("attempt >= 1 AND attempt <= 100", name="attempt"),
        CheckConstraint("lease_generation >= 0", name="lease_generation"),
        CheckConstraint("source_message_sequence >= 0", name="source_message_sequence"),
        CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="error_object",
        ),
        CheckConstraint(
            "(status = 'pending' AND started_at IS NULL AND completed_at IS NULL "
            "AND error IS NULL AND worker_id IS NULL AND lease_token IS NULL "
            "AND lease_generation = 0 AND lease_expires_at IS NULL) "
            "OR (status = 'running' AND started_at >= created_at "
            "AND completed_at IS NULL AND error IS NULL AND worker_id IS NOT NULL "
            "AND lease_token IS NOT NULL AND lease_generation >= 1 "
            "AND lease_expires_at > started_at) "
            "OR (status = 'completed' AND started_at >= created_at "
            "AND completed_at >= started_at AND error IS NULL AND worker_id IS NULL "
            "AND lease_token IS NULL AND lease_expires_at IS NULL) "
            "OR (status = 'failed' AND started_at >= created_at "
            "AND completed_at >= started_at AND error IS NOT NULL AND worker_id IS NULL "
            "AND lease_token IS NULL AND lease_expires_at IS NULL)",
            name="lifecycle",
        ),
        Index("ix_memory_jobs_status_lease_created", "status", "lease_expires_at", "created_at"),
        Index(
            "uq_memory_jobs_active_lease_token",
            "lease_token",
            unique=True,
            postgresql_where=text("lease_token IS NOT NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    source_message_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    worker_id: Mapped[str | None] = mapped_column(String(255))
    lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    lease_generation: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    lease_expires_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    started_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class ToolCallRecord(Base):
    """Stable logical tool call with a unique run-local identity."""

    __tablename__ = "tool_calls"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id"),
            ("runs.tenant_id", "runs.id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "run_id", "tool_call_id"),
        CheckConstraint("turn_number >= 1", name="turn_number"),
        CheckConstraint(
            "status IN "
            "('received', 'waiting_approval', 'running', 'completed', 'failed', 'cancelled')",
            name="status",
        ),
        CheckConstraint("argument_hash ~ '^[0-9a-f]{64}$'", name="argument_hash"),
        CheckConstraint("jsonb_typeof(arguments) = 'object'", name="arguments_object"),
        CheckConstraint(
            "result IS NULL OR jsonb_typeof(result) = 'object'",
            name="result_object",
        ),
        CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="error_object",
        ),
        CheckConstraint(
            "completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at",
            name="timestamp_order",
        ),
        CheckConstraint(
            "status NOT IN ('running', 'completed') OR started_at IS NOT NULL",
            name="started_state",
        ),
        CheckConstraint(
            "(status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL) "
            "OR (status NOT IN ('completed', 'failed', 'cancelled') AND completed_at IS NULL)",
            name="terminal_state",
        ),
        CheckConstraint(
            "(status = 'completed' AND result IS NOT NULL AND error IS NULL) "
            "OR (status IN ('failed', 'cancelled') AND result IS NULL AND error IS NOT NULL) "
            "OR (status NOT IN ('completed', 'failed', 'cancelled') "
            "AND result IS NULL AND error IS NULL)",
            name="terminal_outcome",
        ),
        Index("ix_tool_calls_run_status", "tenant_id", "run_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tool_call_id: Mapped[str] = mapped_column(String(255), nullable=False)
    turn_number: Mapped[int] = mapped_column(Integer, nullable=False)
    tool_name: Mapped[str] = mapped_column(String(100), nullable=False)
    arguments: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    argument_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    workspace_version: Mapped[str | None] = mapped_column(String(255))
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    started_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class ApprovalRecord(Base):
    """Durable human approval request and decision."""

    __tablename__ = "approvals"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id"),
            ("runs.tenant_id", "runs.id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("tenant_id", "run_id", "tool_call_id"),
            (
                "tool_calls.tenant_id",
                "tool_calls.run_id",
                "tool_calls.tool_call_id",
            ),
        ),
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected')",
            name="status",
        ),
        CheckConstraint("jsonb_typeof(arguments) = 'object'", name="arguments_object"),
        CheckConstraint(
            "(status = 'pending' AND decided_by IS NULL AND decided_at IS NULL) "
            "OR (status IN ('approved', 'rejected') "
            "AND decided_by IS NOT NULL AND decided_at IS NOT NULL "
            "AND decided_at >= requested_at)",
            name="decision_state",
        ),
        CheckConstraint(
            "response IS NULL OR octet_length(response) BETWEEN 1 AND 65536",
            name="response_bytes",
        ),
        UniqueConstraint("tenant_id", "run_id", "tool_call_id"),
        Index("ix_approvals_run_status", "tenant_id", "run_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    tool_call_id: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    arguments: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=_EMPTY_JSON,
    )
    decided_by: Mapped[str | None] = mapped_column(String(255))
    requested_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    decided_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    response: Mapped[str | None] = mapped_column(Text)


class CheckpointRecord(Base):
    """Durable checkpoint metadata referencing an immutable workspace snapshot."""

    __tablename__ = "checkpoints"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id", "session_id"),
            ("runs.tenant_id", "runs.id", "runs.session_id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "run_id", "message_sequence"),
        UniqueConstraint("tenant_id", "run_id", "id"),
        CheckConstraint("message_sequence >= 0", name="message_sequence"),
        CheckConstraint("jsonb_typeof(task_plan) = 'object'", name="task_plan_object"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    message_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    workspace_snapshot_uri: Mapped[str] = mapped_column(Text, nullable=False)
    workspace_revision: Mapped[str] = mapped_column(String(255), nullable=False)
    task_plan: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=_EMPTY_JSON,
    )
    context_summary: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class AgentEventRecord(Base):
    """Append-only run event with a unique monotonically allocated sequence."""

    __tablename__ = "agent_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id"),
            ("runs.tenant_id", "runs.id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("run_id", "sequence"),
        UniqueConstraint("run_id", "delivery_key"),
        CheckConstraint("sequence >= 1", name="sequence"),
        CheckConstraint(
            "event_type IN "
            "('run.started', 'context.build_started', 'model.request_started', "
            "'model.text_delta', 'model.tool_call_received', 'tool.approval_required', "
            "'tool.started', 'tool.stdout', 'tool.stderr', 'tool.completed', "
            "'checkpoint.created', 'run.retry_scheduled', 'run.completed', 'run.failed')",
            name="event_type",
        ),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="payload_object"),
        Index("ix_agent_events_tenant_run_sequence", "tenant_id", "run_id", "sequence"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    delivery_key: Mapped[str | None] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class WorkerRecord(Base):
    """Registered execution worker with advertised capacity and liveness."""

    __tablename__ = "workers"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active', 'draining', 'offline')",
            name="status",
        ),
        CheckConstraint("total_slots >= 1 AND total_slots <= 1024", name="total_slots"),
        CheckConstraint(
            "available_slots >= 0 AND available_slots <= total_slots",
            name="available_slots",
        ),
        CheckConstraint(
            "jsonb_typeof(supported_sandbox_types) = 'array'",
            name="sandbox_types_array",
        ),
        CheckConstraint("last_heartbeat_at >= registered_at", name="heartbeat_order"),
        Index("ix_workers_status_heartbeat", "status", "last_heartbeat_at"),
    )

    worker_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    supported_sandbox_types: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    total_slots: Mapped[int] = mapped_column(Integer, nullable=False)
    available_slots: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    registered_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    last_heartbeat_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )


class RunLeaseRecord(Base):
    """Active fenced ownership over one leased or running run."""

    __tablename__ = "run_leases"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id"),
            ("runs.tenant_id", "runs.id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("worker_id",),
            ("workers.worker_id",),
            ondelete="RESTRICT",
        ),
        UniqueConstraint("tenant_id", "run_id"),
        UniqueConstraint("lease_token"),
        CheckConstraint("generation >= 1", name="generation"),
        CheckConstraint("last_heartbeat_at >= acquired_at", name="heartbeat_order"),
        CheckConstraint("expires_at > last_heartbeat_at", name="expiry_order"),
        Index("ix_run_leases_expiry", "expires_at"),
        Index("ix_run_leases_worker", "worker_id", "expires_at"),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    lease_token: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    acquired_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_heartbeat_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    expires_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class WorkspaceLeaseRecord(Base):
    """Persistent generation plus optional active writer for one workspace."""

    __tablename__ = "workspace_writer_leases"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id", "workspace_id"),
            ("runs.tenant_id", "runs.id", "runs.workspace_id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("worker_id",),
            ("workers.worker_id",),
            ondelete="RESTRICT",
        ),
        UniqueConstraint("lease_token"),
        CheckConstraint("generation >= 0", name="generation"),
        CheckConstraint(
            "(run_id IS NULL AND worker_id IS NULL AND run_lease_token IS NULL "
            "AND lease_token IS NULL AND acquired_at IS NULL AND expires_at IS NULL) "
            "OR (run_id IS NOT NULL AND worker_id IS NOT NULL "
            "AND run_lease_token IS NOT NULL AND lease_token IS NOT NULL "
            "AND acquired_at IS NOT NULL AND expires_at > acquired_at)",
            name="ownership",
        ),
        Index("ix_workspace_writer_leases_expiry", "expires_at"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    worker_id: Mapped[str | None] = mapped_column(String(255))
    run_lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    generation: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    acquired_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class ModelCallRecord(Base):
    """Durable model attribution, usage, retry, fallback, and cost metadata."""

    __tablename__ = "model_calls"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id"),
            ("runs.tenant_id", "runs.id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "run_id", "model_call_id"),
        UniqueConstraint("tenant_id", "request_id"),
        CheckConstraint(
            "status IN ('started', 'streaming', 'completed', 'failed')",
            name="status",
        ),
        CheckConstraint("retry_count >= 0", name="retry_count"),
        CheckConstraint("fallback_count >= 0", name="fallback_count"),
        CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="input_tokens",
        ),
        CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="output_tokens",
        ),
        CheckConstraint(
            "cached_tokens IS NULL OR cached_tokens >= 0",
            name="cached_tokens",
        ),
        CheckConstraint(
            "estimated_cost_usd IS NULL OR estimated_cost_usd >= 0",
            name="estimated_cost",
        ),
        CheckConstraint(
            "first_token_at IS NULL OR first_token_at >= started_at",
            name="first_token_timestamp",
        ),
        CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at",
            name="completed_timestamp",
        ),
        CheckConstraint(
            "first_token_at IS NULL OR completed_at IS NULL OR first_token_at <= completed_at",
            name="token_completion_order",
        ),
        CheckConstraint(
            "status != 'streaming' OR first_token_at IS NOT NULL",
            name="streaming_state",
        ),
        CheckConstraint(
            "(status IN ('completed', 'failed') AND completed_at IS NOT NULL) "
            "OR (status NOT IN ('completed', 'failed') AND completed_at IS NULL)",
            name="terminal_state",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    model_call_id: Mapped[str] = mapped_column(String(255), nullable=False)
    request_id: Mapped[str] = mapped_column(String(255), nullable=False)
    route_name: Mapped[str] = mapped_column(String(255), nullable=False)
    provider: Mapped[str | None] = mapped_column(String(255))
    model: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    input_tokens: Mapped[int | None] = mapped_column(BigInteger)
    output_tokens: Mapped[int | None] = mapped_column(BigInteger)
    cached_tokens: Mapped[int | None] = mapped_column(BigInteger)
    estimated_cost_usd: Mapped[decimal.Decimal | None] = mapped_column(Numeric(20, 10))
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    fallback_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    started_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_token_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))


class GatewayRequestRecord(Base):
    """Tenant-scoped durable model-request idempotency record."""

    __tablename__ = "gateway_requests"
    __table_args__ = (
        CheckConstraint(
            "status IN ('in_progress', 'completed', 'failed')",
            name="status",
        ),
        CheckConstraint(
            "(status = 'in_progress' AND events = '[]'::jsonb AND error IS NULL) "
            "OR (status = 'completed' AND jsonb_array_length(events) > 0 AND error IS NULL) "
            "OR (status = 'failed' AND events = '[]'::jsonb AND error IS NOT NULL)",
            name="state_payload",
        ),
        CheckConstraint("request_hash ~ '^[0-9a-f]{64}$'", name="request_hash"),
        CheckConstraint("jsonb_typeof(events) = 'array'", name="events_array"),
        CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="error_object",
        ),
        CheckConstraint("updated_at >= created_at", name="timestamp_order"),
        Index("ix_gateway_requests_status_updated", "status", "updated_at"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    request_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    events: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=_EMPTY_ARRAY,
    )
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class TenantQuotaRecord(Base):
    """Platform-controlled per-tenant execution and gateway ceilings."""

    __tablename__ = "tenant_quotas"
    __table_args__ = (
        CheckConstraint(
            "max_active_runs >= 1 AND max_active_runs <= 10000",
            name="max_active_runs",
        ),
        CheckConstraint(
            "max_queued_runs >= 1 AND max_queued_runs <= 100000",
            name="max_queued_runs",
        ),
        CheckConstraint(
            "max_gateway_requests >= 1 AND max_gateway_requests <= 10000",
            name="max_gateway_requests",
        ),
        CheckConstraint("updated_at >= created_at", name="timestamp_order"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    max_active_runs: Mapped[int] = mapped_column(Integer, nullable=False)
    max_queued_runs: Mapped[int] = mapped_column(Integer, nullable=False)
    max_gateway_requests: Mapped[int] = mapped_column(Integer, nullable=False)
    memory_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("true"),
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class QueueAdmissionRecord(Base):
    """Singleton lock row and durable global queue threshold."""

    __tablename__ = "queue_admission"
    __table_args__ = (
        CheckConstraint("id = 1", name="singleton"),
        CheckConstraint(
            "global_queue_limit >= 1 AND global_queue_limit <= 1000000",
            name="global_queue_limit",
        ),
        CheckConstraint(
            "retry_after_seconds > 0 AND retry_after_seconds <= 3600",
            name="retry_after_seconds",
        ),
        CheckConstraint("updated_at >= created_at", name="timestamp_order"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    global_queue_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_after_seconds: Mapped[decimal.Decimal] = mapped_column(
        Numeric(10, 3),
        nullable=False,
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


class GatewayProviderCapacityRecord(Base):
    """Shared route/provider request slots and fixed token-window state."""

    __tablename__ = "gateway_provider_capacity"
    __table_args__ = (
        CheckConstraint(
            "request_limit >= 1 AND request_limit <= 10000",
            name="request_limit",
        ),
        CheckConstraint(
            "token_limit >= 1 AND token_limit <= 1000000000",
            name="token_limit",
        ),
        CheckConstraint(
            "token_window_seconds > 0 AND token_window_seconds <= 3600",
            name="token_window_seconds",
        ),
        CheckConstraint("accounted_tokens >= 0", name="accounted_tokens"),
        CheckConstraint("updated_at >= token_window_started_at", name="timestamp_order"),
    )

    route_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    request_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    token_limit: Mapped[int] = mapped_column(BigInteger, nullable=False)
    token_window_seconds: Mapped[decimal.Decimal] = mapped_column(
        Numeric(10, 3),
        nullable=False,
    )
    token_window_started_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    accounted_tokens: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )


class GatewayCapacityLeaseRecord(Base):
    """Expiring all-or-nothing tenant and provider gateway admission lease."""

    __tablename__ = "gateway_capacity_leases"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id",),
            ("tenant_quotas.tenant_id",),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("route_name",),
            ("gateway_provider_capacity.route_name",),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "request_id"),
        CheckConstraint("reserved_tokens >= 1", name="reserved_tokens"),
        CheckConstraint("expires_at > acquired_at", name="expiry_order"),
        Index("ix_gateway_capacity_leases_expiry", "expires_at"),
        Index(
            "ix_gateway_capacity_leases_tenant_expiry",
            "tenant_id",
            "expires_at",
        ),
        Index(
            "ix_gateway_capacity_leases_route_expiry",
            "route_name",
            "expires_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    route_name: Mapped[str] = mapped_column(String(100), nullable=False)
    request_id: Mapped[str] = mapped_column(String(255), nullable=False)
    reserved_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False)
    token_window_started_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    acquired_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    expires_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )


class GatewayRateLimitRecord(Base):
    """Shared fixed-window tenant-and-route request counter."""

    __tablename__ = "gateway_rate_limits"
    __table_args__ = (
        CheckConstraint("request_count >= 0", name="request_count"),
        Index("ix_gateway_rate_limits_window", "window_started_at"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    route_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    window_started_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    request_count: Mapped[int] = mapped_column(Integer, nullable=False)


class GatewayCircuitRecord(Base):
    """Shared route circuit state."""

    __tablename__ = "gateway_circuits"
    __table_args__ = (
        CheckConstraint("failure_count >= 0", name="failure_count"),
        CheckConstraint(
            "(probe_in_flight AND probe_started_at IS NOT NULL) "
            "OR (NOT probe_in_flight AND probe_started_at IS NULL)",
            name="probe_state",
        ),
    )

    route_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    opened_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    probe_started_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    probe_in_flight: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


__all__ = [
    "AgentEventRecord",
    "ApprovalRecord",
    "CheckpointRecord",
    "ContextCompactionRecord",
    "GatewayCapacityLeaseRecord",
    "GatewayCircuitRecord",
    "GatewayProviderCapacityRecord",
    "GatewayRateLimitRecord",
    "GatewayRequestRecord",
    "MemoryExtractionJobRecord",
    "MemoryRecord",
    "MessageRecord",
    "ModelCallRecord",
    "QueueAdmissionRecord",
    "RunRecord",
    "SessionRecord",
    "TaskPlanRecord",
    "TenantQuotaRecord",
    "ToolCallRecord",
]
