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


class SessionRecord(Base):
    """Durable tenant-owned conversation."""

    __tablename__ = "sessions"
    __table_args__ = (
        UniqueConstraint("tenant_id", "id"),
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
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "id"),
        UniqueConstraint("tenant_id", "session_id", "idempotency_key"),
        CheckConstraint(
            "status IN "
            "('queued', 'leased', 'running', 'waiting_approval', 'retry_pending', "
            "'completed', 'failed', 'cancelled', 'lost')",
            name="status",
        ),
        CheckConstraint("attempt >= 1", name="attempt"),
        CheckConstraint("next_event_sequence >= 1", name="next_event_sequence"),
        CheckConstraint("creation_hash ~ '^[0-9a-f]{64}$'", name="creation_hash"),
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
            "status NOT IN ('queued', 'waiting_approval', 'retry_pending') "
            "OR (assigned_worker_id IS NULL AND lease_expires_at IS NULL)",
            name="suspended_without_lease",
        ),
        Index("ix_runs_tenant_session_created", "tenant_id", "session_id", "created_at"),
        Index("ix_runs_status_created", "status", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    workspace_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    assigned_worker_id: Mapped[str | None] = mapped_column(String(255))
    lease_expires_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True))
    last_checkpoint_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    cancellation_requested: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
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
            ("tenant_id", "run_id"),
            ("runs.tenant_id", "runs.id"),
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


class CheckpointRecord(Base):
    """Durable checkpoint metadata referencing an immutable workspace snapshot."""

    __tablename__ = "checkpoints"
    __table_args__ = (
        ForeignKeyConstraint(
            ("tenant_id", "run_id"),
            ("runs.tenant_id", "runs.id"),
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "run_id", "message_sequence"),
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
        CheckConstraint("sequence >= 1", name="sequence"),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="payload_object"),
        Index("ix_agent_events_tenant_run_sequence", "tenant_id", "run_id", "sequence"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_UTC_NOW,
    )


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
    "GatewayCircuitRecord",
    "GatewayRateLimitRecord",
    "GatewayRequestRecord",
    "MessageRecord",
    "ModelCallRecord",
    "RunRecord",
    "SessionRecord",
    "TaskPlanRecord",
    "ToolCallRecord",
]
