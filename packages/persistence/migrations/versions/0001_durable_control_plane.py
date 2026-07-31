"""Create the durable control-plane schema.

Revision ID: 0001
Revises:
Create Date: 2026-07-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB()
UTC_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    """Create every Phase 5 durable entity and its tenant/idempotency constraints."""

    op.create_table(
        "sessions",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("workspace_id", UUID, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("approval_mode", sa.String(32), nullable=False),
        sa.Column("model_route", sa.String(255), nullable=False),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "status IN ('active', 'completed', 'cancelled')",
            name="ck_sessions_status",
        ),
        sa.CheckConstraint(
            "approval_mode IN ('require_all', 'require_sensitive', 'auto_approve')",
            name="ck_sessions_approval_mode",
        ),
        sa.CheckConstraint(
            "updated_at >= created_at",
            name="ck_sessions_timestamp_order",
        ),
        sa.UniqueConstraint("tenant_id", "id", name="uq_sessions_tenant_id_id"),
        sa.UniqueConstraint(
            "tenant_id",
            "id",
            "workspace_id",
            name="uq_sessions_tenant_id_id_workspace_id",
        ),
    )
    op.create_index("ix_sessions_tenant_created", "sessions", ["tenant_id", "created_at"])

    op.create_table(
        "runs",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("session_id", UUID, nullable=False),
        sa.Column("workspace_id", UUID, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("assigned_worker_id", sa.String(255)),
        sa.Column("lease_expires_at", UTC_TIMESTAMP),
        sa.Column("last_checkpoint_id", UUID),
        sa.Column(
            "cancellation_requested",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("creation_hash", sa.String(64), nullable=False),
        sa.Column(
            "next_event_sequence",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("started_at", UTC_TIMESTAMP),
        sa.Column("completed_at", UTC_TIMESTAMP),
        sa.CheckConstraint(
            "status IN "
            "('queued', 'leased', 'running', 'waiting_approval', 'retry_pending', "
            "'completed', 'failed', 'cancelled', 'lost')",
            name="ck_runs_status",
        ),
        sa.CheckConstraint("attempt >= 1", name="ck_runs_attempt"),
        sa.CheckConstraint("next_event_sequence >= 1", name="ck_runs_next_event_sequence"),
        sa.CheckConstraint(
            "creation_hash ~ '^[0-9a-f]{64}$'",
            name="ck_runs_creation_hash",
        ),
        sa.CheckConstraint(
            "started_at IS NULL OR started_at >= created_at",
            name="ck_runs_started_timestamp",
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR completed_at >= COALESCE(started_at, created_at)",
            name="ck_runs_completed_timestamp",
        ),
        sa.CheckConstraint(
            "status NOT IN "
            "('running', 'waiting_approval', 'retry_pending', 'completed', 'failed') "
            "OR started_at IS NOT NULL",
            name="ck_runs_started_state",
        ),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL) "
            "OR (status NOT IN ('completed', 'failed', 'cancelled') AND completed_at IS NULL)",
            name="ck_runs_terminal_state",
        ),
        sa.CheckConstraint(
            "status NOT IN ('leased', 'running') "
            "OR (assigned_worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_runs_active_lease",
        ),
        sa.CheckConstraint(
            "status NOT IN ('queued', 'waiting_approval', 'retry_pending') "
            "OR (assigned_worker_id IS NULL AND lease_expires_at IS NULL)",
            name="ck_runs_suspended_without_lease",
        ),
        sa.ForeignKeyConstraint(
            ("tenant_id", "session_id", "workspace_id"),
            ("sessions.tenant_id", "sessions.id", "sessions.workspace_id"),
            name="fk_runs_tenant_id_session_id_workspace_id_sessions",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("tenant_id", "id", name="uq_runs_tenant_id_id"),
        sa.UniqueConstraint(
            "tenant_id",
            "id",
            "session_id",
            name="uq_runs_tenant_id_id_session_id",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "session_id",
            "idempotency_key",
            name="uq_runs_tenant_id_session_id_idempotency_key",
        ),
    )
    op.create_index(
        "ix_runs_tenant_session_created",
        "runs",
        ["tenant_id", "session_id", "created_at"],
    )
    op.create_index("ix_runs_status_created", "runs", ["status", "created_at"])

    _create_messages()
    _create_task_plans()
    _create_tool_calls()
    _create_approvals()
    _create_checkpoints()
    op.create_foreign_key(
        "fk_runs_tenant_id_id_last_checkpoint_id_checkpoints",
        "runs",
        "checkpoints",
        ["tenant_id", "id", "last_checkpoint_id"],
        ["tenant_id", "run_id", "id"],
    )
    _create_agent_events()
    _create_model_calls()
    _create_gateway_requests()
    _create_gateway_rate_limits()
    _create_gateway_circuits()


def _run_foreign_key(name: str) -> sa.ForeignKeyConstraint:
    return sa.ForeignKeyConstraint(
        ("tenant_id", "run_id"),
        ("runs.tenant_id", "runs.id"),
        name=name,
        ondelete="CASCADE",
    )


def _run_session_foreign_key(name: str) -> sa.ForeignKeyConstraint:
    return sa.ForeignKeyConstraint(
        ("tenant_id", "run_id", "session_id"),
        ("runs.tenant_id", "runs.id", "runs.session_id"),
        name=name,
        ondelete="CASCADE",
    )


def _create_messages() -> None:
    op.create_table(
        "messages",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("session_id", UUID, nullable=False),
        sa.Column("run_id", UUID, nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column(
            "metadata",
            JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("sequence >= 1", name="ck_messages_sequence"),
        sa.CheckConstraint(
            "role IN ('system', 'user', 'assistant', 'tool')",
            name="ck_messages_role",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(metadata) = 'object'",
            name="ck_messages_metadata_object",
        ),
        sa.ForeignKeyConstraint(
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            name="fk_messages_tenant_id_session_id_sessions",
            ondelete="CASCADE",
        ),
        _run_session_foreign_key("fk_messages_tenant_id_run_id_session_id_runs"),
        sa.UniqueConstraint(
            "tenant_id",
            "session_id",
            "sequence",
            name="uq_messages_tenant_id_session_id_sequence",
        ),
    )


def _create_task_plans() -> None:
    op.create_table(
        "task_plans",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("run_id", UUID, nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("plan", JSONB, nullable=False),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("version >= 1", name="ck_task_plans_version"),
        sa.CheckConstraint(
            "jsonb_typeof(plan) = 'object'",
            name="ck_task_plans_plan_object",
        ),
        _run_foreign_key("fk_task_plans_tenant_id_run_id_runs"),
        sa.UniqueConstraint(
            "tenant_id",
            "run_id",
            "version",
            name="uq_task_plans_tenant_id_run_id_version",
        ),
    )


def _create_tool_calls() -> None:
    op.create_table(
        "tool_calls",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("run_id", UUID, nullable=False),
        sa.Column("tool_call_id", sa.String(255), nullable=False),
        sa.Column("turn_number", sa.Integer(), nullable=False),
        sa.Column("tool_name", sa.String(100), nullable=False),
        sa.Column("arguments", JSONB, nullable=False),
        sa.Column("argument_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("workspace_version", sa.String(255)),
        sa.Column("result", JSONB),
        sa.Column("started_at", UTC_TIMESTAMP),
        sa.Column("completed_at", UTC_TIMESTAMP),
        sa.CheckConstraint("turn_number >= 1", name="ck_tool_calls_turn_number"),
        sa.CheckConstraint(
            "status IN "
            "('received', 'waiting_approval', 'running', 'completed', 'failed', 'cancelled')",
            name="ck_tool_calls_status",
        ),
        sa.CheckConstraint(
            "argument_hash ~ '^[0-9a-f]{64}$'",
            name="ck_tool_calls_argument_hash",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(arguments) = 'object'",
            name="ck_tool_calls_arguments_object",
        ),
        sa.CheckConstraint(
            "result IS NULL OR jsonb_typeof(result) = 'object'",
            name="ck_tool_calls_result_object",
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at",
            name="ck_tool_calls_timestamp_order",
        ),
        sa.CheckConstraint(
            "status NOT IN ('running', 'completed') OR started_at IS NOT NULL",
            name="ck_tool_calls_started_state",
        ),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL) "
            "OR (status NOT IN ('completed', 'failed', 'cancelled') AND completed_at IS NULL)",
            name="ck_tool_calls_terminal_state",
        ),
        _run_foreign_key("fk_tool_calls_tenant_id_run_id_runs"),
        sa.UniqueConstraint(
            "tenant_id",
            "run_id",
            "tool_call_id",
            name="uq_tool_calls_tenant_id_run_id_tool_call_id",
        ),
    )
    op.create_index(
        "ix_tool_calls_run_status",
        "tool_calls",
        ["tenant_id", "run_id", "status"],
    )


def _create_approvals() -> None:
    op.create_table(
        "approvals",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("run_id", UUID, nullable=False),
        sa.Column("tool_call_id", sa.String(255)),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "arguments",
            JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("decided_by", sa.String(255)),
        sa.Column("requested_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("decided_at", UTC_TIMESTAMP),
        sa.CheckConstraint(
            "status IN ('pending', 'approved', 'rejected')",
            name="ck_approvals_status",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(arguments) = 'object'",
            name="ck_approvals_arguments_object",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND decided_by IS NULL AND decided_at IS NULL) "
            "OR (status IN ('approved', 'rejected') "
            "AND decided_by IS NOT NULL AND decided_at IS NOT NULL "
            "AND decided_at >= requested_at)",
            name="ck_approvals_decision_state",
        ),
        _run_foreign_key("fk_approvals_tenant_id_run_id_runs"),
        sa.ForeignKeyConstraint(
            ("tenant_id", "run_id", "tool_call_id"),
            (
                "tool_calls.tenant_id",
                "tool_calls.run_id",
                "tool_calls.tool_call_id",
            ),
            name="fk_approvals_tenant_id_run_id_tool_call_id_tool_calls",
        ),
    )
    op.create_index(
        "ix_approvals_run_status",
        "approvals",
        ["tenant_id", "run_id", "status"],
    )


def _create_checkpoints() -> None:
    op.create_table(
        "checkpoints",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("run_id", UUID, nullable=False),
        sa.Column("session_id", UUID, nullable=False),
        sa.Column("message_sequence", sa.BigInteger(), nullable=False),
        sa.Column("workspace_snapshot_uri", sa.Text(), nullable=False),
        sa.Column("workspace_revision", sa.String(255), nullable=False),
        sa.Column(
            "task_plan",
            JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("context_summary", sa.Text()),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "message_sequence >= 0",
            name="ck_checkpoints_message_sequence",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(task_plan) = 'object'",
            name="ck_checkpoints_task_plan_object",
        ),
        _run_session_foreign_key("fk_checkpoints_tenant_id_run_id_session_id_runs"),
        sa.ForeignKeyConstraint(
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            name="fk_checkpoints_tenant_id_session_id_sessions",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "run_id",
            "message_sequence",
            name="uq_checkpoints_tenant_id_run_id_message_sequence",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "run_id",
            "id",
            name="uq_checkpoints_tenant_id_run_id_id",
        ),
    )


def _create_agent_events() -> None:
    op.create_table(
        "agent_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("run_id", UUID, nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("sequence >= 1", name="ck_agent_events_sequence"),
        sa.CheckConstraint(
            "event_type IN "
            "('run.started', 'context.build_started', 'model.request_started', "
            "'model.text_delta', 'model.tool_call_received', 'tool.approval_required', "
            "'tool.started', 'tool.stdout', 'tool.stderr', 'tool.completed', "
            "'checkpoint.created', 'run.retry_scheduled', 'run.completed', 'run.failed')",
            name="ck_agent_events_event_type",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name="ck_agent_events_payload_object",
        ),
        _run_foreign_key("fk_agent_events_tenant_id_run_id_runs"),
        sa.UniqueConstraint(
            "run_id",
            "sequence",
            name="uq_agent_events_run_id_sequence",
        ),
    )
    op.create_index(
        "ix_agent_events_tenant_run_sequence",
        "agent_events",
        ["tenant_id", "run_id", "sequence"],
    )


def _create_model_calls() -> None:
    op.create_table(
        "model_calls",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("run_id", UUID, nullable=False),
        sa.Column("model_call_id", sa.String(255), nullable=False),
        sa.Column("request_id", sa.String(255), nullable=False),
        sa.Column("route_name", sa.String(255), nullable=False),
        sa.Column("provider", sa.String(255)),
        sa.Column("model", sa.String(255)),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("input_tokens", sa.BigInteger()),
        sa.Column("output_tokens", sa.BigInteger()),
        sa.Column("cached_tokens", sa.BigInteger()),
        sa.Column("estimated_cost_usd", sa.Numeric(20, 10)),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("fallback_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("started_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("first_token_at", UTC_TIMESTAMP),
        sa.Column("completed_at", UTC_TIMESTAMP),
        sa.CheckConstraint(
            "status IN ('started', 'streaming', 'completed', 'failed')",
            name="ck_model_calls_status",
        ),
        sa.CheckConstraint("retry_count >= 0", name="ck_model_calls_retry_count"),
        sa.CheckConstraint("fallback_count >= 0", name="ck_model_calls_fallback_count"),
        sa.CheckConstraint(
            "input_tokens IS NULL OR input_tokens >= 0",
            name="ck_model_calls_input_tokens",
        ),
        sa.CheckConstraint(
            "output_tokens IS NULL OR output_tokens >= 0",
            name="ck_model_calls_output_tokens",
        ),
        sa.CheckConstraint(
            "cached_tokens IS NULL OR cached_tokens >= 0",
            name="ck_model_calls_cached_tokens",
        ),
        sa.CheckConstraint(
            "estimated_cost_usd IS NULL OR estimated_cost_usd >= 0",
            name="ck_model_calls_estimated_cost",
        ),
        sa.CheckConstraint(
            "first_token_at IS NULL OR first_token_at >= started_at",
            name="ck_model_calls_first_token_timestamp",
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR completed_at >= started_at",
            name="ck_model_calls_completed_timestamp",
        ),
        sa.CheckConstraint(
            "first_token_at IS NULL OR completed_at IS NULL OR first_token_at <= completed_at",
            name="ck_model_calls_token_completion_order",
        ),
        sa.CheckConstraint(
            "status != 'streaming' OR first_token_at IS NOT NULL",
            name="ck_model_calls_streaming_state",
        ),
        sa.CheckConstraint(
            "(status IN ('completed', 'failed') AND completed_at IS NOT NULL) "
            "OR (status NOT IN ('completed', 'failed') AND completed_at IS NULL)",
            name="ck_model_calls_terminal_state",
        ),
        _run_foreign_key("fk_model_calls_tenant_id_run_id_runs"),
        sa.UniqueConstraint(
            "tenant_id",
            "run_id",
            "model_call_id",
            name="uq_model_calls_tenant_id_run_id_model_call_id",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "request_id",
            name="uq_model_calls_tenant_id_request_id",
        ),
    )


def _create_gateway_requests() -> None:
    op.create_table(
        "gateway_requests",
        sa.Column("tenant_id", UUID, primary_key=True),
        sa.Column("request_id", sa.String(255), primary_key=True),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column(
            "events",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("error", JSONB),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "status IN ('in_progress', 'completed', 'failed')",
            name="ck_gateway_requests_status",
        ),
        sa.CheckConstraint(
            "(status = 'in_progress' AND events = '[]'::jsonb AND error IS NULL) "
            "OR (status = 'completed' AND jsonb_array_length(events) > 0 AND error IS NULL) "
            "OR (status = 'failed' AND events = '[]'::jsonb AND error IS NOT NULL)",
            name="ck_gateway_requests_state_payload",
        ),
        sa.CheckConstraint(
            "request_hash ~ '^[0-9a-f]{64}$'",
            name="ck_gateway_requests_request_hash",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(events) = 'array'",
            name="ck_gateway_requests_events_array",
        ),
        sa.CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="ck_gateway_requests_error_object",
        ),
        sa.CheckConstraint(
            "updated_at >= created_at",
            name="ck_gateway_requests_timestamp_order",
        ),
    )
    op.create_index(
        "ix_gateway_requests_status_updated",
        "gateway_requests",
        ["status", "updated_at"],
    )


def _create_gateway_rate_limits() -> None:
    op.create_table(
        "gateway_rate_limits",
        sa.Column("tenant_id", UUID, primary_key=True),
        sa.Column("route_name", sa.String(100), primary_key=True),
        sa.Column("window_started_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "request_count >= 0",
            name="ck_gateway_rate_limits_request_count",
        ),
    )
    op.create_index(
        "ix_gateway_rate_limits_window",
        "gateway_rate_limits",
        ["window_started_at"],
    )


def _create_gateway_circuits() -> None:
    op.create_table(
        "gateway_circuits",
        sa.Column("route_name", sa.String(100), primary_key=True),
        sa.Column(
            "failure_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("opened_at", UTC_TIMESTAMP),
        sa.Column("probe_started_at", UTC_TIMESTAMP),
        sa.Column(
            "probe_in_flight",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "failure_count >= 0",
            name="ck_gateway_circuits_failure_count",
        ),
        sa.CheckConstraint(
            "(probe_in_flight AND probe_started_at IS NOT NULL) "
            "OR (NOT probe_in_flight AND probe_started_at IS NULL)",
            name="ck_gateway_circuits_probe_state",
        ),
    )


def downgrade() -> None:
    """Drop only Sequence 14-owned schema objects in dependency-safe order."""

    op.drop_constraint(
        "fk_runs_tenant_id_id_last_checkpoint_id_checkpoints",
        "runs",
        type_="foreignkey",
    )
    for table_name in (
        "gateway_circuits",
        "gateway_rate_limits",
        "gateway_requests",
        "model_calls",
        "agent_events",
        "checkpoints",
        "approvals",
        "tool_calls",
        "task_plans",
        "messages",
        "runs",
        "sessions",
    ):
        op.drop_table(table_name)
