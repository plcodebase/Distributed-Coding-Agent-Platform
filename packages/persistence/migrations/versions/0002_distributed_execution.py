"""Add the distributed execution queue and fenced leases.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB()
UTC_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    """Add PR 17-20 queue, worker, replay, and workspace ownership state."""

    op.add_column(
        "runs",
        sa.Column(
            "lease_generation",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.create_check_constraint(
        "ck_runs_lease_generation",
        "runs",
        "lease_generation >= 0",
    )
    op.drop_constraint("ck_runs_suspended_without_lease", "runs", type_="check")
    op.create_check_constraint(
        "ck_runs_suspended_without_lease",
        "runs",
        "status NOT IN ('queued', 'waiting_approval', 'retry_pending', 'lost') "
        "OR (assigned_worker_id IS NULL AND lease_expires_at IS NULL)",
    )
    op.create_index(
        "ix_runs_queue_claim",
        "runs",
        ["status", sa.text("priority DESC"), "created_at"],
    )
    op.create_unique_constraint(
        "uq_runs_tenant_id_id_workspace_id",
        "runs",
        ["tenant_id", "id", "workspace_id"],
    )

    op.add_column(
        "tool_calls",
        sa.Column("error", JSONB, nullable=True),
    )
    op.create_check_constraint(
        "ck_tool_calls_error_object",
        "tool_calls",
        "error IS NULL OR jsonb_typeof(error) = 'object'",
    )
    op.create_check_constraint(
        "ck_tool_calls_terminal_outcome",
        "tool_calls",
        "(status = 'completed' AND result IS NOT NULL AND error IS NULL) "
        "OR (status IN ('failed', 'cancelled') AND result IS NULL AND error IS NOT NULL) "
        "OR (status NOT IN ('completed', 'failed', 'cancelled') "
        "AND result IS NULL AND error IS NULL)",
    )

    op.add_column(
        "agent_events",
        sa.Column("delivery_key", sa.String(255), nullable=True),
    )
    op.create_unique_constraint(
        "uq_agent_events_run_id_delivery_key",
        "agent_events",
        ["run_id", "delivery_key"],
    )

    _create_workers()
    _create_run_leases()
    _create_workspace_writer_leases()


def _create_workers() -> None:
    op.create_table(
        "workers",
        sa.Column("worker_id", sa.String(255), primary_key=True),
        sa.Column("supported_sandbox_types", JSONB, nullable=False),
        sa.Column("total_slots", sa.Integer(), nullable=False),
        sa.Column("available_slots", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("registered_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("last_heartbeat_at", UTC_TIMESTAMP, nullable=False),
        sa.CheckConstraint(
            "status IN ('active', 'draining', 'offline')",
            name="ck_workers_status",
        ),
        sa.CheckConstraint(
            "total_slots >= 1 AND total_slots <= 1024",
            name="ck_workers_total_slots",
        ),
        sa.CheckConstraint(
            "available_slots >= 0 AND available_slots <= total_slots",
            name="ck_workers_available_slots",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(supported_sandbox_types) = 'array'",
            name="ck_workers_sandbox_types_array",
        ),
        sa.CheckConstraint(
            "last_heartbeat_at >= registered_at",
            name="ck_workers_heartbeat_order",
        ),
    )
    op.create_index(
        "ix_workers_status_heartbeat",
        "workers",
        ["status", "last_heartbeat_at"],
    )


def _create_run_leases() -> None:
    op.create_table(
        "run_leases",
        sa.Column("run_id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=False),
        sa.Column("lease_token", UUID, nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("acquired_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("last_heartbeat_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("expires_at", UTC_TIMESTAMP, nullable=False),
        sa.CheckConstraint("generation >= 1", name="ck_run_leases_generation"),
        sa.CheckConstraint(
            "last_heartbeat_at >= acquired_at",
            name="ck_run_leases_heartbeat_order",
        ),
        sa.CheckConstraint(
            "expires_at > last_heartbeat_at",
            name="ck_run_leases_expiry_order",
        ),
        sa.ForeignKeyConstraint(
            ("tenant_id", "run_id"),
            ("runs.tenant_id", "runs.id"),
            name="fk_run_leases_tenant_id_run_id_runs",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("worker_id",),
            ("workers.worker_id",),
            name="fk_run_leases_worker_id_workers",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "run_id",
            name="uq_run_leases_tenant_id_run_id",
        ),
        sa.UniqueConstraint("lease_token", name="uq_run_leases_lease_token"),
    )
    op.create_index("ix_run_leases_expiry", "run_leases", ["expires_at"])
    op.create_index(
        "ix_run_leases_worker",
        "run_leases",
        ["worker_id", "expires_at"],
    )


def _create_workspace_writer_leases() -> None:
    op.create_table(
        "workspace_writer_leases",
        sa.Column("tenant_id", UUID, primary_key=True),
        sa.Column("workspace_id", UUID, primary_key=True),
        sa.Column("run_id", UUID),
        sa.Column("worker_id", sa.String(255)),
        sa.Column("run_lease_token", UUID),
        sa.Column("lease_token", UUID),
        sa.Column(
            "generation",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("acquired_at", UTC_TIMESTAMP),
        sa.Column("expires_at", UTC_TIMESTAMP),
        sa.CheckConstraint(
            "generation >= 0",
            name="ck_workspace_writer_leases_generation",
        ),
        sa.CheckConstraint(
            "(run_id IS NULL AND worker_id IS NULL AND run_lease_token IS NULL "
            "AND lease_token IS NULL AND acquired_at IS NULL AND expires_at IS NULL) "
            "OR (run_id IS NOT NULL AND worker_id IS NOT NULL "
            "AND run_lease_token IS NOT NULL AND lease_token IS NOT NULL "
            "AND acquired_at IS NOT NULL AND expires_at > acquired_at)",
            name="ck_workspace_writer_leases_ownership",
        ),
        sa.ForeignKeyConstraint(
            ("tenant_id", "run_id", "workspace_id"),
            ("runs.tenant_id", "runs.id", "runs.workspace_id"),
            name="fk_workspace_writer_leases_tenant_run_workspace_runs",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("worker_id",),
            ("workers.worker_id",),
            name="fk_workspace_writer_leases_worker_id_workers",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "lease_token",
            name="uq_workspace_writer_leases_lease_token",
        ),
    )
    op.create_index(
        "ix_workspace_writer_leases_expiry",
        "workspace_writer_leases",
        ["expires_at"],
    )


def downgrade() -> None:
    """Remove distributed execution state while preserving Phase 5 data."""

    op.drop_index(
        "ix_workspace_writer_leases_expiry",
        table_name="workspace_writer_leases",
    )
    op.drop_table("workspace_writer_leases")
    op.drop_index("ix_run_leases_worker", table_name="run_leases")
    op.drop_index("ix_run_leases_expiry", table_name="run_leases")
    op.drop_table("run_leases")
    op.drop_index("ix_workers_status_heartbeat", table_name="workers")
    op.drop_table("workers")

    op.drop_constraint(
        "uq_agent_events_run_id_delivery_key",
        "agent_events",
        type_="unique",
    )
    op.drop_column("agent_events", "delivery_key")
    op.drop_constraint("ck_tool_calls_terminal_outcome", "tool_calls", type_="check")
    op.drop_constraint("ck_tool_calls_error_object", "tool_calls", type_="check")
    op.drop_column("tool_calls", "error")

    op.drop_index("ix_runs_queue_claim", table_name="runs")
    op.drop_constraint(
        "uq_runs_tenant_id_id_workspace_id",
        "runs",
        type_="unique",
    )
    op.drop_constraint("ck_runs_suspended_without_lease", "runs", type_="check")
    op.create_check_constraint(
        "ck_runs_suspended_without_lease",
        "runs",
        "status NOT IN ('queued', 'waiting_approval', 'retry_pending') "
        "OR (assigned_worker_id IS NULL AND lease_expires_at IS NULL)",
    )
    op.drop_constraint("ck_runs_lease_generation", "runs", type_="check")
    op.drop_column("runs", "lease_generation")
