"""Make rewind create an isolated durable execution branch.

Revision ID: 0016
Revises: 0015
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | None = None
depends_on: str | None = None

_EPOCH_TABLES = (
    "artifacts",
    "runs",
    "messages",
    "task_plans",
    "memories",
    "memory_extraction_jobs",
    "tool_calls",
    "approvals",
    "checkpoints",
    "agent_events",
    "model_calls",
)


def _add_positive_counter(table: str, column: str) -> None:
    op.add_column(
        table,
        sa.Column(column, sa.BigInteger(), nullable=False, server_default=sa.text("1")),
    )
    op.create_check_constraint(column, table, f"{column} >= 1")


def upgrade() -> None:
    _add_positive_counter("sessions", "context_generation")
    for table in _EPOCH_TABLES:
        _add_positive_counter(table, "execution_epoch")
    op.create_check_constraint(
        "execution_epoch_attempt",
        "runs",
        "execution_epoch <= attempt",
    )
    _add_positive_counter("context_compactions", "context_generation")

    op.drop_constraint(
        "fk_approvals_tenant_id_run_id_tool_call_id_tool_calls",
        "approvals",
        type_="foreignkey",
    )
    op.drop_constraint(
        "uq_approvals_tenant_id_run_id_tool_call_id",
        "approvals",
        type_="unique",
    )
    op.drop_index("ix_approvals_run_status", table_name="approvals")
    op.drop_constraint(
        "uq_tool_calls_tenant_id_run_id_tool_call_id",
        "tool_calls",
        type_="unique",
    )
    op.drop_index("ix_tool_calls_run_status", table_name="tool_calls")

    op.create_unique_constraint(
        "uq_tool_calls_tenant_id_run_id_execution_epoch_tool_call_id",
        "tool_calls",
        ("tenant_id", "run_id", "execution_epoch", "tool_call_id"),
    )
    op.create_index(
        "ix_tool_calls_run_status",
        "tool_calls",
        ("tenant_id", "run_id", "execution_epoch", "status"),
    )
    op.create_foreign_key(
        "fk_approvals_branch_tool_call",
        "approvals",
        "tool_calls",
        ["tenant_id", "run_id", "execution_epoch", "tool_call_id"],
        ["tenant_id", "run_id", "execution_epoch", "tool_call_id"],
    )
    op.create_unique_constraint(
        "uq_approvals_tenant_id_run_id_execution_epoch_tool_call_id",
        "approvals",
        ("tenant_id", "run_id", "execution_epoch", "tool_call_id"),
    )
    op.create_index(
        "ix_approvals_run_status",
        "approvals",
        ("tenant_id", "run_id", "execution_epoch", "status"),
    )

    _replace_unique(
        "task_plans",
        "uq_task_plans_tenant_id_run_id_version",
        "uq_task_plans_tenant_id_run_id_execution_epoch_version",
        ("tenant_id", "run_id", "execution_epoch", "version"),
    )
    _replace_unique(
        "memories",
        "uq_memories_memory_identity",
        "uq_memories_memory_identity",
        ("tenant_id", "session_id", "execution_epoch", "kind", "content_hash"),
    )
    _replace_unique(
        "memory_extraction_jobs",
        "uq_memory_extraction_jobs_tenant_id_run_id",
        "uq_memory_extraction_jobs_tenant_id_run_id_execution_epoch",
        ("tenant_id", "run_id", "execution_epoch"),
    )
    _replace_unique(
        "checkpoints",
        "uq_checkpoints_tenant_id_run_id_tool_call_id",
        "uq_checkpoints_tenant_id_run_id_execution_epoch_tool_call_id",
        ("tenant_id", "run_id", "execution_epoch", "tool_call_id"),
    )
    _replace_unique(
        "agent_events",
        "uq_agent_events_run_id_delivery_key",
        "uq_agent_events_run_id_execution_epoch_delivery_key",
        ("run_id", "execution_epoch", "delivery_key"),
    )
    _replace_unique(
        "model_calls",
        "uq_model_calls_tenant_id_run_id_model_call_id",
        "uq_model_calls_tenant_id_run_id_execution_epoch_model_call_id",
        ("tenant_id", "run_id", "execution_epoch", "model_call_id"),
    )
    _replace_unique(
        "model_calls",
        "uq_model_calls_tenant_id_request_id",
        "uq_model_calls_tenant_id_run_id_execution_epoch_request_id",
        ("tenant_id", "run_id", "execution_epoch", "request_id"),
    )


def _replace_unique(
    table: str,
    old_name: str,
    new_name: str,
    columns: tuple[str, ...],
) -> None:
    op.drop_constraint(old_name, table, type_="unique")
    op.create_unique_constraint(new_name, table, columns)


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN "
        "IF EXISTS (SELECT 1 FROM sessions WHERE context_generation <> 1) "
        "OR EXISTS (SELECT 1 FROM runs WHERE execution_epoch <> 1) THEN "
        "RAISE EXCEPTION 'cannot downgrade execution branches after a rewind'; "
        "END IF; END $$"
    )

    op.drop_constraint(
        "fk_approvals_branch_tool_call",
        "approvals",
        type_="foreignkey",
    )
    op.drop_constraint(
        "uq_approvals_tenant_id_run_id_execution_epoch_tool_call_id",
        "approvals",
        type_="unique",
    )
    op.drop_index("ix_approvals_run_status", table_name="approvals")
    op.drop_constraint(
        "uq_tool_calls_tenant_id_run_id_execution_epoch_tool_call_id",
        "tool_calls",
        type_="unique",
    )
    op.drop_index("ix_tool_calls_run_status", table_name="tool_calls")

    op.create_unique_constraint(
        "uq_tool_calls_tenant_id_run_id_tool_call_id",
        "tool_calls",
        ("tenant_id", "run_id", "tool_call_id"),
    )
    op.create_index(
        "ix_tool_calls_run_status",
        "tool_calls",
        ("tenant_id", "run_id", "status"),
    )
    op.create_foreign_key(
        "fk_approvals_tenant_id_run_id_tool_call_id_tool_calls",
        "approvals",
        "tool_calls",
        ["tenant_id", "run_id", "tool_call_id"],
        ["tenant_id", "run_id", "tool_call_id"],
    )
    op.create_unique_constraint(
        "uq_approvals_tenant_id_run_id_tool_call_id",
        "approvals",
        ("tenant_id", "run_id", "tool_call_id"),
    )
    op.create_index(
        "ix_approvals_run_status",
        "approvals",
        ("tenant_id", "run_id", "status"),
    )

    _replace_unique(
        "task_plans",
        "uq_task_plans_tenant_id_run_id_execution_epoch_version",
        "uq_task_plans_tenant_id_run_id_version",
        ("tenant_id", "run_id", "version"),
    )
    _replace_unique(
        "memories",
        "uq_memories_memory_identity",
        "uq_memories_memory_identity",
        ("tenant_id", "session_id", "kind", "content_hash"),
    )
    _replace_unique(
        "memory_extraction_jobs",
        "uq_memory_extraction_jobs_tenant_id_run_id_execution_epoch",
        "uq_memory_extraction_jobs_tenant_id_run_id",
        ("tenant_id", "run_id"),
    )
    _replace_unique(
        "checkpoints",
        "uq_checkpoints_tenant_id_run_id_execution_epoch_tool_call_id",
        "uq_checkpoints_tenant_id_run_id_tool_call_id",
        ("tenant_id", "run_id", "tool_call_id"),
    )
    _replace_unique(
        "agent_events",
        "uq_agent_events_run_id_execution_epoch_delivery_key",
        "uq_agent_events_run_id_delivery_key",
        ("run_id", "delivery_key"),
    )
    _replace_unique(
        "model_calls",
        "uq_model_calls_tenant_id_run_id_execution_epoch_model_call_id",
        "uq_model_calls_tenant_id_run_id_model_call_id",
        ("tenant_id", "run_id", "model_call_id"),
    )
    _replace_unique(
        "model_calls",
        "uq_model_calls_tenant_id_run_id_execution_epoch_request_id",
        "uq_model_calls_tenant_id_request_id",
        ("tenant_id", "request_id"),
    )

    op.drop_constraint("ck_runs_execution_epoch_attempt", "runs")
    op.drop_constraint("ck_context_compactions_context_generation", "context_compactions")
    op.drop_column("context_compactions", "context_generation")
    for table in reversed(_EPOCH_TABLES):
        op.drop_constraint(f"ck_{table}_execution_epoch", table)
        op.drop_column(table, "execution_epoch")
    op.drop_constraint("ck_sessions_context_generation", "sessions")
    op.drop_column("sessions", "context_generation")
