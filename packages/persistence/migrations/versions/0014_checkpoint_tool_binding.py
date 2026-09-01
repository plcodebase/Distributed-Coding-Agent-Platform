"""Bind every durable checkpoint to the exact logical tool call.

Revision ID: 0014
Revises: 0013
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "checkpoints",
        sa.Column("tool_call_id", sa.String(length=255), nullable=True),
    )
    op.execute(
        "UPDATE checkpoints "
        "SET tool_call_id = 'legacy-checkpoint-' || id::text "
        "WHERE tool_call_id IS NULL"
    )
    op.alter_column("checkpoints", "tool_call_id", nullable=False)
    op.drop_constraint(
        "uq_checkpoints_tenant_id_run_id_message_sequence",
        "checkpoints",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_checkpoints_tenant_id_run_id_tool_call_id",
        "checkpoints",
        ("tenant_id", "run_id", "tool_call_id"),
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_checkpoints_tenant_id_run_id_tool_call_id",
        "checkpoints",
        type_="unique",
    )
    # The previous schema could retain only one checkpoint per message sequence.
    # Preserve every row during downgrade by assigning duplicate checkpoints a
    # deterministic sequence above the run's existing maximum before restoring
    # the legacy uniqueness constraint.
    op.execute(
        "WITH ranked AS ("
        "SELECT id, tenant_id, run_id, message_sequence, created_at, "
        "row_number() OVER ("
        "PARTITION BY tenant_id, run_id, message_sequence ORDER BY created_at, id"
        ") AS duplicate_rank, "
        "max(message_sequence) OVER (PARTITION BY tenant_id, run_id) AS max_sequence "
        "FROM checkpoints"
        "), extras AS ("
        "SELECT id, tenant_id, run_id, "
        "max_sequence + row_number() OVER ("
        "PARTITION BY tenant_id, run_id ORDER BY message_sequence, created_at, id"
        ") AS replacement_sequence "
        "FROM ranked WHERE duplicate_rank > 1"
        ") UPDATE checkpoints AS checkpoint "
        "SET message_sequence = extras.replacement_sequence "
        "FROM extras WHERE checkpoint.id = extras.id "
        "AND checkpoint.tenant_id = extras.tenant_id "
        "AND checkpoint.run_id = extras.run_id"
    )
    op.create_unique_constraint(
        "uq_checkpoints_tenant_id_run_id_message_sequence",
        "checkpoints",
        ("tenant_id", "run_id", "message_sequence"),
    )
    op.drop_column("checkpoints", "tool_call_id")
