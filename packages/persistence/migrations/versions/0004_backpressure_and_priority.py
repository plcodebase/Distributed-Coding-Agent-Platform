"""Add explicit priority classes and global queue admission.

Revision ID: 0004
Revises: 0003
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UTC_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    """Add PR 22 scheduling classes and serialized global admission state."""

    op.add_column(
        "runs",
        sa.Column(
            "priority_class",
            sa.String(32),
            nullable=False,
            server_default=sa.text("'interactive'"),
        ),
    )
    op.create_check_constraint(
        "ck_runs_priority",
        "runs",
        "priority >= -100 AND priority <= 100",
    )
    op.create_check_constraint(
        "ck_runs_priority_class",
        "runs",
        "priority_class IN ('interactive', 'background', 'evaluation')",
    )
    op.drop_index("ix_runs_queue_claim", table_name="runs")
    op.create_index(
        "ix_runs_queue_claim",
        "runs",
        ["status", "priority_class", sa.text("priority DESC"), "created_at"],
    )

    op.create_table(
        "queue_admission",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("global_queue_limit", sa.Integer(), nullable=False),
        sa.Column("retry_after_seconds", sa.Numeric(10, 3), nullable=False),
        sa.Column(
            "created_at",
            UTC_TIMESTAMP,
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            UTC_TIMESTAMP,
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("id = 1", name="ck_queue_admission_singleton"),
        sa.CheckConstraint(
            "global_queue_limit >= 1 AND global_queue_limit <= 1000000",
            name="ck_queue_admission_global_queue_limit",
        ),
        sa.CheckConstraint(
            "retry_after_seconds > 0 AND retry_after_seconds <= 3600",
            name="ck_queue_admission_retry_after_seconds",
        ),
        sa.CheckConstraint(
            "updated_at >= created_at",
            name="ck_queue_admission_timestamp_order",
        ),
    )


def downgrade() -> None:
    """Remove PR 22 admission and priority-class state."""

    op.drop_table("queue_admission")
    op.drop_index("ix_runs_queue_claim", table_name="runs")
    op.create_index(
        "ix_runs_queue_claim",
        "runs",
        ["status", sa.text("priority DESC"), "created_at"],
    )
    op.drop_constraint("ck_runs_priority_class", "runs", type_="check")
    op.drop_constraint("ck_runs_priority", "runs", type_="check")
    op.drop_column("runs", "priority_class")
