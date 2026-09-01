"""Add a durable eligibility timestamp for suspended run retries.

Revision ID: 0013_retry_schedule
Revises: 0012_checkpoint_messages
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("retry_ready_at", sa.DateTime(timezone=True)))
    op.create_check_constraint(
        "ck_runs_retry_ready_state",
        "runs",
        "(status = 'retry_pending') = (retry_ready_at IS NOT NULL)",
    )
    op.create_index(
        "ix_runs_retry_ready",
        "runs",
        ("status", "retry_ready_at"),
    )


def downgrade() -> None:
    op.drop_index("ix_runs_retry_ready", table_name="runs")
    op.drop_constraint("ck_runs_retry_ready_state", "runs", type_="check")
    op.drop_column("runs", "retry_ready_at")
