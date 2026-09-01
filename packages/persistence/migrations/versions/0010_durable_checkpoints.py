"""Add the durable post-tool state associated with a pre-tool checkpoint.

Revision ID: 0010
Revises: 0009
Create Date: 2026-08-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Store a checksum-bound post-tool snapshot for cross-worker recovery."""

    op.add_column("checkpoints", sa.Column("completed_snapshot_uri", sa.Text()))
    op.add_column("checkpoints", sa.Column("completed_revision", sa.String(length=255)))
    op.create_check_constraint(
        "ck_checkpoints_completed_snapshot_pair",
        "checkpoints",
        "(completed_snapshot_uri IS NULL) = (completed_revision IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_checkpoints_completed_snapshot_pair",
        "checkpoints",
        type_="check",
    )
    op.drop_column("checkpoints", "completed_revision")
    op.drop_column("checkpoints", "completed_snapshot_uri")
