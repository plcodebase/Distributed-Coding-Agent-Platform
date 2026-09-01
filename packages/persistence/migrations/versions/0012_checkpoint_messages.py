"""Persist the complete normalized model transcript at each checkpoint.

Revision ID: 0012_checkpoint_messages
Revises: 0011_audit_log
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

if TYPE_CHECKING:
    from collections.abc import Sequence

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "checkpoints",
        sa.Column(
            "checkpoint_messages",
            JSONB,
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )
    op.create_check_constraint(
        "ck_checkpoints_messages_array",
        "checkpoints",
        "jsonb_typeof(checkpoint_messages) = 'array'",
    )


def downgrade() -> None:
    op.drop_constraint("ck_checkpoints_messages_array", "checkpoints", type_="check")
    op.drop_column("checkpoints", "checkpoint_messages")
