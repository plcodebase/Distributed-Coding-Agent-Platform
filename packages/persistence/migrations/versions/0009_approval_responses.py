"""Add bounded interaction responses and one approval per tool call.

Revision ID: 0009
Revises: 0008
Create Date: 2026-08-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Persist bounded human responses and remove ambiguous approval joins."""

    op.add_column("approvals", sa.Column("response", sa.Text()))
    op.create_check_constraint(
        "ck_approvals_response_bytes",
        "approvals",
        "response IS NULL OR octet_length(response) BETWEEN 1 AND 65536",
    )
    op.create_unique_constraint(
        "uq_approvals_tenant_id_run_id_tool_call_id",
        "approvals",
        ["tenant_id", "run_id", "tool_call_id"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_approvals_tenant_id_run_id_tool_call_id",
        "approvals",
        type_="unique",
    )
    op.drop_constraint("ck_approvals_response_bytes", "approvals", type_="check")
    op.drop_column("approvals", "response")
