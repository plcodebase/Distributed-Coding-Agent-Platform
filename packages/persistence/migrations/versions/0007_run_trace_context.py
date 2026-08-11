"""Add durable W3C trace context for API-to-worker propagation.

Revision ID: 0007
Revises: 0006
Create Date: 2026-08-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Store only validated W3C routing metadata, never request content."""

    op.add_column("runs", sa.Column("traceparent", sa.String(55)))
    op.add_column("runs", sa.Column("tracestate", sa.String(512)))
    op.create_check_constraint(
        "ck_runs_traceparent",
        "runs",
        "traceparent IS NULL OR (traceparent ~ "
        "'^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$' "
        "AND split_part(traceparent, '-', 2) <> repeat('0', 32) "
        "AND split_part(traceparent, '-', 3) <> repeat('0', 16))",
    )
    op.create_check_constraint(
        "ck_runs_tracestate",
        "runs",
        "tracestate IS NULL OR (traceparent IS NOT NULL "
        "AND octet_length(tracestate) BETWEEN 1 AND 512)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_runs_tracestate", "runs", type_="check")
    op.drop_constraint("ck_runs_traceparent", "runs", type_="check")
    op.drop_column("runs", "tracestate")
    op.drop_column("runs", "traceparent")
