"""Add durable, non-destructive context compaction requests.

Revision ID: 0005
Revises: 0004
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
UTC_TIMESTAMP = sa.DateTime(timezone=True)
JSONB = postgresql.JSONB()


def upgrade() -> None:
    """Add PR 23 compaction metadata without altering durable messages."""

    op.create_table(
        "context_compactions",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("session_id", UUID, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("source_message_sequence", sa.BigInteger(), nullable=False),
        sa.Column("route_name", sa.String(255), nullable=False),
        sa.Column("summary", sa.Text()),
        sa.Column("input_tokens", sa.BigInteger()),
        sa.Column("output_tokens", sa.BigInteger()),
        sa.Column("error", JSONB),
        sa.Column(
            "requested_at",
            UTC_TIMESTAMP,
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", UTC_TIMESTAMP),
        sa.ForeignKeyConstraint(
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "session_id",
            "idempotency_key",
            name="uq_context_compactions_tenant_id_session_id_idempotency_key",
        ),
        sa.CheckConstraint(
            "source_message_sequence >= 0",
            name="ck_context_compactions_source_message_sequence",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'completed', 'failed')",
            name="ck_context_compactions_status",
        ),
        sa.CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="ck_context_compactions_error_object",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND summary IS NULL AND input_tokens IS NULL "
            "AND output_tokens IS NULL AND error IS NULL AND completed_at IS NULL) "
            "OR (status = 'completed' AND summary IS NOT NULL "
            "AND octet_length(summary) > 0 AND octet_length(summary) <= 262144 "
            "AND input_tokens >= 0 AND output_tokens >= 0 AND error IS NULL "
            "AND completed_at >= requested_at) "
            "OR (status = 'failed' AND summary IS NULL AND input_tokens IS NULL "
            "AND output_tokens IS NULL AND error IS NOT NULL "
            "AND completed_at >= requested_at)",
            name="ck_context_compactions_terminal_outcome",
        ),
    )
    op.create_index(
        "ix_context_compactions_tenant_session_requested",
        "context_compactions",
        ["tenant_id", "session_id", "requested_at"],
    )
    op.create_index(
        "uq_context_compactions_one_pending",
        "context_compactions",
        ["tenant_id", "session_id"],
        unique=True,
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    """Remove PR 23 compaction metadata only."""

    op.drop_index(
        "uq_context_compactions_one_pending",
        table_name="context_compactions",
    )
    op.drop_index(
        "ix_context_compactions_tenant_session_requested",
        table_name="context_compactions",
    )
    op.drop_table("context_compactions")
