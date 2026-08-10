"""Add session memory settings, provenance, and extraction jobs.

Revision ID: 0006
Revises: 0005
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
UTC_TIMESTAMP = sa.DateTime(timezone=True)
JSONB = postgresql.JSONB()


def upgrade() -> None:
    """Add PR 24 memory state; versioned task plans already exist from PR 14."""

    op.add_column(
        "sessions",
        sa.Column(
            "memory_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
    )
    op.create_table(
        "memories",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("session_id", UUID, nullable=False),
        sa.Column("source_run_id", UUID, nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column(
            "metadata",
            JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("extracted_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("archived_at", UTC_TIMESTAMP),
        sa.ForeignKeyConstraint(
            ("tenant_id", "session_id"),
            ("sessions.tenant_id", "sessions.id"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("tenant_id", "source_run_id", "session_id"),
            ("runs.tenant_id", "runs.id", "runs.session_id"),
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "kind IN ('fact', 'preference', 'decision', 'constraint')",
            name="ck_memories_kind",
        ),
        sa.CheckConstraint(
            "octet_length(content) > 0 AND octet_length(content) <= 65536",
            name="ck_memories_content_bytes",
        ),
        sa.CheckConstraint(
            "content_hash ~ '^[0-9a-f]{64}$'",
            name="ck_memories_content_hash",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(metadata) = 'object'",
            name="ck_memories_metadata_object",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "session_id",
            "kind",
            "content_hash",
            name="uq_memories_memory_identity",
        ),
        sa.CheckConstraint(
            "archived_at IS NULL OR archived_at >= extracted_at",
            name="ck_memories_archive_timestamp",
        ),
    )
    op.create_index(
        "ix_memories_tenant_session_active",
        "memories",
        ["tenant_id", "session_id", "extracted_at"],
        postgresql_where=sa.text("archived_at IS NULL"),
    )
    op.create_table(
        "memory_extraction_jobs",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("session_id", UUID, nullable=False),
        sa.Column("run_id", UUID, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("source_message_sequence", sa.BigInteger(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("worker_id", sa.String(255)),
        sa.Column("lease_token", UUID),
        sa.Column(
            "lease_generation",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("lease_expires_at", UTC_TIMESTAMP),
        sa.Column("error", JSONB),
        sa.Column(
            "created_at",
            UTC_TIMESTAMP,
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("started_at", UTC_TIMESTAMP),
        sa.Column("completed_at", UTC_TIMESTAMP),
        sa.ForeignKeyConstraint(
            ("tenant_id", "run_id", "session_id"),
            ("runs.tenant_id", "runs.id", "runs.session_id"),
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "run_id",
            name="uq_memory_extraction_jobs_tenant_id_run_id",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed')",
            name="ck_memory_extraction_jobs_status",
        ),
        sa.CheckConstraint(
            "attempt >= 1 AND attempt <= 100",
            name="ck_memory_extraction_jobs_attempt",
        ),
        sa.CheckConstraint(
            "lease_generation >= 0",
            name="ck_memory_extraction_jobs_lease_generation",
        ),
        sa.CheckConstraint(
            "source_message_sequence >= 0",
            name="ck_memory_extraction_jobs_source_message_sequence",
        ),
        sa.CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="ck_memory_extraction_jobs_error_object",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND started_at IS NULL AND completed_at IS NULL "
            "AND error IS NULL AND worker_id IS NULL AND lease_token IS NULL "
            "AND lease_generation = 0 AND lease_expires_at IS NULL) "
            "OR (status = 'running' AND started_at >= created_at "
            "AND completed_at IS NULL AND error IS NULL AND worker_id IS NOT NULL "
            "AND lease_token IS NOT NULL AND lease_generation >= 1 "
            "AND lease_expires_at > started_at) "
            "OR (status = 'completed' AND started_at >= created_at "
            "AND completed_at >= started_at AND error IS NULL AND worker_id IS NULL "
            "AND lease_token IS NULL AND lease_expires_at IS NULL) "
            "OR (status = 'failed' AND started_at >= created_at "
            "AND completed_at >= started_at AND error IS NOT NULL AND worker_id IS NULL "
            "AND lease_token IS NULL AND lease_expires_at IS NULL)",
            name="ck_memory_extraction_jobs_lifecycle",
        ),
    )
    op.create_index(
        "ix_memory_jobs_status_lease_created",
        "memory_extraction_jobs",
        ["status", "lease_expires_at", "created_at"],
    )
    op.create_index(
        "uq_memory_jobs_active_lease_token",
        "memory_extraction_jobs",
        ["lease_token"],
        unique=True,
        postgresql_where=sa.text("lease_token IS NOT NULL"),
    )


def downgrade() -> None:
    """Remove PR 24 memory state while preserving earlier task-plan history."""

    op.drop_index("uq_memory_jobs_active_lease_token", table_name="memory_extraction_jobs")
    op.drop_index("ix_memory_jobs_status_lease_created", table_name="memory_extraction_jobs")
    op.drop_table("memory_extraction_jobs")
    op.drop_index("ix_memories_tenant_session_active", table_name="memories")
    op.drop_table("memories")
    op.drop_column("sessions", "memory_enabled")
