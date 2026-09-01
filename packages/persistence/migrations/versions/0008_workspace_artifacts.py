"""Add immutable workspace snapshots and artifact metadata.

Revision ID: 0008
Revises: 0007
Create Date: 2026-08-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
UTC_TIMESTAMP = sa.DateTime(timezone=True)
JSONB = postgresql.JSONB()


def upgrade() -> None:
    """Create tenant-owned logical workspaces over immutable object artifacts."""

    op.create_table(
        "workspaces",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("display_name", sa.String(255), nullable=False),
        sa.Column("current_snapshot_id", UUID),
        sa.Column("version", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("tenant_id", "id", name="uq_workspaces_tenant_id_id"),
        sa.CheckConstraint(
            "status IN ('pending', 'ready', 'archived')",
            name="ck_workspaces_status",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND current_snapshot_id IS NULL AND version = 0) "
            "OR (status = 'ready' AND current_snapshot_id IS NOT NULL AND version >= 1) "
            "OR status = 'archived'",
            name="ck_workspaces_lifecycle",
        ),
        sa.CheckConstraint(
            "octet_length(display_name) BETWEEN 1 AND 1024",
            name="ck_workspaces_display_name_bytes",
        ),
        sa.CheckConstraint(
            "updated_at >= created_at",
            name="ck_workspaces_timestamp_order",
        ),
    )
    op.create_index(
        "ix_workspaces_tenant_created",
        "workspaces",
        ["tenant_id", "created_at"],
    )

    op.create_table(
        "artifacts",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("workspace_id", UUID, nullable=False),
        sa.Column("run_id", UUID),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("object_key", sa.String(1024), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("content_type", sa.String(255), nullable=False),
        sa.Column("etag", sa.String(1024)),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", UTC_TIMESTAMP),
        sa.ForeignKeyConstraint(
            ("tenant_id", "workspace_id"),
            ("workspaces.tenant_id", "workspaces.id"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("tenant_id", "run_id", "workspace_id"),
            ("runs.tenant_id", "runs.id", "runs.workspace_id"),
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("tenant_id", "id", name="uq_artifacts_tenant_id_id"),
        sa.UniqueConstraint(
            "tenant_id",
            "workspace_id",
            "id",
            name="uq_artifacts_tenant_id_workspace_id_id",
        ),
        sa.UniqueConstraint("object_key", name="uq_artifacts_object_key"),
        sa.CheckConstraint(
            "kind IN ('source_snapshot', 'workspace_checkpoint', 'final_patch', "
            "'command_log', 'evaluation_report')",
            name="ck_artifacts_kind",
        ),
        sa.CheckConstraint(
            "object_key ~ '^[a-z0-9][a-z0-9._/-]{0,1023}$'",
            name="ck_artifacts_object_key",
        ),
        sa.CheckConstraint("sha256 ~ '^[0-9a-f]{64}$'", name="ck_artifacts_sha256"),
        sa.CheckConstraint("size_bytes >= 0", name="ck_artifacts_size_bytes"),
        sa.CheckConstraint(
            "octet_length(content_type) BETWEEN 3 AND 255",
            name="ck_artifacts_content_type",
        ),
        sa.CheckConstraint(
            "etag IS NULL OR octet_length(etag) BETWEEN 1 AND 1024",
            name="ck_artifacts_etag",
        ),
        sa.CheckConstraint(
            "expires_at IS NULL OR expires_at > created_at",
            name="ck_artifacts_retention",
        ),
    )
    op.create_index(
        "ix_artifacts_tenant_workspace_created",
        "artifacts",
        ["tenant_id", "workspace_id", "created_at"],
    )
    op.create_index(
        "ix_artifacts_tenant_run_created",
        "artifacts",
        ["tenant_id", "run_id", "created_at"],
    )

    op.create_table(
        "source_snapshots",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("workspace_id", UUID, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("object_key", sa.String(1024), nullable=False),
        sa.Column("expected_sha256", sa.String(64)),
        sa.Column("compressed_bytes", sa.BigInteger()),
        sa.Column("artifact_id", UUID),
        sa.Column("manifest_sha256", sa.String(64)),
        sa.Column("entry_count", sa.BigInteger()),
        sa.Column("expanded_bytes", sa.BigInteger()),
        sa.Column("error", JSONB),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ("tenant_id", "workspace_id"),
            ("workspaces.tenant_id", "workspaces.id"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("tenant_id", "workspace_id", "artifact_id"),
            ("artifacts.tenant_id", "artifacts.workspace_id", "artifacts.id"),
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "workspace_id",
            "id",
            name="uq_source_snapshots_tenant_id_workspace_id_id",
        ),
        sa.UniqueConstraint("object_key", name="uq_source_snapshots_object_key"),
        sa.CheckConstraint(
            "status IN ('pending', 'validating', 'ready', 'rejected')",
            name="ck_source_snapshots_status",
        ),
        sa.CheckConstraint(
            "object_key ~ '^[a-z0-9][a-z0-9._/-]{0,1023}$'",
            name="ck_source_snapshots_object_key",
        ),
        sa.CheckConstraint(
            "expected_sha256 IS NULL OR expected_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_source_snapshots_expected_sha256",
        ),
        sa.CheckConstraint(
            "manifest_sha256 IS NULL OR manifest_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_source_snapshots_manifest_sha256",
        ),
        sa.CheckConstraint(
            "compressed_bytes IS NULL OR compressed_bytes >= 0",
            name="ck_source_snapshots_compressed_bytes",
        ),
        sa.CheckConstraint(
            "entry_count IS NULL OR entry_count >= 0",
            name="ck_source_snapshots_entry_count",
        ),
        sa.CheckConstraint(
            "expanded_bytes IS NULL OR expanded_bytes >= 0",
            name="ck_source_snapshots_expanded_bytes",
        ),
        sa.CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="ck_source_snapshots_error_object",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND expected_sha256 IS NULL AND compressed_bytes IS NULL "
            "AND artifact_id IS NULL AND manifest_sha256 IS NULL AND entry_count IS NULL "
            "AND expanded_bytes IS NULL AND error IS NULL) "
            "OR (status = 'validating' AND expected_sha256 IS NOT NULL "
            "AND compressed_bytes IS NOT NULL AND artifact_id IS NULL "
            "AND manifest_sha256 IS NULL AND entry_count IS NULL AND expanded_bytes IS NULL "
            "AND error IS NULL) "
            "OR (status = 'ready' AND expected_sha256 IS NOT NULL "
            "AND compressed_bytes IS NOT NULL AND artifact_id IS NOT NULL "
            "AND manifest_sha256 IS NOT NULL AND entry_count IS NOT NULL "
            "AND expanded_bytes IS NOT NULL AND error IS NULL) "
            "OR (status = 'rejected' AND expected_sha256 IS NOT NULL "
            "AND compressed_bytes IS NOT NULL AND artifact_id IS NULL "
            "AND manifest_sha256 IS NULL AND entry_count IS NULL AND expanded_bytes IS NULL "
            "AND error IS NOT NULL)",
            name="ck_source_snapshots_lifecycle",
        ),
        sa.CheckConstraint(
            "updated_at >= created_at",
            name="ck_source_snapshots_timestamp_order",
        ),
    )
    op.create_index(
        "ix_source_snapshots_workspace_created",
        "source_snapshots",
        ["tenant_id", "workspace_id", "created_at"],
    )

    op.create_foreign_key(
        "fk_workspaces_current_snapshot",
        "workspaces",
        "source_snapshots",
        ["tenant_id", "id", "current_snapshot_id"],
        ["tenant_id", "workspace_id", "id"],
    )

    op.create_table(
        "snapshot_validation_jobs",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("workspace_id", UUID, nullable=False),
        sa.Column("snapshot_id", UUID, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("expected_workspace_version", sa.BigInteger(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("worker_id", sa.String(255)),
        sa.Column("lease_token", UUID),
        sa.Column("lease_generation", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("lease_expires_at", UTC_TIMESTAMP),
        sa.Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=sa.text("now()")),
        sa.Column("started_at", UTC_TIMESTAMP),
        sa.Column("completed_at", UTC_TIMESTAMP),
        sa.ForeignKeyConstraint(
            ("tenant_id", "workspace_id", "snapshot_id"),
            ("source_snapshots.tenant_id", "source_snapshots.workspace_id", "source_snapshots.id"),
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "snapshot_id",
            name="uq_snapshot_validation_jobs_tenant_id_snapshot_id",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed')",
            name="ck_snapshot_validation_jobs_status",
        ),
        sa.CheckConstraint(
            "attempt >= 1 AND attempt <= 100",
            name="ck_snapshot_validation_jobs_attempt",
        ),
        sa.CheckConstraint(
            "expected_workspace_version >= 0",
            name="ck_snapshot_validation_jobs_workspace_version",
        ),
        sa.CheckConstraint(
            "lease_generation >= 0",
            name="ck_snapshot_validation_jobs_lease_generation",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND worker_id IS NULL AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND completed_at IS NULL) "
            "OR (status = 'running' AND worker_id IS NOT NULL AND lease_token IS NOT NULL "
            "AND lease_generation >= 1 AND lease_expires_at > started_at "
            "AND started_at IS NOT NULL AND completed_at IS NULL) "
            "OR (status IN ('completed', 'failed') AND worker_id IS NULL AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND started_at IS NOT NULL "
            "AND completed_at >= started_at)",
            name="ck_snapshot_validation_jobs_lifecycle",
        ),
    )
    op.create_index(
        "ix_snapshot_validation_jobs_claim",
        "snapshot_validation_jobs",
        ["status", "lease_expires_at", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_snapshot_validation_jobs_claim", table_name="snapshot_validation_jobs")
    op.drop_table("snapshot_validation_jobs")
    op.drop_constraint("fk_workspaces_current_snapshot", "workspaces", type_="foreignkey")
    op.drop_index("ix_source_snapshots_workspace_created", table_name="source_snapshots")
    op.drop_table("source_snapshots")
    op.drop_index("ix_artifacts_tenant_run_created", table_name="artifacts")
    op.drop_index("ix_artifacts_tenant_workspace_created", table_name="artifacts")
    op.drop_table("artifacts")
    op.drop_index("ix_workspaces_tenant_created", table_name="workspaces")
    op.drop_table("workspaces")
