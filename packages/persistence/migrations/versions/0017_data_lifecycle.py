"""Add tenant lifecycle, legal holds, audit exports, and object cleanup jobs.

Revision ID: 0017
Revises: 0016
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0017"
down_revision: str | None = "0016"
branch_labels: str | None = None
depends_on: str | None = None

_TENANT_WRITE_TABLES = (
    "agent_events",
    "approvals",
    "artifacts",
    "checkpoints",
    "context_compactions",
    "gateway_capacity_leases",
    "gateway_rate_limits",
    "gateway_requests",
    "memories",
    "memory_extraction_jobs",
    "messages",
    "model_calls",
    "run_leases",
    "runs",
    "sessions",
    "snapshot_validation_jobs",
    "source_snapshots",
    "task_plans",
    "tenant_quotas",
    "tool_calls",
    "workspace_writer_leases",
    "workspaces",
)


def upgrade() -> None:
    op.create_check_constraint(
        "ck_audit_log_details_bytes",
        "audit_log",
        "octet_length(details::text) <= 1000000",
    )
    op.create_table(
        "audit_exports",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("requested_by", sa.String(length=255), nullable=False),
        sa.Column("cutoff_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("object_key", sa.String(length=1024), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("content_type", sa.String(length=255), nullable=True),
        sa.Column("etag", sa.String(length=1024), nullable=True),
        sa.Column("record_count", sa.BigInteger(), nullable=True),
        sa.Column("first_occurred_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_occurred_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.CheckConstraint("status IN ('pending', 'completed', 'failed')", name="status"),
        sa.CheckConstraint(
            "object_key IS NULL OR object_key ~ '^[a-z0-9][a-z0-9._/-]*$'",
            name="object_key",
        ),
        sa.CheckConstraint("sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'", name="sha256"),
        sa.CheckConstraint(
            "size_bytes IS NULL OR size_bytes BETWEEN 1 AND 1073741824",
            name="size",
        ),
        sa.CheckConstraint(
            "record_count IS NULL OR record_count BETWEEN 0 AND 10000000",
            name="record_count",
        ),
        sa.CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="error_object",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND object_key IS NULL AND sha256 IS NULL "
            "AND size_bytes IS NULL AND content_type IS NULL AND etag IS NULL "
            "AND record_count IS NULL AND first_occurred_at IS NULL "
            "AND last_occurred_at IS NULL AND completed_at IS NULL AND error IS NULL) "
            "OR (status = 'completed' AND object_key IS NOT NULL AND sha256 IS NOT NULL "
            "AND size_bytes IS NOT NULL AND content_type IS NOT NULL "
            "AND record_count IS NOT NULL AND completed_at >= created_at AND error IS NULL "
            "AND ((record_count = 0 AND first_occurred_at IS NULL "
            "AND last_occurred_at IS NULL) OR (record_count > 0 "
            "AND first_occurred_at IS NOT NULL AND last_occurred_at >= first_occurred_at "
            "AND last_occurred_at <= cutoff_at))) "
            "OR (status = 'failed' AND object_key IS NULL AND sha256 IS NULL "
            "AND size_bytes IS NULL AND content_type IS NULL AND etag IS NULL "
            "AND record_count IS NULL AND first_occurred_at IS NULL "
            "AND last_occurred_at IS NULL AND completed_at IS NULL AND error IS NOT NULL)",
            name="lifecycle",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_audit_exports"),
        sa.UniqueConstraint("tenant_id", "id", name="uq_audit_exports_tenant_id_id"),
        sa.UniqueConstraint("object_key", name="uq_audit_exports_object_key"),
    )
    op.create_index(
        "ix_audit_exports_tenant_created",
        "audit_exports",
        ["tenant_id", "created_at"],
    )

    op.create_table(
        "tenant_lifecycle",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("request_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("requested_by", sa.String(length=255), nullable=True),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delete_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("audit_export_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("deletion_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IN ('active', 'deletion_requested', 'deleting', 'deleted')",
            name="status",
        ),
        sa.CheckConstraint(
            "(status = 'active' AND request_id IS NULL AND requested_by IS NULL "
            "AND requested_at IS NULL AND delete_after IS NULL AND audit_export_id IS NULL "
            "AND deletion_started_at IS NULL AND deleted_at IS NULL) "
            "OR (status = 'deletion_requested' AND request_id IS NOT NULL "
            "AND requested_by IS NOT NULL AND requested_at IS NOT NULL "
            "AND delete_after >= requested_at AND audit_export_id IS NOT NULL "
            "AND deletion_started_at IS NULL AND deleted_at IS NULL) "
            "OR (status = 'deleting' AND request_id IS NOT NULL AND requested_by IS NOT NULL "
            "AND requested_at IS NOT NULL AND delete_after >= requested_at "
            "AND audit_export_id IS NOT NULL AND deletion_started_at >= delete_after "
            "AND deleted_at IS NULL) "
            "OR (status = 'deleted' AND request_id IS NOT NULL AND requested_by IS NOT NULL "
            "AND requested_at IS NOT NULL AND delete_after >= requested_at "
            "AND audit_export_id IS NOT NULL AND deletion_started_at >= delete_after "
            "AND deleted_at >= deletion_started_at)",
            name="lifecycle",
        ),
        sa.CheckConstraint(
            "requested_by IS NULL OR octet_length(requested_by) BETWEEN 1 AND 1024",
            name="requested_by_bytes",
        ),
        sa.CheckConstraint(
            "requested_at IS NULL OR updated_at >= requested_at",
            name="updated_after_request",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "audit_export_id"],
            ["audit_exports.tenant_id", "audit_exports.id"],
            name="fk_tenant_lifecycle_audit_export",
        ),
        sa.PrimaryKeyConstraint("tenant_id", name="pk_tenant_lifecycle"),
        sa.UniqueConstraint("request_id", name="uq_tenant_lifecycle_request_id"),
    )
    op.create_index(
        "ix_tenant_lifecycle_status_delete_after",
        "tenant_lifecycle",
        ["status", "delete_after"],
    )

    op.create_table(
        "legal_holds",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reason", sa.String(length=2000), nullable=False),
        sa.Column("placed_by", sa.String(length=255), nullable=False),
        sa.Column("placed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("released_by", sa.String(length=255), nullable=True),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("octet_length(reason) BETWEEN 1 AND 8000", name="reason_bytes"),
        sa.CheckConstraint(
            "octet_length(placed_by) BETWEEN 1 AND 1024",
            name="placed_by_bytes",
        ),
        sa.CheckConstraint("expires_at IS NULL OR expires_at > placed_at", name="expiry_order"),
        sa.CheckConstraint(
            "(released_by IS NULL AND released_at IS NULL) OR "
            "(released_by IS NOT NULL AND released_at >= placed_at)",
            name="release_state",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_legal_holds"),
    )
    op.create_index(
        "ix_legal_holds_tenant_release_expiry",
        "legal_holds",
        ["tenant_id", "released_at", "expires_at"],
    )

    op.create_table(
        "object_deletion_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("artifact_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("object_key", sa.String(length=1024), nullable=False),
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=True),
        sa.Column("lease_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("outcome_lease_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("lease_generation", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("attempt", sa.Integer(), server_default="0", nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("object_key ~ '^[a-z0-9][a-z0-9._/-]*$'", name="object_key"),
        sa.CheckConstraint(
            "reason IN ('retention_expired', 'tenant_deletion')",
            name="reason",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed')",
            name="status",
        ),
        sa.CheckConstraint("attempt BETWEEN 0 AND 100", name="attempt"),
        sa.CheckConstraint("lease_generation >= 0", name="lease_generation"),
        sa.CheckConstraint(
            "error IS NULL OR jsonb_typeof(error) = 'object'",
            name="error_object",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND worker_id IS NULL AND lease_token IS NULL "
            "AND outcome_lease_token IS NULL AND lease_expires_at IS NULL "
            "AND completed_at IS NULL) "
            "OR (status = 'running' AND worker_id IS NOT NULL AND lease_token IS NOT NULL "
            "AND outcome_lease_token IS NULL "
            "AND lease_generation >= 1 AND attempt >= 1 AND lease_expires_at > updated_at "
            "AND completed_at IS NULL AND error IS NULL) "
            "OR (status = 'completed' AND worker_id IS NULL AND lease_token IS NULL "
            "AND outcome_lease_token IS NOT NULL AND lease_expires_at IS NULL "
            "AND completed_at >= created_at AND error IS NULL) "
            "OR (status = 'failed' AND worker_id IS NULL AND lease_token IS NULL "
            "AND outcome_lease_token IS NOT NULL AND lease_expires_at IS NULL "
            "AND completed_at IS NULL AND error IS NOT NULL)",
            name="lifecycle",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "artifact_id"],
            ["artifacts.tenant_id", "artifacts.id"],
            name="fk_object_deletion_jobs_artifact",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_object_deletion_jobs"),
        sa.UniqueConstraint("object_key", name="uq_object_deletion_jobs_object_key"),
        sa.UniqueConstraint("lease_token", name="uq_object_deletion_jobs_lease_token"),
        sa.UniqueConstraint(
            "outcome_lease_token",
            name="uq_object_deletion_jobs_outcome_lease_token",
        ),
    )
    op.create_index(
        "ix_object_deletion_jobs_claim",
        "object_deletion_jobs",
        ["status", "lease_expires_at", "created_at"],
    )
    op.create_index(
        "ix_object_deletion_jobs_tenant_status",
        "object_deletion_jobs",
        ["tenant_id", "status"],
    )
    op.execute(
        """
        CREATE FUNCTION agent_require_active_tenant_write()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY INVOKER
        SET search_path = pg_catalog, public
        AS $function$
        BEGIN
            IF pg_catalog.current_setting('agent_platform.lifecycle_admin', true) = 'on' THEN
                RETURN NEW;
            END IF;
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended(NEW.tenant_id::text, 280368598612)
            );
            IF EXISTS (
                SELECT 1
                FROM public.tenant_lifecycle
                WHERE tenant_id = NEW.tenant_id
                  AND status <> 'active'
            ) THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'check_violation',
                    MESSAGE = 'tenant is not accepting application writes',
                    CONSTRAINT = 'active_tenant_write';
            END IF;
            RETURN NEW;
        END;
        $function$
        """
    )
    for table in _TENANT_WRITE_TABLES:
        op.execute(
            f"""
            CREATE TRIGGER trg_{table}_active_tenant_write
            BEFORE INSERT OR UPDATE ON {table}
            FOR EACH ROW EXECUTE FUNCTION agent_require_active_tenant_write()
            """
        )


def downgrade() -> None:
    for table in reversed(_TENANT_WRITE_TABLES):
        op.execute(f"DROP TRIGGER trg_{table}_active_tenant_write ON {table}")
    op.execute("DROP FUNCTION agent_require_active_tenant_write()")
    op.drop_index("ix_object_deletion_jobs_tenant_status", table_name="object_deletion_jobs")
    op.drop_index("ix_object_deletion_jobs_claim", table_name="object_deletion_jobs")
    op.drop_table("object_deletion_jobs")
    op.drop_index("ix_legal_holds_tenant_release_expiry", table_name="legal_holds")
    op.drop_table("legal_holds")
    op.drop_index("ix_tenant_lifecycle_status_delete_after", table_name="tenant_lifecycle")
    op.drop_table("tenant_lifecycle")
    op.drop_index("ix_audit_exports_tenant_created", table_name="audit_exports")
    op.drop_table("audit_exports")
    op.drop_constraint("ck_audit_log_details_bytes", "audit_log", type_="check")
