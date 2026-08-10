"""Add bounded gateway capacity and tenant quotas.

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
UTC_TIMESTAMP = sa.DateTime(timezone=True)


def upgrade() -> None:
    """Add PR 21 quota and expiring distributed gateway-capacity state."""

    op.create_table(
        "tenant_quotas",
        sa.Column("tenant_id", UUID, primary_key=True),
        sa.Column("max_active_runs", sa.Integer(), nullable=False),
        sa.Column("max_queued_runs", sa.Integer(), nullable=False),
        sa.Column("max_gateway_requests", sa.Integer(), nullable=False),
        sa.Column(
            "memory_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
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
        sa.CheckConstraint(
            "max_active_runs >= 1 AND max_active_runs <= 10000",
            name="ck_tenant_quotas_max_active_runs",
        ),
        sa.CheckConstraint(
            "max_queued_runs >= 1 AND max_queued_runs <= 100000",
            name="ck_tenant_quotas_max_queued_runs",
        ),
        sa.CheckConstraint(
            "max_gateway_requests >= 1 AND max_gateway_requests <= 10000",
            name="ck_tenant_quotas_max_gateway_requests",
        ),
        sa.CheckConstraint(
            "updated_at >= created_at",
            name="ck_tenant_quotas_timestamp_order",
        ),
    )

    op.create_table(
        "gateway_provider_capacity",
        sa.Column("route_name", sa.String(100), primary_key=True),
        sa.Column("request_limit", sa.Integer(), nullable=False),
        sa.Column("token_limit", sa.BigInteger(), nullable=False),
        sa.Column("token_window_seconds", sa.Numeric(10, 3), nullable=False),
        sa.Column("token_window_started_at", UTC_TIMESTAMP, nullable=False),
        sa.Column(
            "accounted_tokens",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("updated_at", UTC_TIMESTAMP, nullable=False),
        sa.CheckConstraint(
            "request_limit >= 1 AND request_limit <= 10000",
            name="ck_gateway_provider_capacity_request_limit",
        ),
        sa.CheckConstraint(
            "token_limit >= 1 AND token_limit <= 1000000000",
            name="ck_gateway_provider_capacity_token_limit",
        ),
        sa.CheckConstraint(
            "token_window_seconds > 0 AND token_window_seconds <= 3600",
            name="ck_gateway_provider_capacity_token_window_seconds",
        ),
        sa.CheckConstraint(
            "accounted_tokens >= 0",
            name="ck_gateway_provider_capacity_accounted_tokens",
        ),
        sa.CheckConstraint(
            "updated_at >= token_window_started_at",
            name="ck_gateway_provider_capacity_timestamp_order",
        ),
    )

    op.create_table(
        "gateway_capacity_leases",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("tenant_id", UUID, nullable=False),
        sa.Column("route_name", sa.String(100), nullable=False),
        sa.Column("request_id", sa.String(255), nullable=False),
        sa.Column("reserved_tokens", sa.BigInteger(), nullable=False),
        sa.Column("token_window_started_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("acquired_at", UTC_TIMESTAMP, nullable=False),
        sa.Column("expires_at", UTC_TIMESTAMP, nullable=False),
        sa.CheckConstraint(
            "reserved_tokens >= 1",
            name="ck_gateway_capacity_leases_reserved_tokens",
        ),
        sa.CheckConstraint(
            "expires_at > acquired_at",
            name="ck_gateway_capacity_leases_expiry_order",
        ),
        sa.ForeignKeyConstraint(
            ("tenant_id",),
            ("tenant_quotas.tenant_id",),
            name="fk_gateway_capacity_leases_tenant_id_tenant_quotas",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ("route_name",),
            ("gateway_provider_capacity.route_name",),
            name="fk_gateway_capacity_leases_route_name_gateway_provider_capacity",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "request_id",
            name="uq_gateway_capacity_leases_tenant_id_request_id",
        ),
    )
    op.create_index(
        "ix_gateway_capacity_leases_expiry",
        "gateway_capacity_leases",
        ["expires_at"],
    )
    op.create_index(
        "ix_gateway_capacity_leases_tenant_expiry",
        "gateway_capacity_leases",
        ["tenant_id", "expires_at"],
    )
    op.create_index(
        "ix_gateway_capacity_leases_route_expiry",
        "gateway_capacity_leases",
        ["route_name", "expires_at"],
    )


def downgrade() -> None:
    """Remove PR 21 capacity state."""

    op.drop_index(
        "ix_gateway_capacity_leases_route_expiry",
        table_name="gateway_capacity_leases",
    )
    op.drop_index(
        "ix_gateway_capacity_leases_tenant_expiry",
        table_name="gateway_capacity_leases",
    )
    op.drop_index(
        "ix_gateway_capacity_leases_expiry",
        table_name="gateway_capacity_leases",
    )
    op.drop_table("gateway_capacity_leases")
    op.drop_table("gateway_provider_capacity")
    op.drop_table("tenant_quotas")
