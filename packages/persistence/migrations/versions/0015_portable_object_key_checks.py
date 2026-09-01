"""Use PostgreSQL-compatible object-key regular expressions.

Revision ID: 0015
Revises: 0014
"""

from __future__ import annotations

from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | None = None
depends_on: str | None = None

_PORTABLE_OBJECT_KEY = "object_key ~ '^[a-z0-9][a-z0-9._/-]*$'"


def _replace(table: str) -> None:
    op.drop_constraint(f"ck_{table}_object_key", table, type_="check")
    op.create_check_constraint("object_key", table, _PORTABLE_OBJECT_KEY)


def upgrade() -> None:
    _replace("artifacts")
    _replace("source_snapshots")


def downgrade() -> None:
    # PostgreSQL cannot parse the superseded counted-repetition expression, so
    # retain the stricter portable constraint when moving to revision 0014.
    pass
