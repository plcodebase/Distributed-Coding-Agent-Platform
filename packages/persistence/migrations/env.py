"""Async Alembic environment for the PostgreSQL system of record."""

from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig
from typing import TYPE_CHECKING

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from platform_persistence import models  # noqa: F401 - registers migration metadata
from platform_persistence.base import Base

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def database_url() -> str:
    """Load the migration URL without logging or embedding credentials."""

    value = os.environ.get("AGENT_PLATFORM_DATABASE_URL")
    if value is None or not value.startswith("postgresql+asyncpg://"):
        raise RuntimeError("AGENT_PLATFORM_DATABASE_URL must use postgresql+asyncpg")
    return value


def run_migrations_offline() -> None:
    """Emit PostgreSQL migration SQL without opening a connection."""

    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_server_default=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = database_url()
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
