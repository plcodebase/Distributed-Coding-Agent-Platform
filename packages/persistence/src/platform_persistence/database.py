"""Validated PostgreSQL engine and transaction composition."""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Self

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from agent_core.domain.errors import DomainOperationError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from types import TracebackType

type PostgreSQLUrl = SecretStr
MAX_DATABASE_URL_LENGTH = 4096


class DatabaseSettings(BaseSettings):
    """Closed PostgreSQL connection and pool limits."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_PLATFORM_",
        extra="forbid",
        frozen=True,
    )

    database_url: PostgreSQLUrl = SecretStr(
        "postgresql+asyncpg://agent:agent@127.0.0.1:5432/agent_platform"
    )
    database_pool_size: int = Field(default=10, ge=1, le=100)
    database_max_overflow: int = Field(default=10, ge=0, le=100)
    database_pool_timeout_seconds: float = Field(default=10, gt=0, le=120)
    database_statement_timeout_ms: int = Field(default=30_000, ge=100, le=300_000)

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if not 1 <= len(raw) <= MAX_DATABASE_URL_LENGTH:
            raise ValueError("database_url must be between 1 and 4096 characters")
        if not raw.startswith("postgresql+asyncpg://"):
            raise ValueError("database_url must use postgresql+asyncpg")
        return value


class Database(AbstractAsyncContextManager["Database"]):
    """Owned async engine and session factory with explicit cleanup."""

    def __init__(
        self,
        settings: DatabaseSettings,
        *,
        engine: AsyncEngine | None = None,
    ) -> None:
        self.settings = settings
        self.engine = engine or create_async_engine(
            settings.database_url.get_secret_value(),
            pool_pre_ping=True,
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
            pool_timeout=settings.database_pool_timeout_seconds,
            connect_args={
                "server_settings": {
                    "application_name": "agent-platform",
                    "statement_timeout": str(settings.database_statement_timeout_ms),
                    "timezone": "UTC",
                }
            },
        )
        self.sessions = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )
        self._close_lock = asyncio.Lock()
        self._closed = False

    async def __aenter__(self) -> Self:
        if self._closed:
            raise RuntimeError("database is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        await self.aclose()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """Yield one transaction that commits only after the caller succeeds."""

        if self._closed:
            raise RuntimeError("database is closed")
        async with self.sessions() as session, session.begin():
            yield session

    async def ready(self) -> bool:
        """Return whether PostgreSQL can execute a bounded readiness query."""

        if self._closed:
            return False
        try:
            async with self.sessions() as session:
                await session.execute(text("SELECT 1"))
        except Exception:
            return False
        return True

    async def aclose(self) -> None:
        cleanup = asyncio.create_task(self._aclose())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise

    async def _aclose(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            try:
                await self.engine.dispose()
            except Exception as error:
                raise DomainOperationError(
                    code="database_cleanup_failed",
                    message="the database engine could not be closed",
                    retryable=True,
                ) from error
            self._closed = True


__all__ = ["Database", "DatabaseSettings", "PostgreSQLUrl"]
