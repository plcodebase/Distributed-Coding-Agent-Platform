"""Redis list notifications used only to reduce durable PostgreSQL queue latency."""

from __future__ import annotations

import asyncio
import logging
import math
from typing import TYPE_CHECKING, Annotated, cast

from pydantic import Field, SecretStr, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis
from redis.exceptions import RedisError

if TYPE_CHECKING:
    import uuid
    from collections.abc import Awaitable, Callable

_MAX_WAKEUPS = 10_000
_MAX_KEY_CHARS = 128
_MIN_RETENTION_SECONDS = 60
_MAX_RETENTION_SECONDS = 86_400
_LOGGER = logging.getLogger(__name__)


class RedisWakeupSettings(BaseSettings):
    """Validated Redis connection and namespaced list settings."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_PLATFORM_REDIS_",
        case_sensitive=False,
        extra="ignore",
        frozen=True,
    )

    url: SecretStr = SecretStr("redis://127.0.0.1:6379/0")
    key: Annotated[
        str,
        StringConstraints(pattern=r"^[a-z][a-z0-9:._-]{0,127}$"),
    ] = "agent-platform:run-wakeups"
    socket_timeout_seconds: float = Field(default=5, gt=0, le=60)
    retention_seconds: int = Field(default=3600, ge=60, le=86_400)


class RedisRunWakeup:
    """Publish durable hints and wait with a safe polling fallback on Redis failure."""

    def __init__(
        self,
        client: Redis,
        *,
        key: str,
        retention_seconds: int,
        fallback_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        owns_client: bool = False,
    ) -> None:
        if not key or len(key) > _MAX_KEY_CHARS:
            raise ValueError("Redis wakeup key must be nonempty bounded text")
        if type(retention_seconds) is not int or not (
            _MIN_RETENTION_SECONDS <= retention_seconds <= _MAX_RETENTION_SECONDS
        ):
            raise ValueError("Redis wakeup retention must be in [60, 86400]")
        if not callable(fallback_sleep):
            raise TypeError("fallback_sleep must be callable")
        self._client = client
        self._key = key
        self._retention = retention_seconds
        self._fallback_sleep = fallback_sleep
        self._owns_client = owns_client
        self._closed = False

    @classmethod
    def create(cls, settings: RedisWakeupSettings) -> RedisRunWakeup:
        client = Redis.from_url(
            settings.url.get_secret_value(),
            decode_responses=False,
            socket_connect_timeout=settings.socket_timeout_seconds,
            socket_timeout=settings.socket_timeout_seconds,
            retry_on_timeout=False,
        )
        return cls(
            client,
            key=settings.key,
            retention_seconds=settings.retention_seconds,
            owns_client=True,
        )

    async def publish(self, run_id: uuid.UUID) -> None:
        self._require_open()
        pipeline = self._client.pipeline(transaction=True)
        pipeline.lpush(self._key, run_id.hex.encode("ascii"))
        pipeline.ltrim(self._key, 0, _MAX_WAKEUPS - 1)
        pipeline.expire(self._key, self._retention)
        try:
            await pipeline.execute()
        except RedisError:
            # PostgreSQL is authoritative and workers poll it even when this
            # lossy latency hint cannot be published.
            _LOGGER.warning("Redis run wakeup publication failed; polling will recover")

    async def wait(self, timeout_seconds: float) -> None:
        self._require_open()
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("wakeup wait timeout must be positive finite seconds")
        try:
            operation = self._client.blpop(
                [self._key],
                timeout=max(1, math.ceil(timeout_seconds)),
            )
            await cast("Awaitable[object]", operation)
        except RedisError:
            await self._fallback_sleep(timeout_seconds)

    async def ready(self) -> bool:
        if self._closed:
            return False
        try:
            return bool(await self._client.ping())
        except RedisError:
            return False

    async def aclose(self) -> None:
        if self._closed:
            return
        if self._owns_client:
            await self._client.aclose()
        self._closed = True

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("Redis run wakeup is closed")


__all__ = ["RedisRunWakeup", "RedisWakeupSettings"]
