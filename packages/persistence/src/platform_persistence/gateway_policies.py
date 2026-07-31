"""Shared PostgreSQL gateway admission and circuit policies."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Annotated

from pydantic import StringConstraints, TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from platform_persistence.models import GatewayCircuitRecord, GatewayRateLimitRecord

if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

MAX_POLICY_SECONDS = 3600.0
MAX_RATE_LIMIT_REQUESTS = 1_000_000
MAX_CIRCUIT_FAILURE_THRESHOLD = 100
type PolicyRouteName = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=100,
        pattern=r"^[a-z][a-z0-9-]*$",
    ),
]
_ROUTE_NAME_ADAPTER: TypeAdapter[PolicyRouteName] = TypeAdapter(PolicyRouteName)


class PostgresGatewayRateLimiter:
    """Transactionally enforce a shared tenant-and-route fixed window."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        requests_per_window: int,
        window_seconds: float,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if (
            type(requests_per_window) is not int
            or not 1 <= requests_per_window <= MAX_RATE_LIMIT_REQUESTS
        ):
            raise ValueError("requests_per_window must be in [1, 1000000]")
        if (
            isinstance(window_seconds, bool)
            or not isinstance(window_seconds, (int, float))
            or not math.isfinite(window_seconds)
            or not 0 < window_seconds <= MAX_POLICY_SECONDS
        ):
            raise ValueError("window_seconds must be in (0, 3600]")
        self._sessions = sessions
        self._limit = requests_per_window
        self._window = timedelta(seconds=window_seconds)
        self._clock = clock

    async def acquire(self, tenant_id: uuid.UUID, route_name: str) -> float | None:
        route_name = _route_name(route_name)
        now = _aware_utc(self._clock())
        async with self._sessions() as database, database.begin():
            await database.execute(
                insert(GatewayRateLimitRecord)
                .values(
                    tenant_id=tenant_id,
                    route_name=route_name,
                    window_started_at=now,
                    request_count=0,
                )
                .on_conflict_do_nothing(
                    index_elements=(
                        GatewayRateLimitRecord.tenant_id,
                        GatewayRateLimitRecord.route_name,
                    )
                )
            )
            row = await database.scalar(
                select(GatewayRateLimitRecord)
                .where(
                    GatewayRateLimitRecord.tenant_id == tenant_id,
                    GatewayRateLimitRecord.route_name == route_name,
                )
                .with_for_update()
            )
            if row is None:
                raise RuntimeError("gateway rate-limit row disappeared")
            elapsed = now - row.window_started_at
            if elapsed >= self._window:
                row.window_started_at = now
                row.request_count = 1
                return None
            if row.request_count >= self._limit:
                return max(0.0, (self._window - elapsed).total_seconds())
            row.request_count += 1
            return None


class PostgresGatewayCircuitBreaker:
    """Transactionally share closed/open/half-open state across workers."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        failure_threshold: int,
        recovery_seconds: float,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if (
            type(failure_threshold) is not int
            or not 1 <= failure_threshold <= MAX_CIRCUIT_FAILURE_THRESHOLD
        ):
            raise ValueError("failure_threshold must be in [1, 100]")
        if (
            isinstance(recovery_seconds, bool)
            or not isinstance(recovery_seconds, (int, float))
            or not math.isfinite(recovery_seconds)
            or not 0 < recovery_seconds <= MAX_POLICY_SECONDS
        ):
            raise ValueError("recovery_seconds must be in (0, 3600]")
        self._sessions = sessions
        self._threshold = failure_threshold
        self._recovery = timedelta(seconds=recovery_seconds)
        self._clock = clock

    async def allow(self, route_name: str) -> bool:
        route_name = _route_name(route_name)
        now = _aware_utc(self._clock())
        async with self._sessions() as database, database.begin():
            row = await self._locked_row(database, route_name, now)
            if row.opened_at is None:
                return True
            if now - row.opened_at < self._recovery:
                return False
            if row.probe_in_flight:
                if row.probe_started_at is not None and now - row.probe_started_at < self._recovery:
                    return False
                row.probe_in_flight = False
                row.probe_started_at = None
            row.probe_in_flight = True
            row.probe_started_at = now
            row.updated_at = now
            return True

    async def record_success(self, route_name: str) -> None:
        route_name = _route_name(route_name)
        now = _aware_utc(self._clock())
        async with self._sessions() as database, database.begin():
            row = await self._locked_row(database, route_name, now)
            row.failure_count = 0
            row.opened_at = None
            row.probe_in_flight = False
            row.probe_started_at = None
            row.updated_at = now

    async def record_failure(self, route_name: str) -> None:
        route_name = _route_name(route_name)
        now = _aware_utc(self._clock())
        async with self._sessions() as database, database.begin():
            row = await self._locked_row(database, route_name, now)
            row.failure_count += 1
            row.probe_in_flight = False
            row.probe_started_at = None
            if row.failure_count >= self._threshold:
                row.opened_at = now
            row.updated_at = now

    @staticmethod
    async def _locked_row(
        database: AsyncSession,
        route_name: str,
        now: datetime,
    ) -> GatewayCircuitRecord:
        await database.execute(
            insert(GatewayCircuitRecord)
            .values(
                route_name=route_name,
                failure_count=0,
                probe_in_flight=False,
                probe_started_at=None,
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=(GatewayCircuitRecord.route_name,))
        )
        row = await database.scalar(
            select(GatewayCircuitRecord)
            .where(GatewayCircuitRecord.route_name == route_name)
            .with_for_update()
        )
        if row is None:
            raise RuntimeError("gateway circuit row disappeared")
        return row


def _aware_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("gateway policy clock must return a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("gateway policy clock must return an aware timestamp")
    return value.astimezone(UTC)


def _route_name(value: str) -> str:
    try:
        validated = _ROUTE_NAME_ADAPTER.validate_python(value)
    except ValidationError:
        raise ValueError("gateway route name is invalid") from None
    if validated != value:
        raise ValueError("gateway route name must not contain surrounding whitespace")
    return validated


__all__ = ["PostgresGatewayCircuitBreaker", "PostgresGatewayRateLimiter"]
