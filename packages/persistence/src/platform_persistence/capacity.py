"""PostgreSQL tenant quotas and distributed gateway-capacity leases."""

from __future__ import annotations

import math
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Annotated

from pydantic import StringConstraints, TypeAdapter, ValidationError
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert

from agent_core.capacity import (
    CapacityRejection,
    CapacityScope,
    GatewayCapacityClaim,
    GatewayCapacityLease,
    TenantQuota,
)
from agent_core.domain.errors import DomainOperationError
from agent_core.gateway import GatewayRequestIdentifier
from platform_persistence.models import (
    GatewayCapacityLeaseRecord,
    GatewayProviderCapacityRecord,
    TenantQuotaRecord,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from sqlalchemy.sql.elements import ColumnElement

MAX_CAPACITY_SECONDS = 3600.0
MAX_CONCURRENT_REQUESTS = 10_000
MAX_PROVIDER_TOKENS = 1_000_000_000
type CapacityRouteName = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=100,
        pattern=r"^[a-z][a-z0-9-]*$",
    ),
]
_ROUTE_ADAPTER: TypeAdapter[CapacityRouteName] = TypeAdapter(CapacityRouteName)
_REQUEST_ADAPTER: TypeAdapter[GatewayRequestIdentifier] = TypeAdapter(GatewayRequestIdentifier)


class PostgresTenantQuotaRepository:
    """Durable platform-controlled per-tenant quota overrides."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        default_quota: TenantQuota | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not callable(clock):
            raise TypeError("quota clock must be callable")
        self._sessions = sessions
        self._default = default_quota or TenantQuota()
        self._clock = clock

    async def get(self, tenant_id: uuid.UUID) -> TenantQuota:
        now = _aware_time(self._clock())
        async with self._sessions() as database, database.begin():
            row = await ensure_tenant_quota(
                database,
                tenant_id,
                default=self._default,
                occurred_at=now,
                lock=False,
            )
            return _quota_domain(row)

    async def set(
        self,
        tenant_id: uuid.UUID,
        quota: TenantQuota,
        *,
        occurred_at: datetime,
    ) -> TenantQuota:
        if not isinstance(quota, TenantQuota):
            raise TypeError("quota must be a TenantQuota")
        timestamp = _aware_time(occurred_at)
        async with self._sessions() as database, database.begin():
            row = await ensure_tenant_quota(
                database,
                tenant_id,
                default=self._default,
                occurred_at=timestamp,
                lock=True,
            )
            if timestamp < row.updated_at:
                raise ValueError("quota update may not move time backward")
            row.max_active_runs = quota.max_active_runs
            row.max_queued_runs = quota.max_queued_runs
            row.max_gateway_requests = quota.max_gateway_requests
            row.memory_enabled = quota.memory_enabled
            row.updated_at = timestamp
            await database.flush()
            return _quota_domain(row)


class PostgresGatewayCapacityStore:
    """Atomically coordinate tenant slots, route slots, and route token windows."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        default_quota: TenantQuota | None = None,
        provider_request_limit: int,
        provider_token_limit: int,
        token_window_seconds: float,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self._provider_request_limit = _bounded_integer(
            "provider_request_limit",
            provider_request_limit,
            maximum=MAX_CONCURRENT_REQUESTS,
        )
        self._provider_token_limit = _bounded_integer(
            "provider_token_limit",
            provider_token_limit,
            maximum=MAX_PROVIDER_TOKENS,
        )
        self._token_window_seconds = _bounded_seconds(
            "token_window_seconds",
            token_window_seconds,
        )
        if not callable(clock) or not callable(id_factory):
            raise TypeError("capacity clock and id_factory must be callable")
        self._sessions = sessions
        self._default_quota = default_quota or TenantQuota()
        self._clock = clock
        self._id_factory = id_factory

    async def acquire(
        self,
        tenant_id: uuid.UUID,
        route_name: str,
        request_id: str,
        *,
        reserved_tokens: int,
        lease_duration: timedelta,
    ) -> GatewayCapacityClaim:
        route_name = _validated_route(route_name)
        request_id = _validated_request_id(request_id)
        reserved_tokens = _bounded_integer(
            "reserved_tokens",
            reserved_tokens,
            maximum=MAX_PROVIDER_TOKENS,
        )
        lease_seconds = _bounded_seconds("lease_duration", lease_duration.total_seconds())
        now = _aware_time(self._clock())
        async with self._sessions() as database, database.begin():
            quota = await ensure_tenant_quota(
                database,
                tenant_id,
                default=self._default_quota,
                occurred_at=now,
                lock=True,
            )
            route = await self._route_capacity(database, route_name, now=now)
            await database.execute(
                delete(GatewayCapacityLeaseRecord).where(
                    GatewayCapacityLeaseRecord.expires_at <= now
                )
            )
            existing = await database.scalar(
                select(GatewayCapacityLeaseRecord)
                .where(
                    GatewayCapacityLeaseRecord.tenant_id == tenant_id,
                    GatewayCapacityLeaseRecord.request_id == request_id,
                )
                .with_for_update()
            )
            if existing is not None:
                if existing.route_name != route_name or existing.reserved_tokens != reserved_tokens:
                    raise ValueError("gateway capacity request identity is already in use")
                return GatewayCapacityClaim(lease=_lease_domain(existing))

            tenant_count = int(
                await database.scalar(
                    select(func.count())
                    .select_from(GatewayCapacityLeaseRecord)
                    .where(GatewayCapacityLeaseRecord.tenant_id == tenant_id)
                )
                or 0
            )
            if tenant_count >= quota.max_gateway_requests:
                return GatewayCapacityClaim(
                    rejection=CapacityRejection(
                        scope=CapacityScope.TENANT_GATEWAY_REQUESTS,
                        retry_after_seconds=await _database_retry_after(
                            database,
                            now,
                            GatewayCapacityLeaseRecord.tenant_id == tenant_id,
                        ),
                    )
                )

            route_count = int(
                await database.scalar(
                    select(func.count())
                    .select_from(GatewayCapacityLeaseRecord)
                    .where(GatewayCapacityLeaseRecord.route_name == route_name)
                )
                or 0
            )
            if route_count >= route.request_limit:
                return GatewayCapacityClaim(
                    rejection=CapacityRejection(
                        scope=CapacityScope.PROVIDER_REQUESTS,
                        retry_after_seconds=await _database_retry_after(
                            database,
                            now,
                            GatewayCapacityLeaseRecord.route_name == route_name,
                        ),
                    )
                )

            self._reset_token_window(route, now)
            if route.accounted_tokens + reserved_tokens > route.token_limit:
                return GatewayCapacityClaim(
                    rejection=CapacityRejection(
                        scope=CapacityScope.PROVIDER_TOKENS,
                        retry_after_seconds=max(
                            0.001,
                            float(route.token_window_seconds)
                            - (now - route.token_window_started_at).total_seconds(),
                        ),
                    )
                )

            lease_id = self._id_factory()
            if not isinstance(lease_id, uuid.UUID):
                raise TypeError("capacity id_factory must return UUID values")
            expires_at = now + timedelta(seconds=lease_seconds)
            row = GatewayCapacityLeaseRecord(
                id=lease_id,
                tenant_id=tenant_id,
                route_name=route_name,
                request_id=request_id,
                reserved_tokens=reserved_tokens,
                token_window_started_at=route.token_window_started_at,
                acquired_at=now,
                expires_at=expires_at,
            )
            route.accounted_tokens += reserved_tokens
            route.updated_at = now
            database.add(row)
            await database.flush()
            return GatewayCapacityClaim(lease=_lease_domain(row))

    async def release(
        self,
        lease: GatewayCapacityLease,
        *,
        consumed_tokens: int,
    ) -> None:
        if not isinstance(lease, GatewayCapacityLease):
            raise TypeError("lease must be a GatewayCapacityLease")
        if type(consumed_tokens) is not int or not 0 <= consumed_tokens <= MAX_PROVIDER_TOKENS:
            raise ValueError("consumed_tokens must be in [0, 1000000000]")
        now = _aware_time(self._clock())
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(GatewayCapacityLeaseRecord)
                .where(GatewayCapacityLeaseRecord.id == lease.id)
                .with_for_update()
            )
            if row is None:
                return
            if _lease_domain(row) != lease:
                raise ValueError("gateway capacity lease does not match durable ownership")
            route = await database.scalar(
                select(GatewayProviderCapacityRecord)
                .where(GatewayProviderCapacityRecord.route_name == lease.route_name)
                .with_for_update()
            )
            if route is None:
                raise RuntimeError("gateway provider capacity row disappeared")
            self._reset_token_window(route, now)
            if route.token_window_started_at == row.token_window_started_at:
                route.accounted_tokens = max(
                    0,
                    route.accounted_tokens - row.reserved_tokens + consumed_tokens,
                )
            route.updated_at = now
            await database.delete(row)

    async def renew(
        self,
        lease: GatewayCapacityLease,
        *,
        lease_duration: timedelta,
    ) -> GatewayCapacityLease:
        if not isinstance(lease, GatewayCapacityLease):
            raise TypeError("lease must be a GatewayCapacityLease")
        lease_seconds = _bounded_seconds("lease_duration", lease_duration.total_seconds())
        now = _aware_time(self._clock())
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(GatewayCapacityLeaseRecord)
                .where(GatewayCapacityLeaseRecord.id == lease.id)
                .with_for_update()
            )
            if row is None or _lease_domain(row) != lease or row.expires_at <= now:
                raise DomainOperationError(
                    code="gateway_capacity_lease_lost",
                    message="the gateway capacity lease is no longer owned",
                    retryable=True,
                )
            row.expires_at = now + timedelta(seconds=lease_seconds)
            await database.flush()
            return _lease_domain(row)

    async def _route_capacity(
        self,
        database: AsyncSession,
        route_name: str,
        *,
        now: datetime,
    ) -> GatewayProviderCapacityRecord:
        await database.execute(
            insert(GatewayProviderCapacityRecord)
            .values(
                route_name=route_name,
                request_limit=self._provider_request_limit,
                token_limit=self._provider_token_limit,
                token_window_seconds=Decimal(str(self._token_window_seconds)),
                token_window_started_at=now,
                accounted_tokens=0,
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=(GatewayProviderCapacityRecord.route_name,))
        )
        row = await database.scalar(
            select(GatewayProviderCapacityRecord)
            .where(GatewayProviderCapacityRecord.route_name == route_name)
            .with_for_update()
        )
        if row is None:
            raise RuntimeError("gateway provider capacity row disappeared")
        if now < row.token_window_started_at or now < row.updated_at:
            raise ValueError("capacity clock may not move backward")
        configured_window = Decimal(str(self._token_window_seconds))
        if (
            row.request_limit != self._provider_request_limit
            or row.token_limit != self._provider_token_limit
            or row.token_window_seconds != configured_window
        ):
            row.request_limit = self._provider_request_limit
            row.token_limit = self._provider_token_limit
            row.token_window_seconds = configured_window
            row.updated_at = now
        return row

    @staticmethod
    def _reset_token_window(route: GatewayProviderCapacityRecord, now: datetime) -> None:
        if now < route.token_window_started_at or now < route.updated_at:
            raise ValueError("capacity clock may not move backward")
        if (now - route.token_window_started_at).total_seconds() >= float(
            route.token_window_seconds
        ):
            route.token_window_started_at = now
            route.accounted_tokens = 0
            route.updated_at = now


async def ensure_tenant_quota(
    database: AsyncSession,
    tenant_id: uuid.UUID,
    *,
    default: TenantQuota,
    occurred_at: datetime,
    lock: bool,
) -> TenantQuotaRecord:
    """Create a tenant's bounded defaults once and optionally lock the durable row."""

    if not isinstance(tenant_id, uuid.UUID):
        raise TypeError("tenant_id must be a UUID")
    await database.execute(
        insert(TenantQuotaRecord)
        .values(
            tenant_id=tenant_id,
            max_active_runs=default.max_active_runs,
            max_queued_runs=default.max_queued_runs,
            max_gateway_requests=default.max_gateway_requests,
            memory_enabled=default.memory_enabled,
            created_at=occurred_at,
            updated_at=occurred_at,
        )
        .on_conflict_do_nothing(index_elements=(TenantQuotaRecord.tenant_id,))
    )
    statement = select(TenantQuotaRecord).where(TenantQuotaRecord.tenant_id == tenant_id)
    if lock:
        statement = statement.with_for_update()
    row = await database.scalar(statement)
    if row is None:
        raise RuntimeError("tenant quota row disappeared")
    return row


async def _database_retry_after(
    database: AsyncSession,
    now: datetime,
    condition: ColumnElement[bool],
) -> float:
    earliest = await database.scalar(
        select(func.min(GatewayCapacityLeaseRecord.expires_at)).where(condition)
    )
    if not isinstance(earliest, datetime):
        raise TypeError("capacity rejection has no expiring lease")
    return max(0.001, (earliest - now).total_seconds())


def _quota_domain(row: TenantQuotaRecord) -> TenantQuota:
    return TenantQuota(
        max_active_runs=row.max_active_runs,
        max_queued_runs=row.max_queued_runs,
        max_gateway_requests=row.max_gateway_requests,
        memory_enabled=row.memory_enabled,
    )


def _lease_domain(row: GatewayCapacityLeaseRecord) -> GatewayCapacityLease:
    return GatewayCapacityLease(
        id=row.id,
        tenant_id=row.tenant_id,
        route_name=row.route_name,
        request_id=row.request_id,
        reserved_tokens=row.reserved_tokens,
        acquired_at=row.acquired_at,
        expires_at=row.expires_at,
    )


def _aware_time(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("capacity clock must return an aware datetime")
    return value.astimezone(UTC)


def _bounded_integer(name: str, value: int, *, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be in [1, {maximum}]")
    return value


def _bounded_seconds(name: str, value: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 < value <= MAX_CAPACITY_SECONDS
    ):
        raise ValueError(f"{name} must be in (0, {MAX_CAPACITY_SECONDS:g}]")
    return float(value)


def _validated_route(value: str) -> str:
    try:
        return _ROUTE_ADAPTER.validate_python(value)
    except ValidationError:
        raise ValueError("route_name is invalid") from None


def _validated_request_id(value: str) -> str:
    try:
        return _REQUEST_ADAPTER.validate_python(value)
    except ValidationError:
        raise ValueError("request_id is invalid") from None


__all__ = [
    "PostgresGatewayCapacityStore",
    "PostgresTenantQuotaRepository",
    "ensure_tenant_quota",
]
