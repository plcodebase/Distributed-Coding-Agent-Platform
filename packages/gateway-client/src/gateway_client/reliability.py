"""Provider-neutral reliability policies for logical gateway requests."""

from __future__ import annotations

import asyncio
import math
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from agent_core.capacity import (
    CapacityRejection,
    CapacityScope,
    GatewayCapacityClaim,
    GatewayCapacityLease,
    GatewayCapacityStore,
)
from agent_core.domain.errors import DomainOperationError
from agent_core.gateway_reliability import (
    GatewayCircuitBreaker,
    GatewayRateLimiter,
    GatewayRequestClaim,
    GatewayRequestClaimStatus,
    GatewayRequestStore,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from agent_core.domain.errors import ErrorDetail
    from agent_core.domain.models import Sha256Hex
    from agent_core.gateway import GatewayEvent, GatewayRequestIdentifier

MAX_POLICY_SECONDS = 3600.0
MAX_LOCAL_POLICY_KEYS = 100_000
MAX_LOCAL_CIRCUIT_ROUTES = 1000
MAX_RATE_LIMIT_REQUESTS = 1_000_000
MAX_CIRCUIT_FAILURE_THRESHOLD = 100
MAX_PROVIDER_TOKENS_PER_WINDOW = 1_000_000_000
MAX_CONCURRENT_GATEWAY_REQUESTS = 10_000


@dataclass(slots=True)
class _StoredRequest:
    request_hash: Sha256Hex
    status: GatewayRequestClaimStatus
    events: tuple[GatewayEvent, ...] = ()
    error: ErrorDetail | None = None


@dataclass(slots=True)
class _CapacityLeaseState:
    lease: GatewayCapacityLease
    token_window_started_at: datetime


@dataclass(slots=True)
class _TokenWindow:
    started_at: datetime
    tokens: int = 0


class InMemoryGatewayCapacityStore:
    """Deterministic all-or-nothing local gateway/provider capacity policy."""

    def __init__(
        self,
        *,
        tenant_request_limit: int,
        provider_request_limit: int,
        provider_token_limit: int,
        token_window_seconds: float,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self._tenant_limit = _bounded_integer(
            "tenant_request_limit",
            tenant_request_limit,
            maximum=MAX_CONCURRENT_GATEWAY_REQUESTS,
        )
        self._provider_limit = _bounded_integer(
            "provider_request_limit",
            provider_request_limit,
            maximum=MAX_CONCURRENT_GATEWAY_REQUESTS,
        )
        self._token_limit = _bounded_integer(
            "provider_token_limit",
            provider_token_limit,
            maximum=MAX_PROVIDER_TOKENS_PER_WINDOW,
        )
        self._window = _bounded_seconds("token_window_seconds", token_window_seconds)
        if not callable(clock) or not callable(id_factory):
            raise TypeError("capacity clock and id_factory must be callable")
        self._clock = clock
        self._id_factory = id_factory
        self._lock = asyncio.Lock()
        self._leases: dict[uuid.UUID, _CapacityLeaseState] = {}
        self._requests: dict[tuple[uuid.UUID, str], uuid.UUID] = {}
        self._tokens: dict[str, _TokenWindow] = {}

    async def acquire(
        self,
        tenant_id: uuid.UUID,
        route_name: str,
        request_id: str,
        *,
        reserved_tokens: int,
        lease_duration: timedelta,
    ) -> GatewayCapacityClaim:
        reserved_tokens = _bounded_integer(
            "reserved_tokens",
            reserved_tokens,
            maximum=MAX_PROVIDER_TOKENS_PER_WINDOW,
        )
        duration = _bounded_seconds("lease_duration", lease_duration.total_seconds())
        now = _aware_clock_value(self._clock())
        async with self._lock:
            self._prune(now)
            request_key = (tenant_id, request_id)
            existing_id = self._requests.get(request_key)
            if existing_id is not None:
                existing = self._leases[existing_id].lease
                if existing.route_name != route_name or existing.reserved_tokens != reserved_tokens:
                    raise ValueError("gateway capacity request identity is already in use")
                return GatewayCapacityClaim(lease=existing)

            tenant_leases = tuple(
                state.lease for state in self._leases.values() if state.lease.tenant_id == tenant_id
            )
            if len(tenant_leases) >= self._tenant_limit:
                return GatewayCapacityClaim(
                    rejection=CapacityRejection(
                        scope=CapacityScope.TENANT_GATEWAY_REQUESTS,
                        retry_after_seconds=_retry_after(tenant_leases, now),
                    )
                )
            route_leases = tuple(
                state.lease
                for state in self._leases.values()
                if state.lease.route_name == route_name
            )
            if len(route_leases) >= self._provider_limit:
                return GatewayCapacityClaim(
                    rejection=CapacityRejection(
                        scope=CapacityScope.PROVIDER_REQUESTS,
                        retry_after_seconds=_retry_after(route_leases, now),
                    )
                )

            window = self._token_window(route_name, now)
            if window.tokens + reserved_tokens > self._token_limit:
                return GatewayCapacityClaim(
                    rejection=CapacityRejection(
                        scope=CapacityScope.PROVIDER_TOKENS,
                        retry_after_seconds=max(
                            0.001,
                            self._window - (now - window.started_at).total_seconds(),
                        ),
                    )
                )

            lease_id = self._id_factory()
            if not isinstance(lease_id, uuid.UUID):
                raise TypeError("capacity id_factory must return UUID values")
            lease = GatewayCapacityLease(
                id=lease_id,
                tenant_id=tenant_id,
                route_name=route_name,
                request_id=request_id,
                reserved_tokens=reserved_tokens,
                acquired_at=now,
                expires_at=now + timedelta(seconds=duration),
            )
            window.tokens += reserved_tokens
            self._leases[lease.id] = _CapacityLeaseState(
                lease=lease,
                token_window_started_at=window.started_at,
            )
            self._requests[request_key] = lease.id
            return GatewayCapacityClaim(lease=lease)

    async def release(
        self,
        lease: GatewayCapacityLease,
        *,
        consumed_tokens: int,
    ) -> None:
        if (
            type(consumed_tokens) is not int
            or not 0 <= consumed_tokens <= MAX_PROVIDER_TOKENS_PER_WINDOW
        ):
            raise ValueError("consumed_tokens must be in [0, 1000000000]")
        now = _aware_clock_value(self._clock())
        async with self._lock:
            state = self._leases.pop(lease.id, None)
            if state is None:
                return
            if state.lease != lease:
                self._leases[lease.id] = state
                raise ValueError("gateway capacity lease does not match durable ownership")
            self._requests.pop((lease.tenant_id, lease.request_id), None)
            window = self._token_window(lease.route_name, now)
            if window.started_at == state.token_window_started_at:
                window.tokens = max(
                    0,
                    window.tokens - lease.reserved_tokens + consumed_tokens,
                )

    async def renew(
        self,
        lease: GatewayCapacityLease,
        *,
        lease_duration: timedelta,
    ) -> GatewayCapacityLease:
        if not isinstance(lease, GatewayCapacityLease):
            raise TypeError("lease must be a GatewayCapacityLease")
        duration = _bounded_seconds("lease_duration", lease_duration.total_seconds())
        now = _aware_clock_value(self._clock())
        async with self._lock:
            self._prune(now)
            state = self._leases.get(lease.id)
            if state is None or state.lease != lease:
                raise DomainOperationError(
                    code="gateway_capacity_lease_lost",
                    message="the gateway capacity lease is no longer owned",
                    retryable=True,
                )
            renewed = lease.model_copy(update={"expires_at": now + timedelta(seconds=duration)})
            state.lease = renewed
            return renewed

    def _prune(self, now: datetime) -> None:
        for lease_id, state in tuple(self._leases.items()):
            if state.lease.expires_at <= now:
                del self._leases[lease_id]
                self._requests.pop(
                    (state.lease.tenant_id, state.lease.request_id),
                    None,
                )

    def _token_window(self, route_name: str, now: datetime) -> _TokenWindow:
        window = self._tokens.get(route_name)
        if window is None or (now - window.started_at).total_seconds() >= self._window:
            window = _TokenWindow(started_at=now)
            self._tokens[route_name] = window
        return window


class InMemoryGatewayRequestStore:
    """Deterministic process-local request store used by tests and local composition."""

    def __init__(self, *, max_requests: int = 10_000) -> None:
        if type(max_requests) is not int or not 1 <= max_requests <= MAX_LOCAL_POLICY_KEYS:
            raise ValueError("max_requests must be in [1, 100000]")
        self._lock = asyncio.Lock()
        self._requests: dict[tuple[uuid.UUID, GatewayRequestIdentifier], _StoredRequest] = {}
        self._max_requests = max_requests

    async def claim(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
    ) -> GatewayRequestClaim:
        async with self._lock:
            key = (tenant_id, request_id)
            stored = self._requests.get(key)
            if stored is None:
                if len(self._requests) >= self._max_requests:
                    raise RuntimeError("in-memory gateway request store reached capacity")
                self._requests[key] = _StoredRequest(
                    request_hash=request_hash,
                    status=GatewayRequestClaimStatus.IN_PROGRESS,
                )
                return GatewayRequestClaim(
                    status=GatewayRequestClaimStatus.EXECUTE,
                    request_hash=request_hash,
                )
            if stored.request_hash != request_hash:
                return GatewayRequestClaim(
                    status=GatewayRequestClaimStatus.CONFLICT,
                    request_hash=stored.request_hash,
                )
            return GatewayRequestClaim(
                status=stored.status,
                request_hash=stored.request_hash,
                events=stored.events,
                error=stored.error,
            )

    async def complete(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        events: tuple[GatewayEvent, ...],
    ) -> None:
        _ = GatewayRequestClaim(
            status=GatewayRequestClaimStatus.COMPLETED,
            request_hash=request_hash,
            events=events,
        )
        async with self._lock:
            stored = self._require_owned(tenant_id, request_id, request_hash)
            if stored.status is not GatewayRequestClaimStatus.IN_PROGRESS:
                raise ValueError("gateway request is not in progress")
            stored.status = GatewayRequestClaimStatus.COMPLETED
            stored.events = events

    async def fail(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        error: ErrorDetail,
    ) -> None:
        async with self._lock:
            stored = self._require_owned(tenant_id, request_id, request_hash)
            if stored.status is not GatewayRequestClaimStatus.IN_PROGRESS:
                return
            stored.status = GatewayRequestClaimStatus.FAILED
            stored.error = error

    async def release(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
    ) -> None:
        async with self._lock:
            key = (tenant_id, request_id)
            stored = self._requests.get(key)
            if (
                stored is not None
                and stored.request_hash == request_hash
                and stored.status is GatewayRequestClaimStatus.IN_PROGRESS
            ):
                del self._requests[key]
                return
            raise ValueError("gateway request claim is missing or belongs to another payload")

    def _require_owned(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
    ) -> _StoredRequest:
        stored = self._requests.get((tenant_id, request_id))
        if stored is None or stored.request_hash != request_hash:
            raise ValueError("gateway request claim is missing or belongs to another payload")
        return stored


class InMemoryGatewayRateLimiter:
    """Bounded sliding-window limiter for single-process/local operation."""

    def __init__(
        self,
        *,
        requests_per_window: int,
        window_seconds: float,
        max_keys: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
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
        if type(max_keys) is not int or not 1 <= max_keys <= MAX_LOCAL_POLICY_KEYS:
            raise ValueError("max_keys must be in [1, 100000]")
        self._limit = requests_per_window
        self._window = window_seconds
        self._clock = clock
        self._max_keys = max_keys
        self._lock = asyncio.Lock()
        self._requests: dict[tuple[uuid.UUID, str], deque[float]] = {}

    async def acquire(self, tenant_id: uuid.UUID, route_name: str) -> float | None:
        now = _finite_clock_value(self._clock())
        boundary = now - self._window
        key = (tenant_id, route_name)
        async with self._lock:
            if key not in self._requests and len(self._requests) >= self._max_keys:
                raise RuntimeError("in-memory gateway rate limiter reached capacity")
            timestamps = self._requests.setdefault(key, deque())
            while timestamps and timestamps[0] <= boundary:
                timestamps.popleft()
            if len(timestamps) >= self._limit:
                return max(0.0, timestamps[0] + self._window - now)
            timestamps.append(now)
            return None


@dataclass(slots=True)
class _CircuitState:
    failures: int = 0
    opened_at: float | None = None
    probe_in_flight: bool = False
    probe_started_at: float | None = None


class InMemoryGatewayCircuitBreaker:
    """Deterministic closed/open/half-open circuit breaker."""

    def __init__(
        self,
        *,
        failure_threshold: int,
        recovery_seconds: float,
        max_routes: int = 100,
        clock: Callable[[], float] = time.monotonic,
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
        if type(max_routes) is not int or not 1 <= max_routes <= MAX_LOCAL_CIRCUIT_ROUTES:
            raise ValueError("max_routes must be in [1, 1000]")
        self._threshold = failure_threshold
        self._recovery = recovery_seconds
        self._clock = clock
        self._max_routes = max_routes
        self._lock = asyncio.Lock()
        self._states: dict[str, _CircuitState] = {}

    async def allow(self, route_name: str) -> bool:
        now = _finite_clock_value(self._clock())
        async with self._lock:
            if route_name not in self._states and len(self._states) >= self._max_routes:
                raise RuntimeError("in-memory gateway circuit breaker reached capacity")
            state = self._states.setdefault(route_name, _CircuitState())
            if state.opened_at is None:
                return True
            if now - state.opened_at < self._recovery:
                return False
            if state.probe_in_flight:
                if (
                    state.probe_started_at is not None
                    and now - state.probe_started_at < self._recovery
                ):
                    return False
                state.probe_in_flight = False
                state.probe_started_at = None
            state.probe_in_flight = True
            state.probe_started_at = now
            return True

    async def record_success(self, route_name: str) -> None:
        async with self._lock:
            if route_name not in self._states and len(self._states) >= self._max_routes:
                raise RuntimeError("in-memory gateway circuit breaker reached capacity")
            self._states[route_name] = _CircuitState()

    async def record_failure(self, route_name: str) -> None:
        now = _finite_clock_value(self._clock())
        async with self._lock:
            if route_name not in self._states and len(self._states) >= self._max_routes:
                raise RuntimeError("in-memory gateway circuit breaker reached capacity")
            state = self._states.setdefault(route_name, _CircuitState())
            state.probe_in_flight = False
            state.probe_started_at = None
            state.failures += 1
            if state.failures >= self._threshold:
                state.opened_at = now


def _finite_clock_value(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("gateway policy clock must return a finite number")
    return float(value)


def _aware_clock_value(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("gateway capacity clock must return an aware datetime")
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
        or not 0 < value <= MAX_POLICY_SECONDS
    ):
        raise ValueError(f"{name} must be in (0, {MAX_POLICY_SECONDS:g}]")
    return float(value)


def _retry_after(leases: tuple[GatewayCapacityLease, ...], now: datetime) -> float:
    return max(0.001, min((lease.expires_at - now).total_seconds() for lease in leases))


__all__ = [
    "GatewayCapacityStore",
    "GatewayCircuitBreaker",
    "GatewayRateLimiter",
    "GatewayRequestClaim",
    "GatewayRequestClaimStatus",
    "GatewayRequestStore",
    "InMemoryGatewayCapacityStore",
    "InMemoryGatewayCircuitBreaker",
    "InMemoryGatewayRateLimiter",
    "InMemoryGatewayRequestStore",
]
