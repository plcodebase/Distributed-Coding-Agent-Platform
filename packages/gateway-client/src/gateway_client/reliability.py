"""Provider-neutral reliability policies for logical gateway requests."""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

from agent_core.gateway_reliability import (
    GatewayCircuitBreaker,
    GatewayRateLimiter,
    GatewayRequestClaim,
    GatewayRequestClaimStatus,
    GatewayRequestStore,
)

if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable

    from agent_core.domain.errors import ErrorDetail
    from agent_core.domain.models import Sha256Hex
    from agent_core.gateway import GatewayEvent, GatewayRequestIdentifier

MAX_POLICY_SECONDS = 3600.0
MAX_LOCAL_POLICY_KEYS = 100_000
MAX_LOCAL_CIRCUIT_ROUTES = 1000


@dataclass(slots=True)
class _StoredRequest:
    request_hash: Sha256Hex
    status: GatewayRequestClaimStatus
    events: tuple[GatewayEvent, ...] = ()
    error: ErrorDetail | None = None


class InMemoryGatewayRequestStore:
    """Deterministic process-local request store used by tests and local composition."""

    def __init__(self, *, max_requests: int = 10_000) -> None:
        if not 1 <= max_requests <= MAX_LOCAL_POLICY_KEYS:
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
        if type(requests_per_window) is not int or requests_per_window < 1:
            raise ValueError("requests_per_window must be positive")
        if (
            isinstance(window_seconds, bool)
            or not isinstance(window_seconds, (int, float))
            or not math.isfinite(window_seconds)
            or not 0 < window_seconds <= MAX_POLICY_SECONDS
        ):
            raise ValueError("window_seconds must be in (0, 3600]")
        if not 1 <= max_keys <= MAX_LOCAL_POLICY_KEYS:
            raise ValueError("max_keys must be in [1, 100000]")
        self._limit = requests_per_window
        self._window = window_seconds
        self._clock = clock
        self._max_keys = max_keys
        self._lock = asyncio.Lock()
        self._requests: dict[tuple[uuid.UUID, str], deque[float]] = {}

    async def acquire(self, tenant_id: uuid.UUID, route_name: str) -> float | None:
        now = self._clock()
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
        if type(failure_threshold) is not int or failure_threshold < 1:
            raise ValueError("failure_threshold must be positive")
        if (
            isinstance(recovery_seconds, bool)
            or not isinstance(recovery_seconds, (int, float))
            or not math.isfinite(recovery_seconds)
            or not 0 < recovery_seconds <= MAX_POLICY_SECONDS
        ):
            raise ValueError("recovery_seconds must be in (0, 3600]")
        if not 1 <= max_routes <= MAX_LOCAL_CIRCUIT_ROUTES:
            raise ValueError("max_routes must be in [1, 1000]")
        self._threshold = failure_threshold
        self._recovery = recovery_seconds
        self._clock = clock
        self._max_routes = max_routes
        self._lock = asyncio.Lock()
        self._states: dict[str, _CircuitState] = {}

    async def allow(self, route_name: str) -> bool:
        now = self._clock()
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
        now = self._clock()
        async with self._lock:
            if route_name not in self._states and len(self._states) >= self._max_routes:
                raise RuntimeError("in-memory gateway circuit breaker reached capacity")
            state = self._states.setdefault(route_name, _CircuitState())
            state.probe_in_flight = False
            state.probe_started_at = None
            state.failures += 1
            if state.failures >= self._threshold:
                state.opened_at = now


__all__ = [
    "GatewayCircuitBreaker",
    "GatewayRateLimiter",
    "GatewayRequestClaim",
    "GatewayRequestClaimStatus",
    "GatewayRequestStore",
    "InMemoryGatewayCircuitBreaker",
    "InMemoryGatewayRateLimiter",
    "InMemoryGatewayRequestStore",
]
