"""Typed and bounded client boundary for the centralized model gateway."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import random
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Annotated, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.gateway import (
    GatewayEvent,
    GatewayRequest,
    GatewayResponseCompleted,
    ModelGateway,
    parse_gateway_event,
)
from gateway_client.reliability import (
    GatewayCircuitBreaker,
    GatewayRateLimiter,
    GatewayRequestClaim,
    GatewayRequestClaimStatus,
    GatewayRequestStore,
    InMemoryGatewayCircuitBreaker,
    InMemoryGatewayRateLimiter,
    InMemoryGatewayRequestStore,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable
    from types import TracebackType

    from agent_core.domain.models import Sha256Hex


DEFAULT_MODEL_ROUTES = (
    "coding-default",
    "coding-fast",
    "coding-strong",
    "summarization",
    "code-review",
)
DEFAULT_GATEWAY_REQUEST_BYTES = 1024 * 1024
MAX_GATEWAY_REQUEST_BYTES = 8 * 1024 * 1024
MAX_GATEWAY_STREAM_BYTES = 64 * 1024 * 1024
MAX_GATEWAY_STREAM_EVENTS = 1_000_000
type RouteName = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=100,
        pattern=r"^[a-z][a-z0-9-]*$",
    ),
]


class GatewayClientConfig(DomainModel):
    """Closed limits and route allowlist for one gateway client."""

    route_names: tuple[RouteName, ...] = Field(
        default=DEFAULT_MODEL_ROUTES,
        min_length=1,
        max_length=100,
    )
    max_request_bytes: int = Field(
        default=DEFAULT_GATEWAY_REQUEST_BYTES,
        ge=1,
        le=MAX_GATEWAY_REQUEST_BYTES,
    )
    max_stream_events: int = Field(default=100_000, ge=1, le=MAX_GATEWAY_STREAM_EVENTS)
    max_stream_bytes: int = Field(
        default=8 * 1024 * 1024,
        ge=1,
        le=MAX_GATEWAY_STREAM_BYTES,
    )
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_seconds: float = Field(default=0.05, gt=0, le=60)
    retry_max_delay_seconds: float = Field(default=2, gt=0, le=300)
    retry_jitter_ratio: float = Field(default=0.2, ge=0, le=1)
    circuit_failure_threshold: int = Field(default=5, ge=1, le=100)
    circuit_recovery_seconds: float = Field(default=30, gt=0, le=3600)
    rate_limit_requests: int = Field(default=60, ge=1, le=1_000_000)
    rate_limit_window_seconds: float = Field(default=60, gt=0, le=3600)

    @model_validator(mode="after")
    def validate_configuration(self) -> Self:
        if len(self.route_names) != len(set(self.route_names)):
            raise ValueError("gateway route names must be unique")
        if self.retry_base_delay_seconds > self.retry_max_delay_seconds:
            raise ValueError("retry base delay may not exceed maximum delay")
        return self


class GatewayClient(AbstractAsyncContextManager["GatewayClient"]):
    """Validate, bound, and lifecycle-manage normalized gateway streams."""

    def __init__(
        self,
        gateway: ModelGateway,
        *,
        config: GatewayClientConfig | None = None,
        close: Callable[[], Awaitable[None]] | None = None,
        request_store: GatewayRequestStore | None = None,
        rate_limiter: GatewayRateLimiter | None = None,
        circuit_breaker: GatewayCircuitBreaker | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self._gateway = gateway
        self._config = config or GatewayClientConfig()
        self._routes = frozenset(self._config.route_names)
        self._close = close
        self._request_store = request_store or InMemoryGatewayRequestStore()
        self._rate_limiter = rate_limiter or InMemoryGatewayRateLimiter(
            requests_per_window=self._config.rate_limit_requests,
            window_seconds=self._config.rate_limit_window_seconds,
        )
        self._circuit_breaker = circuit_breaker or InMemoryGatewayCircuitBreaker(
            failure_threshold=self._config.circuit_failure_threshold,
            recovery_seconds=self._config.circuit_recovery_seconds,
        )
        self._sleep = sleep
        self._random_value = random_value
        self._close_lock = asyncio.Lock()
        self._closing = False
        self._cleanup_required = False
        self._closed = False

    async def __aenter__(self) -> Self:
        self._require_available()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        await self.aclose()

    async def aclose(self) -> None:
        """Close the owned model boundary exactly once."""

        task = asyncio.create_task(self._aclose())
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _aclose(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            self._closing = True
            try:
                if self._close is not None:
                    await self._close()
            except Exception as error:
                self._closing = False
                self._cleanup_required = True
                raise DomainOperationError(
                    code="gateway_cleanup_failed",
                    message="the gateway client could not be closed",
                    retryable=True,
                ) from error
            self._closing = False
            self._cleanup_required = False
            self._closed = True

    async def stream(self, request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        """Execute or replay one bounded, attributed, idempotent logical request."""

        self._require_available(request=request)
        self._validate_request(request)
        request_hash = _request_hash(request)
        claim = await self._claim_request(request, request_hash)
        stored_events = self._stored_events_or_raise(request, claim)
        if stored_events is not None:
            for event in stored_events:
                yield event
            return

        provider_contacted = False
        completed = False
        try:
            await self._enforce_rate_limit(request)
            await self._require_closed_circuit(request)
            provider_contacted = True
            execution = self._execute_with_retries(request, request_hash)
            try:
                async for event in execution:
                    if isinstance(event, GatewayResponseCompleted):
                        # The execution coordinator yields its terminal event only
                        # after the completed response is durable. Record that fact
                        # before handing control to a consumer that may immediately
                        # close the async generator.
                        completed = True
                    yield event
            finally:
                await _close_stream(execution, request=request)
            completed = True
        finally:
            if provider_contacted and not completed:
                await self._fail_request(
                    request,
                    request_hash,
                    ErrorDetail(
                        code="gateway_request_aborted",
                        message="the gateway request did not complete",
                        retryable=True,
                    ),
                )
            elif not provider_contacted:
                await self._release_request(request, request_hash)

    @staticmethod
    def _stored_events_or_raise(
        request: GatewayRequest,
        claim: GatewayRequestClaim,
    ) -> tuple[GatewayEvent, ...] | None:
        if claim.status is GatewayRequestClaimStatus.COMPLETED:
            return claim.events
        if claim.status is GatewayRequestClaimStatus.CONFLICT:
            raise DomainOperationError(
                code="gateway_request_conflict",
                message="the gateway request ID belongs to a different payload",
                details={"request_id": request.request_id},
            )
        if claim.status is GatewayRequestClaimStatus.IN_PROGRESS:
            raise DomainOperationError(
                code="gateway_request_in_progress",
                message="the gateway request is already running",
                retryable=True,
                details={"request_id": request.request_id},
            )
        if claim.status is GatewayRequestClaimStatus.FAILED:
            if claim.error is None:
                raise DomainOperationError(
                    code="gateway_idempotency_corrupt",
                    message="the stored gateway request state is invalid",
                    retryable=True,
                    details={"request_id": request.request_id},
                )
            raise DomainOperationError(
                code="gateway_request_failed",
                message="the gateway request has a stored terminal failure",
                retryable=claim.error.retryable,
                details={"request_id": request.request_id},
            )
        return None

    async def _execute_with_retries(
        self,
        request: GatewayRequest,
        request_hash: Sha256Hex,
    ) -> AsyncIterator[GatewayEvent]:
        retained_events: list[GatewayEvent] = []
        for attempt in range(1, self._config.max_attempts + 1):
            if attempt > 1:
                await self._require_closed_circuit(request)
            attempt_events: list[GatewayEvent] = []
            attempt_stream = self._stream_attempt(request)
            try:
                try:
                    async for event in attempt_stream:
                        attempt_events.append(event)
                        retained_events.append(event)
                        if not isinstance(event, GatewayResponseCompleted):
                            yield event
                finally:
                    await _close_stream(attempt_stream, request=request)
            except DomainOperationError as error:
                if error.retryable:
                    await self._record_circuit_failure(request)
                if not self._can_retry(attempt, attempt_events, error):
                    raise
                await self._sleep(self._retry_delay(attempt))
                continue

            await self._record_circuit_success(request)
            await self._complete_request(request, request_hash, tuple(retained_events))
            yield _require_terminal_event(retained_events, request)
            return

        raise DomainOperationError(
            code="gateway_retry_exhausted",
            message="the gateway retry policy ended without a result",
            retryable=True,
            details={"request_id": request.request_id},
        )

    async def _enforce_rate_limit(self, request: GatewayRequest) -> None:
        try:
            retry_after = await self._rate_limiter.acquire(
                request.tenant_id,
                request.route_name,
            )
        except Exception as error:
            raise DomainOperationError(
                code="gateway_rate_limit_unavailable",
                message="the gateway admission policy is unavailable",
                retryable=True,
                details={"request_id": request.request_id},
            ) from error
        if retry_after is not None:
            raise DomainOperationError(
                code="gateway_rate_limited",
                message="the tenant and model route exceeded their request limit",
                retryable=True,
                details={
                    "request_id": request.request_id,
                    "retry_after_seconds": retry_after,
                    "route_name": request.route_name,
                },
            )

    async def _require_closed_circuit(self, request: GatewayRequest) -> None:
        try:
            allowed = await self._circuit_breaker.allow(request.route_name)
        except Exception as error:
            raise DomainOperationError(
                code="gateway_circuit_unavailable",
                message="the gateway route health policy is unavailable",
                retryable=True,
                details={"request_id": request.request_id},
            ) from error
        if not allowed:
            raise DomainOperationError(
                code="gateway_circuit_open",
                message="the model route circuit is open",
                retryable=True,
                details={
                    "request_id": request.request_id,
                    "route_name": request.route_name,
                },
            )

    async def _record_circuit_success(self, request: GatewayRequest) -> None:
        try:
            await self._circuit_breaker.record_success(request.route_name)
        except Exception as error:
            raise DomainOperationError(
                code="gateway_circuit_unavailable",
                message="the gateway route health policy is unavailable",
                retryable=True,
                details={"request_id": request.request_id},
            ) from error

    async def _record_circuit_failure(self, request: GatewayRequest) -> None:
        try:
            await self._circuit_breaker.record_failure(request.route_name)
        except Exception as error:
            raise DomainOperationError(
                code="gateway_circuit_unavailable",
                message="the gateway route health policy is unavailable",
                retryable=True,
                details={"request_id": request.request_id},
            ) from error

    def _can_retry(
        self,
        attempt: int,
        attempt_events: list[GatewayEvent],
        error: DomainOperationError,
    ) -> bool:
        return not attempt_events and error.retryable and attempt < self._config.max_attempts

    def _validate_request(self, request: GatewayRequest) -> None:
        if request.route_name not in self._routes:
            raise DomainOperationError(
                code="gateway_route_not_allowed",
                message="the requested model route is not configured",
                details={
                    "request_id": request.request_id,
                    "route_name": request.route_name,
                },
            )
        request_bytes = _request_size(request)
        if request_bytes > self._config.max_request_bytes:
            raise DomainOperationError(
                code="gateway_request_limit",
                message="the gateway request exceeded its configured byte limit",
                details={
                    "limit_bytes": self._config.max_request_bytes,
                    "request_bytes": request_bytes,
                    "request_id": request.request_id,
                },
            )

    async def _stream_attempt(self, request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        event_count = 0
        stream_bytes = 0
        terminal = False
        delegate_stream = self._gateway.stream(request)
        try:
            iterator = delegate_stream.__aiter__()
            while True:
                try:
                    raw_event = await anext(iterator)
                except StopAsyncIteration:
                    break
                except DomainOperationError as error:
                    raise _opaque_gateway_failure(request, retryable=error.retryable) from error
                except Exception as error:
                    raise _opaque_gateway_failure(request, retryable=True) from error

                _ensure_not_terminal(terminal, request)
                try:
                    event = parse_gateway_event(raw_event)
                except ValueError as error:
                    raise _invalid_stream_error(
                        request,
                        "the gateway emitted an invalid normalized event",
                    ) from error
                event_count += 1
                stream_bytes += _event_size(event)
                _enforce_stream_limits(
                    request=request,
                    event_count=event_count,
                    stream_bytes=stream_bytes,
                    config=self._config,
                )
                terminal = isinstance(event, GatewayResponseCompleted)
                yield event
        finally:
            await _close_stream(delegate_stream, request=request)

        if not terminal:
            raise DomainOperationError(
                code="incomplete_gateway_stream",
                message="the gateway stream ended without a terminal event",
                retryable=True,
                details={"request_id": request.request_id},
            )

    async def _claim_request(
        self,
        request: GatewayRequest,
        request_hash: Sha256Hex,
    ) -> GatewayRequestClaim:
        try:
            return await self._request_store.claim(
                request.tenant_id,
                request.request_id,
                request_hash,
            )
        except Exception as error:
            raise DomainOperationError(
                code="gateway_idempotency_unavailable",
                message="the gateway request could not be claimed",
                retryable=True,
                details={"request_id": request.request_id},
            ) from error

    async def _complete_request(
        self,
        request: GatewayRequest,
        request_hash: Sha256Hex,
        events: tuple[GatewayEvent, ...],
    ) -> None:
        try:
            await self._request_store.complete(
                request.tenant_id,
                request.request_id,
                request_hash,
                events,
            )
        except Exception as error:
            raise DomainOperationError(
                code="gateway_idempotency_commit_failed",
                message="the gateway response could not be committed",
                retryable=True,
                details={"request_id": request.request_id},
            ) from error

    async def _fail_request(
        self,
        request: GatewayRequest,
        request_hash: Sha256Hex,
        error: ErrorDetail,
    ) -> None:
        operation = asyncio.create_task(
            self._request_store.fail(
                request.tenant_id,
                request.request_id,
                request_hash,
                error,
            )
        )
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError:
            try:
                await operation
            except Exception:
                self._cleanup_required = True
            raise
        except Exception:
            self._cleanup_required = True

    async def _release_request(
        self,
        request: GatewayRequest,
        request_hash: Sha256Hex,
    ) -> None:
        operation = asyncio.create_task(
            self._request_store.release(
                request.tenant_id,
                request.request_id,
                request_hash,
            )
        )
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError:
            try:
                await operation
            except Exception:
                self._cleanup_required = True
            raise
        except Exception:
            self._cleanup_required = True

    def _retry_delay(self, attempt: int) -> float:
        base = min(
            self._config.retry_max_delay_seconds,
            self._config.retry_base_delay_seconds * (2 ** (attempt - 1)),
        )
        random_value = float(self._random_value())
        if not math.isfinite(random_value) or not 0 <= random_value <= 1:
            raise DomainOperationError(
                code="gateway_retry_policy_invalid",
                message="the gateway retry jitter source returned an invalid value",
            )
        jitter = (random_value * 2 - 1) * self._config.retry_jitter_ratio
        return float(max(0.0, base * (1 + jitter)))

    def _require_available(self, *, request: GatewayRequest | None = None) -> None:
        if self._closed:
            raise _client_closed_error(request)
        if self._closing or self._cleanup_required:
            raise DomainOperationError(
                code="gateway_cleanup_required",
                message="the gateway client requires cleanup before reuse",
                retryable=True,
                details=({"request_id": request.request_id} if request is not None else None),
            )


def _event_size(event: GatewayEvent) -> int:
    payload = json.dumps(
        event.model_dump(mode="json"),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return len(payload.encode("utf-8"))


def _request_size(request: GatewayRequest) -> int:
    return len(request.model_dump_json().encode("utf-8"))


def _request_hash(request: GatewayRequest) -> Sha256Hex:
    payload = json.dumps(
        request.model_dump(mode="json"),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _require_terminal_event(
    events: list[GatewayEvent],
    request: GatewayRequest,
) -> GatewayResponseCompleted:
    if not events or not isinstance(events[-1], GatewayResponseCompleted):
        raise DomainOperationError(
            code="invalid_gateway_stream",
            message="the committed gateway response has no terminal event",
            retryable=True,
            details={"request_id": request.request_id},
        )
    return events[-1]


async def _close_stream(
    stream: AsyncIterator[GatewayEvent],
    *,
    request: GatewayRequest,
) -> None:
    close = getattr(stream, "aclose", None)
    if close is not None:
        close_call: Callable[[], Awaitable[None]] = close
        try:
            await close_call()
        except Exception as error:
            raise DomainOperationError(
                code="gateway_cleanup_failed",
                message="the gateway stream could not be closed",
                retryable=True,
                details={"request_id": request.request_id},
            ) from error


def _client_closed_error(request: GatewayRequest | None = None) -> DomainOperationError:
    if request is None:
        return DomainOperationError(
            code="gateway_closed",
            message="the gateway client is closed",
        )
    return DomainOperationError(
        code="gateway_closed",
        message="the gateway client is closed",
        details={"request_id": request.request_id},
    )


def _invalid_stream_error(
    request: GatewayRequest,
    message: str,
) -> DomainOperationError:
    return DomainOperationError(
        code="invalid_gateway_stream",
        message=message,
        details={"request_id": request.request_id},
    )


def _opaque_gateway_failure(
    request: GatewayRequest,
    *,
    retryable: bool,
) -> DomainOperationError:
    return DomainOperationError(
        code="model_gateway_failure",
        message="the model gateway stream failed",
        retryable=retryable,
        details={"request_id": request.request_id},
    )


def _ensure_not_terminal(terminal: bool, request: GatewayRequest) -> None:
    if terminal:
        raise _invalid_stream_error(
            request,
            "the gateway emitted data after its terminal event",
        )


def _enforce_stream_limits(
    *,
    request: GatewayRequest,
    event_count: int,
    stream_bytes: int,
    config: GatewayClientConfig,
) -> None:
    if event_count > config.max_stream_events or stream_bytes > config.max_stream_bytes:
        raise DomainOperationError(
            code="gateway_stream_limit",
            message="the gateway stream exceeded a configured limit",
            details={
                "event_count": event_count,
                "request_id": request.request_id,
                "stream_bytes": stream_bytes,
            },
        )


__all__ = [
    "DEFAULT_GATEWAY_REQUEST_BYTES",
    "DEFAULT_MODEL_ROUTES",
    "MAX_GATEWAY_REQUEST_BYTES",
    "MAX_GATEWAY_STREAM_BYTES",
    "MAX_GATEWAY_STREAM_EVENTS",
    "GatewayClient",
    "GatewayClientConfig",
]
