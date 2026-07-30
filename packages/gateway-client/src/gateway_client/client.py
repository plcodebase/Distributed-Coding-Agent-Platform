"""Typed and bounded client boundary for the centralized model gateway."""

from __future__ import annotations

import asyncio
import json
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Annotated, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError
from agent_core.gateway import (
    GatewayEvent,
    GatewayRequest,
    GatewayResponseCompleted,
    ModelGateway,
    parse_gateway_event,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable
    from types import TracebackType


DEFAULT_MODEL_ROUTES = (
    "coding-default",
    "coding-fast",
    "coding-strong",
    "summarization",
    "code-review",
)
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
    max_stream_events: int = Field(default=100_000, ge=1, le=MAX_GATEWAY_STREAM_EVENTS)
    max_stream_bytes: int = Field(
        default=8 * 1024 * 1024,
        ge=1,
        le=MAX_GATEWAY_STREAM_BYTES,
    )

    @model_validator(mode="after")
    def validate_routes(self) -> Self:
        if len(self.route_names) != len(set(self.route_names)):
            raise ValueError("gateway route names must be unique")
        return self


class GatewayClient(AbstractAsyncContextManager["GatewayClient"]):
    """Validate, bound, and lifecycle-manage normalized gateway streams."""

    def __init__(
        self,
        gateway: ModelGateway,
        *,
        config: GatewayClientConfig | None = None,
        close: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._gateway = gateway
        self._config = config or GatewayClientConfig()
        self._routes = frozenset(self._config.route_names)
        self._close = close
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
        """Return a closed, bounded stream of provider-neutral events."""

        self._require_available(request=request)
        if request.route_name not in self._routes:
            raise DomainOperationError(
                code="gateway_route_not_allowed",
                message="the requested model route is not configured",
                details={
                    "request_id": request.request_id,
                    "route_name": request.route_name,
                },
            )

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
    "DEFAULT_MODEL_ROUTES",
    "MAX_GATEWAY_STREAM_BYTES",
    "MAX_GATEWAY_STREAM_EVENTS",
    "GatewayClient",
    "GatewayClientConfig",
]
