"""Least-privilege HTTP and WebSocket event gateway."""

import asyncio
import hmac
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from typing import Annotated

import anyio
from fastapi import Depends, FastAPI, Header, Query, Request, Response, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from agent_api.app import (
    MAX_METRICS_TOKEN_BYTES,
    MIN_METRICS_TOKEN_BYTES,
    _drain_event_sockets,
    _error_headers,
    _error_status,
    _run_event_socket,
)
from agent_api.auth import Principal
from agent_api.body_limit import (
    MAX_CONFIGURED_HTTP_REQUEST_BODY_BYTES,
    MAX_HTTP_REQUEST_BODY_BYTES,
    RequestBodyLimitMiddleware,
)
from agent_api.dependencies import EventGatewayServices
from agent_api.schemas import EventListResponse, HealthResponse
from agent_core.domain.errors import DomainOperationError
from agent_core.event_store import MAX_EVENT_PAGE_SIZE
from platform_telemetry import PlatformTelemetry

type CloseCallback = Callable[[], Awaitable[None]]


def create_event_gateway_app(  # noqa: PLR0915 - explicit least-privilege route table
    services: EventGatewayServices,
    *,
    close: CloseCallback | None = None,
    max_request_body_bytes: int = MAX_HTTP_REQUEST_BODY_BYTES,
    telemetry: PlatformTelemetry | None = None,
    metrics_token: str | None = None,
) -> FastAPI:
    """Create an event-only application with no control-plane mutation routes."""

    if type(max_request_body_bytes) is not int or not (
        1 <= max_request_body_bytes <= MAX_CONFIGURED_HTTP_REQUEST_BODY_BYTES
    ):
        raise ValueError("max_request_body_bytes is outside the supported range")
    if metrics_token is not None and not (
        MIN_METRICS_TOKEN_BYTES <= len(metrics_token.encode("utf-8")) <= MAX_METRICS_TOKEN_BYTES
    ):
        raise ValueError("metrics_token must be between 16 bytes and 4 KiB")
    active_event_sockets: set[asyncio.Task[None]] = set()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        del application
        try:
            yield
        finally:
            with anyio.CancelScope(shield=True):
                try:
                    await _drain_event_sockets(active_event_sockets)
                finally:
                    if close is not None:
                        await close()

    app = FastAPI(title="Agent Platform Event Gateway", version="0.1.0", lifespan=lifespan)
    app.add_middleware(RequestBodyLimitMiddleware, max_body_bytes=max_request_body_bytes)

    @app.exception_handler(DomainOperationError)
    async def domain_error_handler(
        request: Request,
        error: DomainOperationError,
    ) -> JSONResponse:
        del request
        return JSONResponse(
            status_code=_error_status(error),
            content={"error": error.as_dict()},
            headers=_error_headers(error),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request,
        error: RequestValidationError,
    ) -> JSONResponse:
        del request, error
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "invalid_request",
                    "message": "the request did not match the required schema",
                    "retryable": False,
                    "details": {},
                }
            },
        )

    async def principal(
        authorization: Annotated[str | None, Header()] = None,
    ) -> Principal:
        identity = await services.authenticator.authenticate(authorization)
        if services.tenant_access is not None:
            await services.tenant_access.require_active(identity.tenant_id)
        return identity

    @app.get("/metrics", include_in_schema=False)
    async def metrics(authorization: Annotated[str | None, Header()] = None) -> Response:
        if telemetry is None:
            return Response(status_code=404)
        if metrics_token is not None:
            scheme, separator, candidate = (authorization or "").partition(" ")
            if (
                not separator
                or scheme.lower() != "bearer"
                or not hmac.compare_digest(candidate, metrics_token)
            ):
                return Response(status_code=401, headers={"WWW-Authenticate": "Bearer"})
        return Response(
            content=telemetry.metrics.render(),
            media_type=telemetry.metrics.content_type,
        )

    @app.get("/health/live", response_model=HealthResponse)
    async def live() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get(
        "/health/ready",
        response_model=HealthResponse,
        responses={503: {"model": HealthResponse}},
    )
    async def ready(response: Response) -> HealthResponse:
        if not await services.readiness.ready():
            response.status_code = 503
            return HealthResponse(status="unavailable")
        return HealthResponse(status="ok")

    @app.get("/v1/runs/{run_id}/events", response_model=EventListResponse)
    async def list_events(
        run_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
        after: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=MAX_EVENT_PAGE_SIZE)] = 100,
    ) -> EventListResponse:
        if await services.runs.get(identity.tenant_id, run_id) is None:
            raise DomainOperationError(
                code="resource_not_found",
                message="run was not found",
                details={"id": str(run_id)},
            )
        page = await services.events.read_page(
            identity.tenant_id,
            run_id,
            after=after,
            limit=limit,
        )
        return EventListResponse(
            events=page.events,
            next_after=page.next_after,
            has_more=page.has_more,
        )

    @app.websocket("/v1/runs/{run_id}/stream")
    async def stream_events(
        websocket: WebSocket,
        run_id: uuid.UUID,
        after: Annotated[int, Query(ge=0)] = 0,
    ) -> None:
        try:
            identity = await services.authenticator.authenticate(
                websocket.headers.get("authorization")
            )
            if services.tenant_access is not None:
                await services.tenant_access.require_active(identity.tenant_id)
        except DomainOperationError as error:
            await websocket.close(code=4401 if error.code == "authentication_required" else 4403)
            return
        except Exception:
            await websocket.close(code=1011)
            return
        try:
            if await services.runs.get(identity.tenant_id, run_id) is None:
                await websocket.close(code=4404)
                return
        except Exception:
            await websocket.close(code=1011)
            return
        await websocket.accept()
        if telemetry is not None and after > 0:
            telemetry.metrics.event_reconnects.inc()
        event_stream = services.events.stream(identity.tenant_id, run_id, after=after)
        handler_task = asyncio.current_task()
        if handler_task is None:
            close_stream = getattr(event_stream, "aclose", None)
            if close_stream is not None:
                await close_stream()
            await websocket.close(code=1011)
            return
        active_event_sockets.add(handler_task)
        try:
            await _run_event_socket(websocket, event_stream)
        except Exception:
            with suppress(Exception):
                await websocket.close(code=1011)
        finally:
            active_event_sockets.discard(handler_task)

    return app


__all__ = ["create_event_gateway_app"]
