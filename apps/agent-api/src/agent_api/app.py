"""FastAPI control plane for durable sessions, runs, approvals, and events."""

import asyncio
import hmac
import math
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import Annotated

import anyio
from fastapi import (
    Depends,
    FastAPI,
    Header,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from agent_api.auth import Principal
from agent_api.body_limit import (
    MAX_CONFIGURED_HTTP_REQUEST_BODY_BYTES,
    MAX_HTTP_REQUEST_BODY_BYTES,
    RequestBodyLimitMiddleware,
)
from agent_api.dependencies import ApiServices
from agent_api.schemas import (
    ApprovalDecisionRequest,
    CreateRunRequest,
    CreateSessionRequest,
    EventListResponse,
    HealthResponse,
    MemoryListResponse,
    MemorySettingRequest,
    RewindRequest,
    RunCreationResponse,
    RunStatusResponse,
)
from agent_core.context import CONTEXT_COMPACTION_ROUTE
from agent_core.control import (
    ApprovalDecision,
    PersistedApproval,
    PersistedContextCompaction,
    PersistedMemory,
    PersistedTaskState,
    TaskPlanUpdate,
    run_creation_hash,
)
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import Run, Session
from agent_core.domain.status import RunStatus, SessionStatus
from agent_core.event_store import MAX_EVENT_PAGE_SIZE, StoredEvent
from platform_telemetry import ErrorCategory, PlatformTelemetry, TelemetryContext

type CloseCallback = Callable[[], Awaitable[None]]
type RequestHandler = Callable[[Request], Awaitable[Response]]
MIN_METRICS_TOKEN_BYTES = 16
MAX_METRICS_TOKEN_BYTES = 4_096


def create_app(  # noqa: PLR0915 - explicit route table remains locally auditable
    services: ApiServices,
    *,
    close: CloseCallback | None = None,
    max_request_body_bytes: int = MAX_HTTP_REQUEST_BODY_BYTES,
    telemetry: PlatformTelemetry | None = None,
    metrics_token: str | None = None,
) -> FastAPI:
    """Compose one dependency-injected API instance without global mutable state."""

    if (
        type(max_request_body_bytes) is not int
        or not 1 <= max_request_body_bytes <= MAX_CONFIGURED_HTTP_REQUEST_BODY_BYTES
    ):
        raise ValueError(
            "max_request_body_bytes must be an integer in "
            f"[1, {MAX_CONFIGURED_HTTP_REQUEST_BODY_BYTES}]"
        )
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

    app = FastAPI(
        title="Agent Platform API",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_body_bytes=max_request_body_bytes,
    )

    if telemetry is not None:

        @app.middleware("http")
        async def telemetry_middleware(
            request: Request,
            call_next: RequestHandler,
        ) -> Response:
            started = time.monotonic()
            parent = telemetry.extract(dict(request.headers))
            route = "unmatched"
            status = 500
            with telemetry.span(
                "api.request",
                parent=parent,
                attributes={
                    "http.request.method": request.method,
                },
            ) as span:
                request.state.telemetry_span = span
                try:
                    response = await call_next(request)
                    status = response.status_code
                    return response
                finally:
                    route_object = request.scope.get("route")
                    route_path = getattr(route_object, "path", None)
                    if isinstance(route_path, str):
                        route = route_path
                    route_label = telemetry.metrics.route(route)
                    method = telemetry.metrics.method(request.method)
                    telemetry.metrics.api_requests.labels(
                        route=route_label,
                        method=method,
                        status=str(status),
                    ).inc()
                    telemetry.metrics.api_duration.labels(
                        route=route_label,
                        method=method,
                    ).observe(time.monotonic() - started)
                    span.set_attribute("http.route", route)
                    span.set_attribute("http.response.status_code", status)

    @app.exception_handler(DomainOperationError)
    async def domain_error_handler(
        request: Request,
        error: DomainOperationError,
    ) -> JSONResponse:
        _record_api_error(
            request,
            telemetry,
            category=_domain_error_category(error),
            retryable=error.retryable,
        )
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
        del error
        _record_api_error(
            request,
            telemetry,
            category=ErrorCategory.VALIDATION,
            retryable=False,
        )
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

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, error: Exception) -> JSONResponse:
        del error
        _record_api_error(
            request,
            telemetry,
            category=ErrorCategory.INTERNAL,
            retryable=True,
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "internal_error",
                    "message": "the request could not be completed",
                    "retryable": True,
                    "details": {},
                }
            },
        )

    async def principal(
        request: Request,
        authorization: Annotated[str | None, Header()] = None,
    ) -> Principal:
        authenticated = await services.authenticator.authenticate(authorization)
        if telemetry is not None:
            span = getattr(request.state, "telemetry_span", None)
            if span is not None:
                telemetry.annotate(
                    span,
                    TelemetryContext(tenant_id=str(authenticated.tenant_id)),
                )
        return authenticated

    @app.get("/metrics", include_in_schema=False)
    async def metrics(
        authorization: Annotated[str | None, Header()] = None,
    ) -> Response:
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

    @app.post("/v1/sessions", response_model=Session, status_code=201)
    async def create_session(
        body: CreateSessionRequest,
        identity: Annotated[Principal, Depends(principal)],
    ) -> Session:
        now = datetime.now(UTC)
        session = Session(
            id=uuid.uuid4(),
            tenant_id=identity.tenant_id,
            workspace_id=body.workspace_id,
            status=SessionStatus.ACTIVE,
            approval_mode=body.approval_mode,
            model_route=body.model_route,
            memory_enabled=body.memory_enabled,
            created_at=now,
            updated_at=now,
        )
        return await services.sessions.create(session)

    @app.get("/v1/sessions/{session_id}", response_model=Session)
    async def get_session(
        session_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
    ) -> Session:
        session = await services.sessions.get(identity.tenant_id, session_id)
        if session is None:
            raise _not_found("session", session_id)
        return session

    @app.post(
        "/v1/sessions/{session_id}/compact",
        response_model=PersistedContextCompaction,
        status_code=202,
    )
    async def compact_session(
        session_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
        idempotency_key: Annotated[
            str,
            Header(
                alias="Idempotency-Key",
                min_length=1,
                max_length=255,
                pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
            ),
        ],
    ) -> PersistedContextCompaction:
        session = await services.sessions.get(identity.tenant_id, session_id)
        if session is None:
            raise _not_found("session", session_id)
        context = _require_service(services.context, "context compaction")
        compaction = await context.request_compaction(
            identity.tenant_id,
            session_id,
            compaction_id=uuid.uuid4(),
            idempotency_key=idempotency_key,
            route_name=CONTEXT_COMPACTION_ROUTE,
            requested_at=datetime.now(UTC),
        )
        if compaction is None:
            raise _not_found("session", session_id)
        return compaction

    @app.get(
        "/v1/sessions/{session_id}/compactions/{compaction_id}",
        response_model=PersistedContextCompaction,
    )
    async def get_compaction(
        session_id: uuid.UUID,
        compaction_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
    ) -> PersistedContextCompaction:
        session = await services.sessions.get(identity.tenant_id, session_id)
        if session is None:
            raise _not_found("session", session_id)
        context = _require_service(services.context, "context compaction")
        compaction = await context.get(identity.tenant_id, session_id, compaction_id)
        if compaction is None:
            raise _not_found("context compaction", compaction_id)
        return compaction

    @app.patch("/v1/sessions/{session_id}/memory", response_model=Session)
    async def set_session_memory(
        session_id: uuid.UUID,
        body: MemorySettingRequest,
        identity: Annotated[Principal, Depends(principal)],
    ) -> Session:
        memories = _require_service(services.memories, "long-term memory")
        changed = await memories.set_session_enabled(
            identity.tenant_id,
            session_id,
            enabled=body.enabled,
            updated_at=datetime.now(UTC),
        )
        if not changed:
            raise _not_found("session", session_id)
        session = await services.sessions.get(identity.tenant_id, session_id)
        if session is None:
            raise _not_found("session", session_id)
        return session

    @app.get("/v1/sessions/{session_id}/memories", response_model=MemoryListResponse)
    async def list_memories(
        session_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
    ) -> MemoryListResponse:
        session = await services.sessions.get(identity.tenant_id, session_id)
        if session is None:
            raise _not_found("session", session_id)
        memories = _require_service(services.memories, "long-term memory")
        return MemoryListResponse(
            memories=await memories.list_active(
                identity.tenant_id,
                session_id,
                limit=limit,
            )
        )

    @app.delete(
        "/v1/sessions/{session_id}/memories/{memory_id}",
        response_model=PersistedMemory,
    )
    async def archive_memory(
        session_id: uuid.UUID,
        memory_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
    ) -> PersistedMemory:
        session = await services.sessions.get(identity.tenant_id, session_id)
        if session is None:
            raise _not_found("session", session_id)
        memories = _require_service(services.memories, "long-term memory")
        memory = await memories.archive(
            identity.tenant_id,
            session_id,
            memory_id,
            archived_at=datetime.now(UTC),
        )
        if memory is None:
            raise _not_found("memory", memory_id)
        return memory

    @app.post("/v1/sessions/{session_id}/runs", response_model=RunCreationResponse)
    async def create_run(
        session_id: uuid.UUID,
        body: CreateRunRequest,
        identity: Annotated[Principal, Depends(principal)],
        idempotency_key: Annotated[
            str,
            Header(
                alias="Idempotency-Key",
                min_length=1,
                max_length=255,
                pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
            ),
        ],
    ) -> RunCreationResponse:
        session = await services.sessions.get(identity.tenant_id, session_id)
        if session is None:
            raise _not_found("session", session_id)
        if session.status is not SessionStatus.ACTIVE:
            raise DomainOperationError(
                code="session_state_conflict",
                message="runs may only be created for active sessions",
            )
        now = datetime.now(UTC)
        trace_carrier: dict[str, str] = {}
        if telemetry is not None:
            telemetry.inject(trace_carrier)
        run = Run(
            id=uuid.uuid4(),
            session_id=session.id,
            workspace_id=session.workspace_id,
            status=RunStatus.QUEUED,
            priority=body.priority,
            priority_class=body.priority_class,
            attempt=1,
            traceparent=trace_carrier.get("traceparent"),
            tracestate=trace_carrier.get("tracestate"),
            created_at=now,
        )
        result = await services.runs.create_idempotent(
            identity.tenant_id,
            run,
            idempotency_key=idempotency_key,
            creation_hash=run_creation_hash(
                priority=body.priority,
                priority_class=body.priority_class,
            ),
        )
        if telemetry is not None and result.created:
            telemetry.metrics.runs_accepted.inc()
        return RunCreationResponse(run=result.run, created=result.created)

    @app.get("/v1/runs/{run_id}", response_model=Run)
    async def get_run(
        run_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
    ) -> Run:
        return await _require_run(services, identity, run_id)

    @app.get("/v1/runs/{run_id}/status", response_model=RunStatusResponse)
    async def run_status(
        run_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
    ) -> RunStatusResponse:
        run = await _require_run(services, identity, run_id)
        task_plan = (
            await services.tasks.get(identity.tenant_id, run_id)
            if services.tasks is not None
            else None
        )
        latest_compaction = (
            await services.context.latest_completed(identity.tenant_id, run.session_id)
            if services.context is not None
            else None
        )
        return RunStatusResponse(
            run=run,
            task_plan=task_plan,
            latest_compaction=latest_compaction,
        )

    @app.get("/v1/runs/{run_id}/task-plan", response_model=PersistedTaskState)
    async def get_task_plan(
        run_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
    ) -> PersistedTaskState:
        _ = await _require_run(services, identity, run_id)
        tasks = _require_service(services.tasks, "task tracking")
        state = await tasks.get(identity.tenant_id, run_id)
        if state is None:
            raise _not_found("task plan", run_id)
        return state

    @app.put("/v1/runs/{run_id}/task-plan", response_model=PersistedTaskState)
    async def update_task_plan(
        run_id: uuid.UUID,
        body: TaskPlanUpdate,
        identity: Annotated[Principal, Depends(principal)],
    ) -> PersistedTaskState:
        _ = await _require_run(services, identity, run_id)
        tasks = _require_service(services.tasks, "task tracking")
        state = await tasks.update(
            identity.tenant_id,
            run_id,
            body,
            plan_id=uuid.uuid4(),
            created_at=datetime.now(UTC),
        )
        if state is None:
            raise _not_found("run", run_id)
        return state

    @app.post("/v1/runs/{run_id}/cancel", response_model=Run)
    async def cancel_run(
        run_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
    ) -> Run:
        run = await services.runs.request_cancel(
            identity.tenant_id,
            run_id,
            occurred_at=datetime.now(UTC),
        )
        if run is None:
            raise _not_found("run", run_id)
        return run

    @app.post(
        "/v1/runs/{run_id}/approvals/{approval_id}",
        response_model=PersistedApproval,
    )
    async def decide_approval(
        run_id: uuid.UUID,
        approval_id: uuid.UUID,
        body: ApprovalDecisionRequest,
        identity: Annotated[Principal, Depends(principal)],
    ) -> PersistedApproval:
        approval = await services.approvals.decide(
            identity.tenant_id,
            run_id,
            approval_id,
            ApprovalDecision(
                approved=body.approved,
                decided_by=identity.subject,
                decided_at=datetime.now(UTC),
            ),
        )
        if approval is None:
            raise _not_found("approval", approval_id)
        return approval

    @app.post("/v1/runs/{run_id}/rewind", response_model=Run)
    async def rewind_run(
        run_id: uuid.UUID,
        body: RewindRequest,
        identity: Annotated[Principal, Depends(principal)],
    ) -> Run:
        run = await services.runs.rewind(
            identity.tenant_id,
            run_id,
            body.checkpoint_id,
        )
        if run is None:
            raise _not_found("run or checkpoint", run_id)
        return run

    @app.get("/v1/runs/{run_id}/events", response_model=EventListResponse)
    async def list_events(
        run_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
        after: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=MAX_EVENT_PAGE_SIZE)] = 100,
    ) -> EventListResponse:
        _ = await _require_run(services, identity, run_id)
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
        except DomainOperationError:
            await websocket.close(code=4401)
            return
        except Exception:
            await websocket.close(code=1011)
            return
        try:
            run = await services.runs.get(identity.tenant_id, run_id)
            if run is None:
                await websocket.close(code=4404)
                return
        except Exception:
            await websocket.close(code=1011)
            return
        await websocket.accept()
        if telemetry is not None and after > 0:
            telemetry.metrics.event_reconnects.inc()
        event_stream = services.events.stream(
            identity.tenant_id,
            run_id,
            after=after,
        )
        handler_task: asyncio.Task[None] | None = asyncio.current_task()
        if handler_task is None:
            await _close_event_stream(event_stream)
            await websocket.close(code=1011)
            return
        active_event_sockets.add(handler_task)
        try:
            await _run_event_socket(websocket, event_stream)
        except WebSocketDisconnect:
            pass
        except Exception:
            with suppress(Exception):
                await websocket.close(code=1011)
        finally:
            active_event_sockets.discard(handler_task)

    return app


async def _require_run(
    services: ApiServices,
    identity: Principal,
    run_id: uuid.UUID,
) -> Run:
    run = await services.runs.get(identity.tenant_id, run_id)
    if run is None:
        raise _not_found("run", run_id)
    return run


def _not_found(resource: str, identifier: uuid.UUID) -> DomainOperationError:
    return DomainOperationError(
        code="resource_not_found",
        message=f"{resource} was not found",
        details={"id": str(identifier)},
    )


def _require_service[T](service: T | None, feature: str) -> T:
    if service is None:
        raise DomainOperationError(
            code="feature_unavailable",
            message=f"{feature} is not configured",
            retryable=True,
        )
    return service


def _error_status(error: DomainOperationError) -> int:
    if error.code == "authentication_required":
        return 401
    if error.code == "resource_not_found":
        return 404
    if error.code.endswith("_conflict") or error.code.endswith("_in_progress"):
        return 409
    if error.code in {
        "gateway_capacity_exhausted",
        "gateway_rate_limited",
        "queue_overloaded",
        "tenant_queue_quota_exceeded",
    }:
        return 429
    if error.retryable:
        return 503
    return 400


def _error_headers(error: DomainOperationError) -> dict[str, str] | None:
    headers: dict[str, str] = {}
    if error.code == "authentication_required":
        headers["WWW-Authenticate"] = "Bearer"
    retry_after = error.details.get("retry_after_seconds")
    if (
        not isinstance(retry_after, bool)
        and isinstance(retry_after, (int, float))
        and math.isfinite(retry_after)
        and retry_after > 0
    ):
        headers["Retry-After"] = str(max(1, math.ceil(retry_after)))
    return headers or None


def _record_api_error(
    request: Request,
    telemetry: PlatformTelemetry | None,
    *,
    category: ErrorCategory,
    retryable: bool,
) -> None:
    if telemetry is None:
        return
    span = getattr(request.state, "telemetry_span", None)
    if span is not None:
        telemetry.record_error(
            span,
            category=category,
            component="agent-api",
            retryable=retryable,
        )


def _domain_error_category(error: DomainOperationError) -> ErrorCategory:
    code = error.code
    exact = {
        "authentication_required": ErrorCategory.AUTHENTICATION,
        "resource_not_found": ErrorCategory.VALIDATION,
    }
    if code in exact:
        return exact[code]
    rules = (
        (("authoriz", "permission", "denied"), ErrorCategory.AUTHORIZATION),
        (("capacity", "overload", "quota"), ErrorCategory.CAPACITY),
        (("rate_limit",), ErrorCategory.RATE_LIMIT),
        (("timeout", "expired"), ErrorCategory.TIMEOUT),
        (("cancel",), ErrorCategory.CANCELLED),
        (("conflict", "in_progress", "lease_lost"), ErrorCategory.CONFLICT),
        (("sandbox", "command"), ErrorCategory.SANDBOX),
        (("persist", "database", "event_store"), ErrorCategory.PERSISTENCE),
        (("provider",), ErrorCategory.PROVIDER),
        (("gateway", "unavailable"), ErrorCategory.DEPENDENCY),
    )
    category = next(
        (category for markers, category in rules if any(marker in code for marker in markers)),
        None,
    )
    if category is not None:
        return category
    if code.startswith("invalid_") or code.endswith("_invalid"):
        return ErrorCategory.VALIDATION
    return ErrorCategory.INTERNAL


async def _close_event_stream(stream: AsyncIterator[StoredEvent]) -> None:
    close = getattr(stream, "aclose", None)
    if close is None:
        return
    close_call: Callable[[], Awaitable[None]] = close

    async def invoke_close() -> None:
        await close_call()

    cleanup = asyncio.create_task(invoke_close())
    await _await_cleanup(cleanup)


async def _await_cleanup(cleanup: asyncio.Task[None]) -> None:
    with anyio.CancelScope(shield=True):
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise


async def _run_event_socket(
    websocket: WebSocket,
    event_stream: AsyncIterator[StoredEvent],
) -> None:
    async def send_events() -> None:
        try:
            async for event in event_stream:
                await websocket.send_json(event.model_dump(mode="json"))
        finally:
            await _close_event_stream(event_stream)

    async def wait_for_disconnect() -> None:
        message = await websocket.receive()
        if message["type"] != "websocket.disconnect":
            await websocket.close(code=1008)

    sender = asyncio.create_task(send_events())
    receiver = asyncio.create_task(wait_for_disconnect())
    tasks = (sender, receiver)
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            await task
    finally:
        cleanup = asyncio.create_task(_cancel_socket_tasks(tasks))
        await _await_cleanup(cleanup)


async def _cancel_socket_tasks(tasks: tuple[asyncio.Task[None], ...]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    _ = await asyncio.gather(*tasks, return_exceptions=True)


async def _drain_event_sockets(tasks: set[asyncio.Task[None]]) -> None:
    if not tasks:
        return
    cleanup = asyncio.create_task(_cancel_socket_tasks(tuple(tasks)))
    await _await_cleanup(cleanup)


__all__ = ["create_app"]
