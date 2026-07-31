"""FastAPI control plane for durable sessions, runs, approvals, and events."""

import asyncio
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
    RewindRequest,
    RunCreationResponse,
)
from agent_core.control import ApprovalDecision, PersistedApproval, run_creation_hash
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import Run, Session
from agent_core.domain.status import RunStatus, SessionStatus
from agent_core.event_store import MAX_EVENT_PAGE_SIZE, StoredEvent

type CloseCallback = Callable[[], Awaitable[None]]


def create_app(  # noqa: PLR0915 - explicit route table remains locally auditable
    services: ApiServices,
    *,
    close: CloseCallback | None = None,
    max_request_body_bytes: int = MAX_HTTP_REQUEST_BODY_BYTES,
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

    @app.exception_handler(DomainOperationError)
    async def domain_error_handler(
        request: Request,
        error: DomainOperationError,
    ) -> JSONResponse:
        del request
        return JSONResponse(
            status_code=_error_status(error),
            content={"error": error.as_dict()},
            headers=(
                {"WWW-Authenticate": "Bearer"} if error.code == "authentication_required" else None
            ),
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

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, error: Exception) -> JSONResponse:
        del request, error
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

    async def principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
        return await services.authenticator.authenticate(authorization)

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
        run = Run(
            id=uuid.uuid4(),
            session_id=session.id,
            workspace_id=session.workspace_id,
            status=RunStatus.QUEUED,
            priority=body.priority,
            attempt=1,
            created_at=now,
        )
        result = await services.runs.create_idempotent(
            identity.tenant_id,
            run,
            idempotency_key=idempotency_key,
            creation_hash=run_creation_hash(priority=body.priority),
        )
        return RunCreationResponse(run=result.run, created=result.created)

    @app.get("/v1/runs/{run_id}", response_model=Run)
    async def get_run(
        run_id: uuid.UUID,
        identity: Annotated[Principal, Depends(principal)],
    ) -> Run:
        return await _require_run(services, identity, run_id)

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


def _error_status(error: DomainOperationError) -> int:
    if error.code == "authentication_required":
        return 401
    if error.code == "resource_not_found":
        return 404
    if error.code.endswith("_conflict") or error.code.endswith("_in_progress"):
        return 409
    if error.code == "gateway_rate_limited":
        return 429
    if error.retryable:
        return 503
    return 400


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
