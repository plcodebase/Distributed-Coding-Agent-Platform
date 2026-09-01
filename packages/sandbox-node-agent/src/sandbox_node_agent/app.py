"""Private, capability-scoped HTTP API for node-owned sandboxes."""

from __future__ import annotations

import base64
import binascii
import hashlib
import uuid  # noqa: TC003 - FastAPI resolves path fields at runtime
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, FastAPI, Header, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse

from agent_core.artifacts import StoredObject
from agent_core.domain.errors import DomainOperationError
from agent_core.sandbox import CommandCompleted, WorkspaceSnapshot
from agent_core.tools import ToolExecutionCompleted
from sandbox_node_agent.body_limit import NodeRequestBodyLimitMiddleware
from sandbox_node_agent.contracts import (
    CommandEnvelope,
    CommandErrorEnvelope,
    CreateSandboxRequest,
    CreateSandboxResponse,
    ExecuteCommandRequest,
    ExecuteToolRequest,
    NodeAgentError,
    NodeAgentErrorResponse,
    ReadFileRequest,
    ReadFileResponse,
    RestoreSnapshotRequest,
    ToolEnvelope,
    ToolErrorEnvelope,
    WorkspacePatchRequest,
    WorkspacePatchResponse,
    WriteFileRequest,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from sandbox_node_agent.registry import NodeSandboxRegistry

MAX_NODE_REQUEST_BODY_BYTES = 142 * 1024 * 1024
_CAPABILITY_TOKEN_BYTES = 64


def create_node_agent_app(  # noqa: PLR0915 - explicit private route table
    registry: NodeSandboxRegistry,
    *,
    close_registry: bool = True,
    close: Callable[[], Awaitable[None]] | None = None,
    dependencies_ready: Callable[[], Awaitable[bool]] | None = None,
    max_request_body_bytes: int = MAX_NODE_REQUEST_BODY_BYTES,
) -> FastAPI:
    """Create the private ASGI application; TLS is mandatory at the server boundary."""

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            try:
                if close_registry:
                    await registry.aclose()
            finally:
                if close is not None:
                    await close()

    app = FastAPI(title="sandbox-node-agent", lifespan=lifespan)
    app.add_middleware(
        NodeRequestBodyLimitMiddleware,
        max_body_bytes=max_request_body_bytes,
    )

    async def capability(
        authorization: Annotated[str | None, Header()] = None,
    ) -> str:
        if authorization is None or not authorization.startswith("Bearer "):
            raise _not_found()
        token = authorization.removeprefix("Bearer ")
        if len(token) != _CAPABILITY_TOKEN_BYTES or any(
            character not in "0123456789abcdef" for character in token
        ):
            raise _not_found()
        return token

    @app.exception_handler(DomainOperationError)
    async def domain_error_handler(
        _request: object,
        error: DomainOperationError,
    ) -> JSONResponse:
        status = 404 if error.code == "sandbox_not_found" else (503 if error.retryable else 409)
        return JSONResponse(
            status_code=status,
            content=NodeAgentErrorResponse(
                error=NodeAgentError(
                    code=error.code,
                    message=error.message,
                    retryable=error.retryable,
                )
            ).model_dump(mode="json"),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _request: object,
        _error: RequestValidationError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=_error_response(
                "invalid_node_request",
                "the node request is invalid",
            ),
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_request: object, _error: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content=_error_response(
                "sandbox_node_failed",
                "the sandbox node operation failed",
                retryable=True,
            ),
        )

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> JSONResponse:
        try:
            available = await registry.ready() and (
                dependencies_ready is None or await dependencies_ready()
            )
        except Exception:  # Readiness is an opaque fail-closed operational boundary.
            available = False
        return JSONResponse(
            status_code=200 if available else 503,
            content={"status": "ready" if available else "unavailable"},
        )

    @app.post("/v1/sandboxes", response_model=CreateSandboxResponse)
    async def create_sandbox(body: CreateSandboxRequest) -> CreateSandboxResponse:
        return await registry.create(body)

    @app.post("/v1/sandboxes/{sandbox_id}/commands")
    async def execute_command(
        sandbox_id: uuid.UUID,
        body: ExecuteCommandRequest,
        token: str = Depends(capability),
    ) -> StreamingResponse:
        resources = await registry.get(sandbox_id, token)
        sandbox = resources.sandbox

        async def stream() -> AsyncIterator[bytes]:
            terminal = False
            try:
                async for event in sandbox.execute(body.command):
                    if terminal:
                        _raise_protocol_error("the sandbox emitted data after its terminal result")
                    terminal = isinstance(event, CommandCompleted)
                    yield (CommandEnvelope(event=event).model_dump_json() + "\n").encode("utf-8")
                if not terminal:
                    _raise_protocol_error("the sandbox command ended without a terminal result")
            except DomainOperationError as error:
                yield (
                    CommandErrorEnvelope(
                        error=NodeAgentError(
                            code=error.code,
                            message=error.message,
                            retryable=error.retryable,
                        )
                    ).model_dump_json()
                    + "\n"
                ).encode("utf-8")
            except Exception:
                yield (
                    CommandErrorEnvelope(
                        error=NodeAgentError(
                            code="sandbox_command_failed",
                            message="the sandbox command failed",
                            retryable=True,
                        )
                    ).model_dump_json()
                    + "\n"
                ).encode("utf-8")

        return StreamingResponse(stream(), media_type="application/x-ndjson")

    @app.post("/v1/sandboxes/{sandbox_id}/tools/{tool_name}")
    async def execute_tool(
        sandbox_id: uuid.UUID,
        tool_name: str,
        body: ExecuteToolRequest,
        token: str = Depends(capability),
    ) -> StreamingResponse:
        resources = await registry.get(sandbox_id, token)
        prepared = resources.tools.prepare(tool_name, body.arguments)

        async def stream() -> AsyncIterator[bytes]:
            terminal = False
            try:
                async for event in prepared.stream(body.context):
                    if terminal:
                        _raise_protocol_error("the tool emitted data after its terminal result")
                    terminal = isinstance(event, ToolExecutionCompleted)
                    yield (ToolEnvelope(event=event).model_dump_json() + "\n").encode("utf-8")
                if not terminal:
                    _raise_protocol_error("the tool ended without a terminal result")
            except DomainOperationError as error:
                yield (
                    ToolErrorEnvelope(
                        error=NodeAgentError(
                            code=error.code,
                            message=error.message,
                            retryable=error.retryable,
                        )
                    ).model_dump_json()
                    + "\n"
                ).encode("utf-8")
            except Exception:
                yield (
                    ToolErrorEnvelope(
                        error=NodeAgentError(
                            code="tool_execution_failed",
                            message="the node tool execution failed",
                            retryable=True,
                        )
                    ).model_dump_json()
                    + "\n"
                ).encode("utf-8")

        return StreamingResponse(stream(), media_type="application/x-ndjson")

    @app.post("/v1/sandboxes/{sandbox_id}/files/read", response_model=ReadFileResponse)
    async def read_file(
        sandbox_id: uuid.UUID,
        body: ReadFileRequest,
        token: str = Depends(capability),
    ) -> ReadFileResponse:
        sandbox = (await registry.get(sandbox_id, token)).sandbox
        content = await sandbox.read_file(body.path)
        if len(content) > body.max_bytes:
            raise DomainOperationError(
                code="sandbox_read_limit",
                message="the sandbox file exceeds the requested byte limit",
            )
        return ReadFileResponse(
            content_base64=base64.b64encode(content).decode("ascii"),
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )

    @app.post("/v1/sandboxes/{sandbox_id}/files/write", status_code=204)
    async def write_file(
        sandbox_id: uuid.UUID,
        body: WriteFileRequest,
        token: str = Depends(capability),
    ) -> Response:
        sandbox = (await registry.get(sandbox_id, token)).sandbox
        try:
            content = base64.b64decode(body.content_base64, validate=True)
        except (ValueError, binascii.Error):
            raise DomainOperationError(
                code="sandbox_write_invalid",
                message="the sandbox write content is invalid",
            ) from None
        if len(content) != body.size_bytes or hashlib.sha256(content).hexdigest() != body.sha256:
            raise DomainOperationError(
                code="sandbox_write_checksum_mismatch",
                message="the sandbox write content failed integrity validation",
            )
        await sandbox.write_file(body.path, content)
        return Response(status_code=204)

    @app.post(
        "/v1/sandboxes/{sandbox_id}/snapshots",
        response_model=WorkspaceSnapshot,
    )
    async def create_snapshot(
        sandbox_id: uuid.UUID,
        token: str = Depends(capability),
    ) -> WorkspaceSnapshot:
        sandbox = (await registry.get(sandbox_id, token)).sandbox
        return await sandbox.create_snapshot()

    @app.post(
        "/v1/sandboxes/{sandbox_id}/final-patch",
        response_model=StoredObject,
    )
    async def final_patch(
        sandbox_id: uuid.UUID,
        token: str = Depends(capability),
    ) -> StoredObject:
        resources = await registry.get(sandbox_id, token)
        if resources.final_patch_exporter is None:
            raise DomainOperationError(
                code="final_patch_unavailable",
                message="the sandbox does not support final patch export",
            )
        return await resources.final_patch_exporter.final_patch()

    @app.post(
        "/v1/sandboxes/{sandbox_id}/context/current-patch",
        response_model=WorkspacePatchResponse,
    )
    async def current_patch(
        sandbox_id: uuid.UUID,
        body: WorkspacePatchRequest,
        token: str = Depends(capability),
    ) -> WorkspacePatchResponse:
        resources = await registry.get(sandbox_id, token)
        if resources.current_patch_exporter is None:
            raise DomainOperationError(
                code="context_git_diff_unavailable",
                message="the sandbox does not support current Git diff context",
            )
        content = await resources.current_patch_exporter.current_patch(max_bytes=body.max_bytes)
        return WorkspacePatchResponse(
            content_base64=base64.b64encode(content).decode("ascii"),
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )

    @app.post("/v1/sandboxes/{sandbox_id}/restore", status_code=204)
    async def restore_snapshot(
        sandbox_id: uuid.UUID,
        body: RestoreSnapshotRequest,
        token: str = Depends(capability),
    ) -> Response:
        sandbox = (await registry.get(sandbox_id, token)).sandbox
        await sandbox.restore_snapshot(body.snapshot)
        return Response(status_code=204)

    @app.post("/v1/sandboxes/{sandbox_id}/cancel", status_code=204)
    async def cancel(
        sandbox_id: uuid.UUID,
        token: str = Depends(capability),
    ) -> Response:
        sandbox = (await registry.get(sandbox_id, token)).sandbox
        await sandbox.cancel_active()
        return Response(status_code=204)

    @app.delete("/v1/sandboxes/{sandbox_id}", status_code=204)
    async def destroy(
        sandbox_id: uuid.UUID,
        token: str = Depends(capability),
    ) -> Response:
        await registry.destroy(sandbox_id, token)
        return Response(status_code=204)

    return app


def _error_response(code: str, message: str, *, retryable: bool = False) -> dict[str, object]:
    return NodeAgentErrorResponse(
        error=NodeAgentError(code=code, message=message, retryable=retryable)
    ).model_dump(mode="json")


def _not_found() -> DomainOperationError:
    return DomainOperationError(code="sandbox_not_found", message="the sandbox was not found")


def _raise_protocol_error(message: str) -> None:
    raise DomainOperationError(code="sandbox_protocol_error", message=message)


__all__ = ["MAX_NODE_REQUEST_BODY_BYTES", "create_node_agent_app"]
