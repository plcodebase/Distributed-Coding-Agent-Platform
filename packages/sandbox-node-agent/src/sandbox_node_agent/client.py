"""Worker-side remote Sandbox implementation over the mTLS node-agent API."""

from __future__ import annotations

import base64
import binascii
import hashlib
from typing import TYPE_CHECKING, NoReturn, Self

import httpx
from pydantic import TypeAdapter

from agent_core.artifacts import StoredObject
from agent_core.domain.errors import DomainOperationError
from agent_core.sandbox import CommandCompleted, CommandEvent, CommandSpec, WorkspaceSnapshot
from agent_core.tools import (
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolExecutionEvent,
    ToolRegistry,
)
from sandbox_node_agent.contracts import (
    CommandErrorEnvelope,
    CommandStreamEnvelope,
    CreateSandboxRequest,
    CreateSandboxResponse,
    ExecuteToolRequest,
    NodeAgentErrorResponse,
    NodeToolDefinition,
    ReadFileRequest,
    ReadFileResponse,
    RestoreSnapshotRequest,
    ToolErrorEnvelope,
    ToolStreamEnvelope,
    WorkspacePatchRequest,
    WorkspacePatchResponse,
    WriteFileRequest,
)
from sandbox_node_agent.remote_tools import create_remote_tool_registry
from sandbox_node_agent.tls import NodeAgentTlsSettings, create_client_ssl_context

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from agent_core.domain.base import FrozenJsonObject

_STREAM_ADAPTER: TypeAdapter[CommandStreamEnvelope] = TypeAdapter(CommandStreamEnvelope)
_TOOL_STREAM_ADAPTER: TypeAdapter[ToolStreamEnvelope] = TypeAdapter(ToolStreamEnvelope)
_MAX_PROTOCOL_OVERHEAD_BYTES = 1024 * 1024
_HTTP_SUCCESS_MIN = 200
_HTTP_SUCCESS_MAX = 300
_HTTP_SERVER_ERROR_MIN = 500


class RemoteSandbox:
    """Capability-scoped proxy; workers never receive a Podman runtime socket."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        owns_client: bool,
        sandbox_id: object,
        capability_token: str,
        tools: tuple[NodeToolDefinition, ...],
    ) -> None:
        response = CreateSandboxResponse(
            sandbox_id=sandbox_id,
            capability_token=capability_token,
        )
        self._client = client
        self._owns_client = owns_client
        self._sandbox_id = response.sandbox_id
        self._capability = response.capability_token
        self._tool_definitions = tools
        self._destroyed = False

    @classmethod
    async def create(
        cls,
        request: CreateSandboxRequest,
        settings: NodeAgentTlsSettings,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> Self:
        actual_client = client
        owns_client = client is None
        if actual_client is None:
            context = create_client_ssl_context(settings)
            actual_client = httpx.AsyncClient(
                base_url=settings.base_url,
                verify=context,
                timeout=httpx.Timeout(
                    settings.operation_timeout_seconds,
                    connect=settings.connect_timeout_seconds,
                ),
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
                trust_env=False,
            )
        try:
            response = await actual_client.post(
                "/v1/sandboxes",
                content=request.model_dump_json(),
                headers={"content-type": "application/json"},
            )
            await _raise_for_status(response)
            created = CreateSandboxResponse.model_validate_json(response.content)
            return cls(
                client=actual_client,
                owns_client=owns_client,
                sandbox_id=created.sandbox_id,
                capability_token=created.capability_token,
                tools=created.tools,
            )
        except BaseException:
            if owns_client:
                await actual_client.aclose()
            raise

    async def execute(self, command: CommandSpec) -> AsyncIterator[CommandEvent]:
        self._require_available()
        terminal = False
        observed_bytes = 0
        try:
            async with self._client.stream(
                "POST",
                f"/v1/sandboxes/{self._sandbox_id}/commands",
                content=('{"command":' + command.model_dump_json() + "}").encode("utf-8"),
                headers=self._headers(),
            ) as response:
                await _raise_for_status(response)
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    observed_bytes += len(line.encode("utf-8")) + 1
                    if observed_bytes > command.max_output_bytes + _MAX_PROTOCOL_OVERHEAD_BYTES:
                        _raise_remote_error(
                            code="sandbox_protocol_limit",
                            message="the node-agent command stream exceeded its byte limit",
                        )
                    envelope = _STREAM_ADAPTER.validate_json(line)
                    if isinstance(envelope, CommandErrorEnvelope):
                        _raise_remote_error(
                            code=envelope.error.code,
                            message=envelope.error.message,
                            retryable=envelope.error.retryable,
                        )
                    if terminal:
                        _raise_remote_error(
                            code="sandbox_protocol_error",
                            message="the node agent emitted data after command completion",
                        )
                    terminal = isinstance(envelope.event, CommandCompleted)
                    yield envelope.event
        except DomainOperationError:
            raise
        except (httpx.HTTPError, ValueError, UnicodeError):
            raise DomainOperationError(
                code="sandbox_node_unavailable",
                message="the sandbox node request failed",
                retryable=True,
            ) from None
        if not terminal:
            raise DomainOperationError(
                code="sandbox_protocol_error",
                message="the node agent command ended without a terminal result",
                retryable=True,
            )

    async def read_file(self, path: str, *, max_bytes: int = 4 * 1024 * 1024) -> bytes:
        self._require_available()
        response = await self._post(
            f"/v1/sandboxes/{self._sandbox_id}/files/read",
            ReadFileRequest(path=path, max_bytes=max_bytes).model_dump_json(),
        )
        payload = ReadFileResponse.model_validate_json(response.content)
        try:
            content = base64.b64decode(payload.content_base64, validate=True)
        except (ValueError, binascii.Error):
            raise _protocol_error() from None
        if (
            len(content) != payload.size_bytes
            or hashlib.sha256(content).hexdigest() != payload.sha256
        ):
            raise _protocol_error()
        return content

    async def current_patch(self, *, max_bytes: int = 1024 * 1024) -> bytes:
        self._require_available()
        response = await self._post(
            f"/v1/sandboxes/{self._sandbox_id}/context/current-patch",
            WorkspacePatchRequest(max_bytes=max_bytes).model_dump_json(),
        )
        payload = WorkspacePatchResponse.model_validate_json(response.content)
        try:
            content = base64.b64decode(payload.content_base64, validate=True)
        except (ValueError, binascii.Error):
            raise _protocol_error() from None
        if (
            len(content) != payload.size_bytes
            or hashlib.sha256(content).hexdigest() != payload.sha256
        ):
            raise _protocol_error()
        return content

    def tool_registry(self) -> ToolRegistry:
        """Build the locally validating proxy registrations advertised by the node."""

        self._require_available()
        return create_remote_tool_registry(self, self._tool_definitions)

    async def _execute_tool(
        self,
        tool_name: str,
        arguments: FrozenJsonObject,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        self._require_available()
        terminal = False
        observed_bytes = 0
        request = ExecuteToolRequest(arguments=arguments, context=context)
        try:
            async with self._client.stream(
                "POST",
                f"/v1/sandboxes/{self._sandbox_id}/tools/{tool_name}",
                content=request.model_dump_json().encode("utf-8"),
                headers=self._headers(),
            ) as response:
                await _raise_for_status(response)
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    observed_bytes += len(line.encode("utf-8")) + 1
                    limit = (
                        context.max_output_bytes
                        + context.max_result_bytes
                        + _MAX_PROTOCOL_OVERHEAD_BYTES
                    )
                    if observed_bytes > limit:
                        _raise_remote_error(
                            code="tool_output_limit",
                            message="the node tool stream exceeded its byte limit",
                        )
                    envelope = _TOOL_STREAM_ADAPTER.validate_json(line)
                    if isinstance(envelope, ToolErrorEnvelope):
                        _raise_remote_error(
                            code=envelope.error.code,
                            message=envelope.error.message,
                            retryable=envelope.error.retryable,
                        )
                    if terminal:
                        _raise_remote_error(
                            code="tool_protocol_error",
                            message="the node emitted data after tool completion",
                        )
                    terminal = isinstance(envelope.event, ToolExecutionCompleted)
                    yield envelope.event
        except DomainOperationError:
            raise
        except (httpx.HTTPError, ValueError, UnicodeError) as error:
            raise DomainOperationError(
                code="sandbox_node_unavailable",
                message="the node tool request failed",
                retryable=True,
                details={"failure_type": type(error).__name__},
            ) from None
        if not terminal:
            raise DomainOperationError(
                code="tool_protocol_error",
                message="the node tool stream ended without a terminal result",
                retryable=True,
            )

    async def write_file(self, path: str, content: bytes) -> None:
        self._require_available()
        if not isinstance(content, bytes):
            raise DomainOperationError(
                code="sandbox_write_invalid",
                message="sandbox writes require bytes",
            )
        request = WriteFileRequest(
            path=path,
            content_base64=base64.b64encode(content).decode("ascii"),
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
        await self._post(
            f"/v1/sandboxes/{self._sandbox_id}/files/write",
            request.model_dump_json(),
        )

    async def create_snapshot(self) -> WorkspaceSnapshot:
        self._require_available()
        response = await self._post(f"/v1/sandboxes/{self._sandbox_id}/snapshots", "{}")
        return WorkspaceSnapshot.model_validate_json(response.content)

    async def final_patch(self) -> StoredObject:
        """Ask the node to persist and checksum the run's immutable final patch."""

        self._require_available()
        response = await self._post(f"/v1/sandboxes/{self._sandbox_id}/final-patch", "{}")
        try:
            return StoredObject.model_validate_json(response.content)
        except ValueError:
            raise _protocol_error() from None

    async def restore_snapshot(self, snapshot: WorkspaceSnapshot) -> None:
        self._require_available()
        await self._post(
            f"/v1/sandboxes/{self._sandbox_id}/restore",
            RestoreSnapshotRequest(snapshot=snapshot).model_dump_json(),
        )

    async def cancel_active(self) -> None:
        self._require_available()
        await self._post(f"/v1/sandboxes/{self._sandbox_id}/cancel", "{}")

    async def destroy(self) -> None:
        if self._destroyed:
            return
        response = await self._client.delete(
            f"/v1/sandboxes/{self._sandbox_id}",
            headers=self._headers(),
        )
        await _raise_for_status(response)
        self._destroyed = True
        if self._owns_client:
            await self._client.aclose()

    async def _post(self, path: str, body: str) -> httpx.Response:
        try:
            response = await self._client.post(
                path,
                content=body.encode("utf-8"),
                headers=self._headers(),
            )
        except DomainOperationError:
            raise
        except httpx.HTTPError:
            raise DomainOperationError(
                code="sandbox_node_unavailable",
                message="the sandbox node request failed",
                retryable=True,
            ) from None
        await _raise_for_status(response)
        return response

    def _headers(self) -> dict[str, str]:
        return {
            "authorization": f"Bearer {self._capability}",
            "content-type": "application/json",
        }

    def _require_available(self) -> None:
        if self._destroyed:
            raise DomainOperationError(
                code="sandbox_destroyed",
                message="the remote sandbox has been destroyed",
            )


async def _raise_for_status(response: httpx.Response) -> None:
    if _HTTP_SUCCESS_MIN <= response.status_code < _HTTP_SUCCESS_MAX:
        return
    await response.aread()
    try:
        failure = NodeAgentErrorResponse.model_validate_json(response.content)
    except ValueError:
        raise DomainOperationError(
            code="sandbox_node_unavailable",
            message="the sandbox node returned an invalid failure response",
            retryable=response.status_code >= _HTTP_SERVER_ERROR_MIN,
        ) from None
    raise DomainOperationError(
        code=failure.error.code,
        message=failure.error.message,
        retryable=failure.error.retryable,
    )


def _protocol_error() -> DomainOperationError:
    return DomainOperationError(
        code="sandbox_protocol_error",
        message="the sandbox node returned invalid content",
    )


def _raise_remote_error(*, code: str, message: str, retryable: bool = False) -> NoReturn:
    raise DomainOperationError(code=code, message=message, retryable=retryable)


__all__ = ["RemoteSandbox"]
