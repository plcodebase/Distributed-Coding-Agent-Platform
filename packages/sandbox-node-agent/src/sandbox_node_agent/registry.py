"""Capability-scoped lifecycle registry for node-owned sandboxes."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import secrets
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from pydantic import SecretStr

from agent_core.domain.errors import DomainOperationError
from sandbox_node_agent.contracts import CreateSandboxRequest, CreateSandboxResponse

if TYPE_CHECKING:
    from agent_core.artifacts import StoredObject
    from agent_core.sandbox import Sandbox
    from agent_core.tools import ToolRegistry

_MAX_NODE_SANDBOXES = 4096
_CAPABILITY_TOKEN_BYTES = 64


class SandboxAuthorizer(Protocol):
    """Validate that a request carries the current durable run lease."""

    async def authorize(self, request: CreateSandboxRequest) -> None: ...


class SandboxFactory(Protocol):
    """Materialize a workspace and create its node-local sandbox."""

    async def create(self, request: CreateSandboxRequest) -> NodeSandboxResources: ...


class FinalPatchExporter(Protocol):
    """Export the immutable final patch before a node sandbox is destroyed."""

    async def final_patch(self) -> StoredObject: ...


class CurrentPatchExporter(Protocol):
    """Read the bounded current patch without persisting a final artifact."""

    async def current_patch(self, *, max_bytes: int) -> bytes: ...


@dataclass(frozen=True, slots=True)
class NodeSandboxResources:
    """Node-local sandbox plus the validated repository tools bound to it."""

    sandbox: Sandbox
    tools: ToolRegistry
    final_patch_exporter: FinalPatchExporter | None = None
    current_patch_exporter: CurrentPatchExporter | None = None


@dataclass(frozen=True, slots=True)
class _SandboxEntry:
    request: CreateSandboxRequest
    resources: NodeSandboxResources
    capability: SecretStr


class NodeSandboxRegistry:
    """Own exact sandbox instances and authorize operations with scoped capabilities."""

    def __init__(
        self,
        *,
        authorizer: SandboxAuthorizer,
        factory: SandboxFactory,
        max_sandboxes: int = 128,
    ) -> None:
        if type(max_sandboxes) is not int or not 1 <= max_sandboxes <= _MAX_NODE_SANDBOXES:
            raise ValueError("max_sandboxes must be an integer in [1, 4096]")
        self._authorizer = authorizer
        self._factory = factory
        self._max_sandboxes = max_sandboxes
        self._entries: dict[uuid.UUID, _SandboxEntry] = {}
        self._by_lease: dict[tuple[uuid.UUID, uuid.UUID, int], uuid.UUID] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def create(self, request: CreateSandboxRequest) -> CreateSandboxResponse:
        await self._authorizer.authorize(request)
        key = (request.run_id, request.run_lease_token, request.run_lease_generation)
        async with self._lock:
            self._require_open()
            existing_id = self._by_lease.get(key)
            if existing_id is not None:
                existing = self._entries[existing_id]
                if existing.request != request:
                    raise DomainOperationError(
                        code="sandbox_creation_conflict",
                        message="the run lease already owns a different sandbox request",
                    )
                return _response(existing_id, existing)
            if len(self._entries) >= self._max_sandboxes:
                raise DomainOperationError(
                    code="sandbox_capacity_exhausted",
                    message="the node has no available sandbox capacity",
                    retryable=True,
                )
            sandbox_id = uuid.uuid4()
            capability = SecretStr(secrets.token_hex(32))
            resources = await self._factory.create(request)
            entry = _SandboxEntry(request=request, resources=resources, capability=capability)
            self._entries[sandbox_id] = entry
            self._by_lease[key] = sandbox_id
            return _response(sandbox_id, entry)

    async def get(self, sandbox_id: uuid.UUID, capability: str) -> NodeSandboxResources:
        async with self._lock:
            self._require_open()
            entry = self._entries.get(sandbox_id)
            if entry is None or not _capability_matches(entry.capability, capability):
                raise _not_found()
            return entry.resources

    async def destroy(self, sandbox_id: uuid.UUID, capability: str) -> None:
        async with self._lock:
            self._require_open()
            entry = self._entries.get(sandbox_id)
            if entry is None or not _capability_matches(entry.capability, capability):
                raise _not_found()
            await entry.resources.sandbox.destroy()
            self._entries.pop(sandbox_id, None)
            key = (
                entry.request.run_id,
                entry.request.run_lease_token,
                entry.request.run_lease_generation,
            )
            self._by_lease.pop(key, None)

    async def aclose(self) -> None:
        async with self._lock:
            if self._closed:
                return
            failures: list[BaseException] = []
            for entry in tuple(self._entries.values()):
                try:
                    await entry.resources.sandbox.destroy()
                except BaseException as error:
                    failures.append(error)
            if failures:
                raise DomainOperationError(
                    code="sandbox_node_cleanup_failed",
                    message="one or more node sandboxes could not be destroyed",
                    retryable=True,
                ) from failures[0]
            self._entries.clear()
            self._by_lease.clear()
            self._closed = True

    async def ready(self) -> bool:
        async with self._lock:
            return not self._closed and len(self._entries) < self._max_sandboxes

    def _require_open(self) -> None:
        if self._closed:
            raise DomainOperationError(
                code="sandbox_node_closed",
                message="the sandbox node agent is closed",
                retryable=True,
            )


def _response(sandbox_id: uuid.UUID, entry: _SandboxEntry) -> CreateSandboxResponse:
    return CreateSandboxResponse(
        sandbox_id=sandbox_id,
        capability_token=entry.capability.get_secret_value(),
        tools=tuple(
            {
                "definition": definition,
                "effect": entry.resources.tools.effect(definition.name),
            }
            for definition in entry.resources.tools.definitions
        ),
    )


def _capability_matches(expected: SecretStr, supplied: str) -> bool:
    if len(supplied) != _CAPABILITY_TOKEN_BYTES:
        return False
    expected_digest = hashlib.sha256(expected.get_secret_value().encode("ascii")).digest()
    try:
        supplied_digest = hashlib.sha256(supplied.encode("ascii")).digest()
    except UnicodeEncodeError:
        return False
    return hmac.compare_digest(expected_digest, supplied_digest)


def _not_found() -> DomainOperationError:
    return DomainOperationError(
        code="sandbox_not_found",
        message="the sandbox was not found",
    )


__all__ = [
    "FinalPatchExporter",
    "NodeSandboxRegistry",
    "NodeSandboxResources",
    "SandboxAuthorizer",
    "SandboxFactory",
]
