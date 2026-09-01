"""Worker-side tool registry whose handlers execute inside the node agent."""

from __future__ import annotations

from typing import TYPE_CHECKING

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

from agent_core.domain.errors import DomainOperationError
from agent_core.tools import PreparedToolExecution, ToolEffect, ToolExecutionContext, ToolRegistry

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agent_core.domain.base import FrozenJsonObject
    from agent_core.gateway import GatewayToolDefinition
    from agent_core.tools import ToolExecutionEvent
    from sandbox_node_agent.client import RemoteSandbox
    from sandbox_node_agent.contracts import NodeToolDefinition


class _RemotePrepared(PreparedToolExecution):
    def __init__(
        self,
        owner: RemoteSandbox,
        definition: NodeToolDefinition,
        arguments: FrozenJsonObject,
    ) -> None:
        self._owner = owner
        self._definition = definition
        self._arguments = arguments

    @property
    def effect(self) -> ToolEffect:
        return self._definition.effect

    def stream(
        self,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        return self._owner._execute_tool(
            self._definition.definition.name,
            self._arguments,
            context,
        )


class _RemoteRegistration:
    def __init__(self, owner: RemoteSandbox, definition: NodeToolDefinition) -> None:
        self._owner = owner
        self._node_definition = definition
        schema = definition.definition.input_schema.to_json_object()
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError:
            raise DomainOperationError(
                code="sandbox_tool_schema_invalid",
                message="the node agent returned an invalid tool schema",
            ) from None
        self._validator = Draft202012Validator(schema)

    @property
    def definition(self) -> GatewayToolDefinition:
        return self._node_definition.definition

    @property
    def effect(self) -> ToolEffect:
        return self._node_definition.effect

    def prepare(self, arguments: FrozenJsonObject) -> PreparedToolExecution:
        try:
            self._validator.validate(arguments.to_json_object())
        except ValidationError as error:
            path = ".".join(str(part) for part in error.absolute_path)
            raise DomainOperationError(
                code="malformed_tool_arguments",
                message="model-generated tool arguments failed schema validation",
                details={
                    "tool_name": self.definition.name,
                    "issues": [{"path": path, "type": str(error.validator)}],
                },
            ) from None
        return _RemotePrepared(self._owner, self._node_definition, arguments)


def create_remote_tool_registry(
    owner: RemoteSandbox,
    definitions: tuple[NodeToolDefinition, ...],
) -> ToolRegistry:
    return ToolRegistry(tuple(_RemoteRegistration(owner, item) for item in definitions))


__all__ = ["create_remote_tool_registry"]
