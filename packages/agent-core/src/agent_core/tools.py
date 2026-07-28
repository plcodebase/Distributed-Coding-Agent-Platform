"""Typed tool registration and argument validation for the deterministic loop."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Protocol

from pydantic import Field, JsonValue, StringConstraints, ValidationError

from agent_core.domain.base import DomainModel, FrozenJsonObject, JsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.gateway import GatewayToolDefinition

if TYPE_CHECKING:
    from collections.abc import Sequence

type OutputChunk = Annotated[str, StringConstraints(min_length=1)]


class ToolArguments(DomainModel):
    """Base class for closed, immutable, tool-specific argument schemas."""


class ToolExecutionResult(DomainModel):
    """A completed tool result plus bounded-output candidates for event rendering."""

    result: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    stdout: tuple[OutputChunk, ...] = ()
    stderr: tuple[OutputChunk, ...] = ()


class ToolHandler[ArgumentsT: ToolArguments](Protocol):
    """A handler that can only receive arguments validated against its declared type."""

    async def __call__(self, arguments: ArgumentsT) -> ToolExecutionResult:
        """Execute one validated logical tool call."""


class PreparedToolExecution(Protocol):
    """A validated, single-use tool operation ready for execution."""

    async def execute(self) -> ToolExecutionResult:
        """Invoke the registered handler with its already validated arguments."""


class _PreparedToolExecution[ArgumentsT: ToolArguments]:
    def __init__(
        self,
        *,
        handler: ToolHandler[ArgumentsT],
        arguments: ArgumentsT,
    ) -> None:
        self._handler = handler
        self._arguments = arguments

    async def execute(self) -> ToolExecutionResult:
        result = await self._handler(self._arguments)
        if not isinstance(result, ToolExecutionResult):
            raise TypeError("tool handler must return ToolExecutionResult")
        return result


class ToolRegistration(Protocol):
    """Type-erased registry entry whose generic validation remains internal."""

    @property
    def definition(self) -> GatewayToolDefinition:
        """Return the provider-neutral schema advertised to the model."""

    def prepare(self, arguments: FrozenJsonObject) -> PreparedToolExecution:
        """Validate raw model arguments without executing the tool."""


class RegisteredTool[ArgumentsT: ToolArguments]:
    """Bind one typed Pydantic argument model to one async tool handler."""

    def __init__(
        self,
        *,
        name: str,
        description: str,
        arguments_type: type[ArgumentsT],
        handler: ToolHandler[ArgumentsT],
    ) -> None:
        self._arguments_type = arguments_type
        self._handler = handler
        self._definition = GatewayToolDefinition(
            name=name,
            description=description,
            input_schema=arguments_type.model_json_schema(),
        )

    @property
    def definition(self) -> GatewayToolDefinition:
        return self._definition

    def prepare(self, arguments: FrozenJsonObject) -> PreparedToolExecution:
        validated = self._arguments_type.model_validate(arguments.to_json_object())
        return _PreparedToolExecution(handler=self._handler, arguments=validated)


class ToolRegistry:
    """Immutable lookup table that validates every model-generated tool call."""

    def __init__(self, registrations: Sequence[ToolRegistration] = ()) -> None:
        tools = {registration.definition.name: registration for registration in registrations}
        if len(tools) != len(registrations):
            raise ValueError("tool registration names must be unique")
        self._tools = tools
        self._definitions = tuple(registration.definition for registration in registrations)

    @property
    def definitions(self) -> tuple[GatewayToolDefinition, ...]:
        return self._definitions

    def prepare(
        self,
        tool_name: str,
        arguments: FrozenJsonObject,
    ) -> PreparedToolExecution:
        registration = self._tools.get(tool_name)
        if registration is None:
            raise DomainOperationError(
                code="unknown_tool",
                message="the requested tool is not registered",
                details={"tool_name": tool_name},
            )
        try:
            return registration.prepare(arguments)
        except ValidationError as error:
            issues: list[JsonValue] = []
            for issue in error.errors(include_input=False, include_url=False):
                issue_value: JsonObject = {
                    "path": ".".join(str(part) for part in issue["loc"]),
                    "type": issue["type"],
                }
                issues.append(issue_value)
            details: JsonObject = {
                "tool_name": tool_name,
                "issues": issues,
            }
            raise DomainOperationError(
                code="malformed_tool_arguments",
                message="model-generated tool arguments failed schema validation",
                details=details,
            ) from error


__all__ = [
    "PreparedToolExecution",
    "RegisteredTool",
    "ToolArguments",
    "ToolExecutionResult",
    "ToolHandler",
    "ToolRegistration",
    "ToolRegistry",
]
