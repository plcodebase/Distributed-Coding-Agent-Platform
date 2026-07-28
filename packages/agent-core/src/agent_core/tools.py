"""Typed, streaming tool registration for the deterministic agent loop."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from enum import StrEnum
from typing import Annotated, Literal, Protocol

from pydantic import Field, JsonValue, StringConstraints, ValidationError

from agent_core.domain.base import DomainModel, FrozenJsonObject, JsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.gateway import GatewayToolDefinition

type ToolOutputText = Annotated[str, StringConstraints(min_length=1)]


class ToolArguments(DomainModel):
    """Base class for closed, immutable, tool-specific argument schemas."""


class ToolExecutionContext(DomainModel):
    """Resource ceilings a handler must observe before producing data."""

    max_output_bytes: int = Field(ge=1)
    max_result_bytes: int = Field(ge=1)


class ToolExecutionEventKind(StrEnum):
    """Discriminator values for one tool execution stream."""

    OUTPUT = "output"
    COMPLETED = "completed"


class ToolOutputChannel(StrEnum):
    """Ordered output channels produced by a tool."""

    STDOUT = "stdout"
    STDERR = "stderr"


class ToolOutputChunk(DomainModel):
    """One ordered, non-empty output chunk from a running tool."""

    kind: Literal[ToolExecutionEventKind.OUTPUT] = ToolExecutionEventKind.OUTPUT
    channel: ToolOutputChannel
    chunk: ToolOutputText


class ToolExecutionCompleted(DomainModel):
    """The single terminal value of a successful tool execution stream."""

    kind: Literal[ToolExecutionEventKind.COMPLETED] = ToolExecutionEventKind.COMPLETED
    result: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))


type ToolExecutionEvent = ToolOutputChunk | ToolExecutionCompleted


class ToolHandler[ArgumentsT: ToolArguments](Protocol):
    """A handler receiving validated arguments and explicit resource ceilings."""

    def __call__(
        self,
        arguments: ArgumentsT,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        """Stream ordered output followed by exactly one terminal result."""


class PreparedToolExecution(Protocol):
    """A validated, single-use tool operation ready for bounded execution."""

    def stream(
        self,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        """Create the execution stream for the already validated arguments."""


class _PreparedToolExecution[ArgumentsT: ToolArguments]:
    def __init__(
        self,
        *,
        handler: ToolHandler[ArgumentsT],
        arguments: ArgumentsT,
    ) -> None:
        self._handler = handler
        self._arguments = arguments

    def stream(
        self,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        stream = self._handler(self._arguments, context)
        if not isinstance(stream, AsyncGenerator):
            raise TypeError("tool handler must return an async generator")
        return stream


class ToolRegistration(Protocol):
    """Type-erased registry entry whose generic validation remains internal."""

    @property
    def definition(self) -> GatewayToolDefinition:
        """Return the provider-neutral schema advertised to the model."""

    def prepare(self, arguments: FrozenJsonObject) -> PreparedToolExecution:
        """Validate raw model arguments without executing the tool."""


class RegisteredTool[ArgumentsT: ToolArguments]:
    """Bind one typed Pydantic argument model to one streaming tool handler."""

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
    "ToolExecutionCompleted",
    "ToolExecutionContext",
    "ToolExecutionEvent",
    "ToolExecutionEventKind",
    "ToolHandler",
    "ToolOutputChannel",
    "ToolOutputChunk",
    "ToolRegistration",
    "ToolRegistry",
]
