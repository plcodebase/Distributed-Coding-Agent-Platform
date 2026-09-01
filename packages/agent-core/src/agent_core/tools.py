"""Typed, streaming tool registration for the deterministic agent loop."""

from __future__ import annotations

import uuid  # noqa: TC003 - Pydantic resolves this field type at runtime
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


class ToolEffect(StrEnum):
    """Declared side-effect class used by checkpoint and approval policy."""

    READ_ONLY = "read_only"
    WORKSPACE_MUTATION = "workspace_mutation"
    COMMAND = "command"
    INTERACTION = "interaction"
    CONTROL_MUTATION = "control_mutation"


class ToolExecutionContext(DomainModel):
    """Execution identity, checkpoint metadata, and resource ceilings."""

    run_id: uuid.UUID
    tool_call_id: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    checkpoint_id: uuid.UUID | None = None
    workspace_revision: (
        Annotated[
            str,
            StringConstraints(strip_whitespace=True, min_length=1),
        ]
        | None
    ) = None
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

    @property
    def effect(self) -> ToolEffect:
        """Return the registration's declared side-effect class."""

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
        effect: ToolEffect,
    ) -> None:
        self._handler = handler
        self._arguments = arguments
        self._effect = effect

    @property
    def effect(self) -> ToolEffect:
        return self._effect

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

    @property
    def effect(self) -> ToolEffect:
        """Return the registration's declared side-effect class."""

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
        effect: ToolEffect,
    ) -> None:
        self._arguments_type = arguments_type
        self._handler = handler
        self._effect = effect
        self._definition = GatewayToolDefinition(
            name=name,
            description=description,
            input_schema=arguments_type.model_json_schema(),
        )

    @property
    def definition(self) -> GatewayToolDefinition:
        return self._definition

    @property
    def effect(self) -> ToolEffect:
        return self._effect

    def prepare(self, arguments: FrozenJsonObject) -> PreparedToolExecution:
        validated = self._arguments_type.model_validate(arguments.to_json_object())
        return _PreparedToolExecution(
            handler=self._handler,
            arguments=validated,
            effect=self._effect,
        )


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

    @property
    def registrations(self) -> tuple[ToolRegistration, ...]:
        """Return immutable registrations for explicit composition of capability sets."""

        return tuple(self._tools[definition.name] for definition in self._definitions)

    def effect(self, tool_name: str) -> ToolEffect:
        """Return a registered tool's declared effect without preparing a call."""

        registration = self._tools.get(tool_name)
        if registration is None:
            raise DomainOperationError(
                code="unknown_tool",
                message="the requested tool is not registered",
                details={"tool_name": tool_name},
            )
        return registration.effect

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
    "ToolEffect",
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
