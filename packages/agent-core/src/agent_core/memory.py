"""Bounded, gateway-backed long-term memory extraction contracts."""

from __future__ import annotations

import json
import uuid  # noqa: TC003 - Pydantic resolves identifiers at runtime
from typing import TYPE_CHECKING, Annotated, Never, Protocol, Self

from pydantic import Field, StringConstraints, ValidationError, model_validator

from agent_core.control import MemoryKind  # noqa: TC001 - Pydantic resolves runtime field
from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError
from agent_core.gateway import (
    GatewayFinishReason,
    GatewayInvalidToolCallEvent,
    GatewayMessage,
    GatewayRequest,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCallEvent,
    MessageRole,
    ModelGateway,
)
from platform_telemetry import Redactor

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from agent_core.gateway import GatewayEvent
    from agent_core.loop import IdGenerator

MAX_MEMORY_EXTRACTION_SOURCE_BYTES = 4 * 1024 * 1024
MAX_MEMORY_EXTRACTION_RESPONSE_BYTES = 1024 * 1024
MAX_EXTRACTED_MEMORIES = 100
MAX_MEMORY_CONTENT_CHARACTERS = 65_536


class MemoryExtractionInput(DomainModel):
    """Bounded durable transcript supplied to one extraction call."""

    tenant_id: uuid.UUID
    session_id: uuid.UUID
    run_id: uuid.UUID
    execution_epoch: int = Field(default=1, ge=1)
    transcript: str

    @model_validator(mode="after")
    def validate_transcript(self) -> Self:
        if not self.transcript.strip():
            raise ValueError("memory extraction transcript may not be empty")
        if len(self.transcript.encode("utf-8")) > MAX_MEMORY_EXTRACTION_SOURCE_BYTES:
            raise ValueError("memory extraction transcript exceeds its byte limit")
        return self


class ExtractedMemory(DomainModel):
    """One validated memory candidate before durable provenance is attached."""

    kind: MemoryKind
    content: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=1,
            max_length=MAX_MEMORY_CONTENT_CHARACTERS,
        ),
    ]

    @model_validator(mode="after")
    def validate_content_bytes(self) -> Self:
        if len(self.content.encode("utf-8")) > MAX_MEMORY_CONTENT_CHARACTERS:
            raise ValueError("memory content exceeds its UTF-8 byte limit")
        return self


class MemoryExtractionResult(DomainModel):
    """Validated candidates plus normalized model usage."""

    memories: tuple[ExtractedMemory, ...] = Field(max_length=MAX_EXTRACTED_MEMORIES)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_unique_memories(self) -> Self:
        identities = [(item.kind, item.content) for item in self.memories]
        if len(identities) != len(set(identities)):
            raise ValueError("extracted memories must be unique")
        return self


class MemoryExtractor(Protocol):
    """Provider-neutral asynchronous memory extraction boundary."""

    async def extract(self, request: MemoryExtractionInput) -> MemoryExtractionResult: ...


class _MemoryEnvelope(DomainModel):
    memories: tuple[ExtractedMemory, ...] = Field(max_length=MAX_EXTRACTED_MEMORIES)


class GatewayMemoryExtractor:
    """Extract closed JSON memory candidates through the normalized gateway."""

    def __init__(
        self,
        *,
        gateway: ModelGateway,
        id_generator: IdGenerator,
        route_name: str = "summarization",
        redactor: Redactor | None = None,
    ) -> None:
        self._gateway = gateway
        self._ids = id_generator
        self._route_name = route_name
        self._redactor = redactor or Redactor()

    async def extract(self, request: MemoryExtractionInput) -> MemoryExtractionResult:
        gateway_request = GatewayRequest(
            tenant_id=request.tenant_id,
            session_id=request.session_id,
            run_id=request.run_id,
            execution_epoch=request.execution_epoch,
            turn_number=1,
            model_call_id=self._ids.new_id("memory-model-call"),
            request_id=self._ids.new_id("memory-request"),
            route_name=self._route_name,
            messages=(
                GatewayMessage(
                    role=MessageRole.SYSTEM,
                    content=(
                        "Extract only durable coding-project facts, user preferences, "
                        "decisions, and constraints. Return exactly a JSON object with a "
                        "memories array; every item has kind and content. Return an empty "
                        "array when nothing is durable. Never include credentials."
                    ),
                ),
                GatewayMessage(
                    role=MessageRole.USER,
                    content=self._redactor.redact_text(request.transcript),
                ),
            ),
        )
        chunks: list[str] = []
        byte_count = 0
        terminal: GatewayResponseCompleted | None = None
        stream = self._gateway.stream(gateway_request)
        primary_error: BaseException | None = None
        try:
            while True:
                event = await _next_event(stream)
                if event is None:
                    break
                if terminal is not None:
                    _fail("memory_extraction_invalid", "the extraction stream continued")
                if isinstance(event, GatewayTextDelta):
                    byte_count += len(event.delta.encode("utf-8"))
                    if byte_count > MAX_MEMORY_EXTRACTION_RESPONSE_BYTES:
                        _fail(
                            "memory_extraction_limit",
                            "the extraction response exceeded its byte limit",
                        )
                    chunks.append(event.delta)
                elif isinstance(event, (GatewayToolCallEvent, GatewayInvalidToolCallEvent)):
                    _fail(
                        "memory_extraction_invalid",
                        "the memory extraction route attempted a tool call",
                    )
                elif isinstance(event, GatewayResponseCompleted):
                    terminal = event
        except BaseException as error:
            primary_error = error
            raise
        finally:
            await _close_stream(stream, primary_error=primary_error)
        if terminal is None or terminal.finish_reason is not GatewayFinishReason.STOP:
            _fail(
                "memory_extraction_invalid",
                "the memory extraction route returned an unsupported terminal outcome",
            )
        envelope = _parse_envelope(self._redactor.redact_text("".join(chunks)))
        return MemoryExtractionResult(
            memories=envelope.memories,
            input_tokens=terminal.input_tokens,
            output_tokens=terminal.output_tokens,
        )


def _parse_envelope(value: str) -> _MemoryEnvelope:
    try:
        decoded = json.loads(value, parse_constant=_reject_json_constant)
        return _MemoryEnvelope.model_validate(decoded)
    except (TypeError, ValueError, ValidationError) as error:
        raise DomainOperationError(
            code="memory_extraction_invalid",
            message="the memory extraction route returned invalid structured output",
        ) from error


def _reject_json_constant(value: str) -> Never:
    raise ValueError(f"invalid JSON constant: {value}")


async def _next_event(stream: AsyncIterator[GatewayEvent]) -> GatewayEvent | None:
    try:
        return await anext(stream)
    except StopAsyncIteration:
        return None
    except Exception as error:
        raise DomainOperationError(
            code="memory_extraction_failed",
            message="memory extraction could not be completed",
            retryable=True,
        ) from error


async def _close_stream(
    stream: AsyncIterator[GatewayEvent],
    *,
    primary_error: BaseException | None,
) -> None:
    close = getattr(stream, "aclose", None)
    if close is None:
        return
    try:
        await close()
    except Exception as error:
        if primary_error is None:
            raise DomainOperationError(
                code="memory_extraction_failed",
                message="the memory extraction stream could not be closed",
                retryable=True,
            ) from error


def _fail(code: str, message: str) -> Never:
    raise DomainOperationError(code=code, message=message)


__all__ = [
    "MAX_EXTRACTED_MEMORIES",
    "MAX_MEMORY_EXTRACTION_RESPONSE_BYTES",
    "MAX_MEMORY_EXTRACTION_SOURCE_BYTES",
    "ExtractedMemory",
    "GatewayMemoryExtractor",
    "MemoryExtractionInput",
    "MemoryExtractionResult",
    "MemoryExtractor",
]
