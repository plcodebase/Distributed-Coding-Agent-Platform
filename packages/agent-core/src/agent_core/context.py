"""Composable, bounded context assembly and gateway-backed compression."""

from __future__ import annotations

import json
import uuid  # noqa: TC003 - Pydantic resolves UUID fields at runtime
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Never, Protocol, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import DomainModel, FrozenJsonObject, JsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.domain.models import IdentifierString  # noqa: TC001 - runtime Pydantic field
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
    from collections.abc import AsyncIterator, Iterable, Sequence

    from agent_core.gateway import GatewayEvent
    from agent_core.loop import IdGenerator

MAX_CONTEXT_FRAGMENTS = 8192
MAX_CONTEXT_MESSAGES = 4096
MAX_CONTEXT_INPUT_BYTES = 16 * 1024 * 1024
MAX_CONTEXT_ITEM_BYTES = 1024 * 1024
MAX_COMPRESSION_INPUT_BYTES = 4 * 1024 * 1024
MAX_CONTEXT_SUMMARY_BYTES = 256 * 1024
MAX_RECENT_CONTEXT_MESSAGES = 512
CONTEXT_COMPACTION_ROUTE = "summarization"
_TASK_PLAN_CHUNK_BYTES = MAX_CONTEXT_ITEM_BYTES // 16
_TERMINAL_TASK_STATUSES = frozenset({"completed", "cancelled"})


class ContextSource(StrEnum):
    """Every source required by the design's composable context pipeline."""

    SYSTEM_INSTRUCTIONS = "system_instructions"
    PROJECT_INSTRUCTIONS = "project_instructions"
    CONVERSATION_HISTORY = "conversation_history"
    REFERENCED_FILES = "referenced_files"
    ACTIVE_TASK_PLAN = "active_task_plan"
    RECENT_TOOL_RESULTS = "recent_tool_results"
    LONG_TERM_MEMORY = "long_term_memory"
    CURRENT_GIT_DIFF = "current_git_diff"
    COMPACTED_SUMMARY = "compacted_summary"


class ReferencedContextFile(DomainModel):
    """Bounded file context already selected by a contained workspace adapter."""

    path: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    content: str
    active: bool = False

    @model_validator(mode="after")
    def validate_bytes(self) -> Self:
        _require_utf8_limit("referenced file", self.content, MAX_CONTEXT_ITEM_BYTES)
        return self


class ContextToolResult(DomainModel):
    """Sanitized recent tool feedback supplied to the context pipeline."""

    tool_call_id: IdentifierString
    tool_name: IdentifierString
    content: str
    is_error: bool = False

    @model_validator(mode="after")
    def validate_bytes(self) -> Self:
        _require_utf8_limit("tool result", self.content, MAX_CONTEXT_ITEM_BYTES)
        return self


class ContextMemorySnippet(DomainModel):
    """Tenant-filtered durable memory ready for context contribution."""

    memory_id: uuid.UUID
    content: str

    @model_validator(mode="after")
    def validate_bytes(self) -> Self:
        _require_utf8_limit("memory", self.content, MAX_CONTEXT_ITEM_BYTES)
        return self


class ContextBuildRequest(DomainModel):
    """Complete bounded input shared by independently testable contributors."""

    tenant_id: uuid.UUID
    session_id: uuid.UUID
    run_id: uuid.UUID
    route_name: IdentifierString
    system_instructions: str = ""
    project_instructions: str = ""
    conversation: tuple[GatewayMessage, ...] = Field(default=(), max_length=4096)
    referenced_files: tuple[ReferencedContextFile, ...] = Field(default=(), max_length=256)
    task_plan: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    recent_tool_results: tuple[ContextToolResult, ...] = Field(default=(), max_length=256)
    memories: tuple[ContextMemorySnippet, ...] = Field(default=(), max_length=512)
    current_git_diff: str = ""
    previous_summary: str | None = None
    force_compaction: bool = False

    @model_validator(mode="after")
    def validate_size(self) -> Self:
        if not self.conversation and not any(
            (
                self.system_instructions,
                self.project_instructions,
                self.referenced_files,
                self.task_plan,
                self.recent_tool_results,
                self.memories,
                self.current_git_diff,
                self.previous_summary,
            )
        ):
            raise ValueError("context request must contain at least one source")
        for name, value in (
            ("system instructions", self.system_instructions),
            ("project instructions", self.project_instructions),
            ("Git diff", self.current_git_diff),
            ("previous summary", self.previous_summary or ""),
        ):
            _require_utf8_limit(name, value, MAX_CONTEXT_ITEM_BYTES)
        if len(self.model_dump_json().encode("utf-8")) > MAX_CONTEXT_INPUT_BYTES:
            raise ValueError("serialized context input exceeds its byte limit")
        return self


class ContextFragment(DomainModel):
    """One typed contributor output with explicit preservation semantics."""

    id: IdentifierString
    source: ContextSource
    message: GatewayMessage
    critical: bool = False
    priority: int = Field(default=0, ge=-100, le=100)
    ordinal: int = Field(ge=0, le=MAX_CONTEXT_FRAGMENTS)

    @model_validator(mode="after")
    def validate_size(self) -> Self:
        if len(self.message.model_dump_json().encode("utf-8")) > MAX_CONTEXT_ITEM_BYTES:
            raise ValueError("serialized context fragment exceeds its byte limit")
        return self


class ContextContributor(Protocol):
    """Infrastructure-free asynchronous source of bounded context fragments."""

    async def contribute(self, request: ContextBuildRequest) -> tuple[ContextFragment, ...]:
        """Return deterministic fragments for one context build."""


class ContextRouteBudget(DomainModel):
    """Conservative input/output reservation for one logical model route."""

    route_name: IdentifierString
    max_context_tokens: int = Field(ge=256, le=10_000_000)
    reserved_output_tokens: int = Field(default=4096, ge=1, le=1_000_000)

    @model_validator(mode="after")
    def validate_available(self) -> Self:
        if self.reserved_output_tokens >= self.max_context_tokens:
            raise ValueError("reserved output tokens must be below the context limit")
        return self

    @property
    def available_input_tokens(self) -> int:
        return self.max_context_tokens - self.reserved_output_tokens


class ContextBudgetRegistry:
    """Immutable route-to-budget lookup configured by composition."""

    def __init__(self, budgets: Sequence[ContextRouteBudget]) -> None:
        if not budgets:
            raise ValueError("at least one context route budget is required")
        values = {budget.route_name: budget for budget in budgets}
        if len(values) != len(budgets):
            raise ValueError("context route budgets must have unique route names")
        self._budgets = values

    def get(self, route_name: str) -> ContextRouteBudget:
        budget = self._budgets.get(route_name)
        if budget is None:
            raise DomainOperationError(
                code="context_route_unconfigured",
                message="the model route has no configured context budget",
                details={"route_name": route_name},
            )
        return budget


class TokenEstimator(Protocol):
    """Conservative model-independent token estimate boundary."""

    def estimate_messages(self, messages: Sequence[GatewayMessage]) -> int:
        """Return a nonnegative conservative estimate for serialized messages."""


class Utf8TokenEstimator:
    """Use one token per serialized UTF-8 byte as a conservative upper bound."""

    def estimate_messages(self, messages: Sequence[GatewayMessage]) -> int:
        payload = json.dumps(
            [message.model_dump(mode="json") for message in messages],
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return len(payload.encode("utf-8"))


class ContextCompressionRequest(DomainModel):
    """Bounded source material passed to the summarization route."""

    tenant_id: uuid.UUID
    session_id: uuid.UUID
    run_id: uuid.UUID
    source_text: str
    max_summary_bytes: int = Field(ge=1, le=MAX_CONTEXT_SUMMARY_BYTES)

    @model_validator(mode="after")
    def validate_source(self) -> Self:
        _require_utf8_limit("compression source", self.source_text, MAX_COMPRESSION_INPUT_BYTES)
        return self


class ContextCompressionResult(DomainModel):
    """Validated gateway summary plus normalized usage."""

    summary: Annotated[str, StringConstraints(min_length=1)]
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_summary(self) -> Self:
        _require_utf8_limit("context summary", self.summary, MAX_CONTEXT_SUMMARY_BYTES)
        return self


class ContextCompressor(Protocol):
    """Model-backed compression boundary injected into the context pipeline."""

    async def compress(self, request: ContextCompressionRequest) -> ContextCompressionResult:
        """Return one bounded summary without mutating durable source history."""


class ContextBuildResult(DomainModel):
    """Final valid gateway context and auditable retention decision."""

    messages: tuple[GatewayMessage, ...] = Field(min_length=1, max_length=MAX_CONTEXT_MESSAGES)
    estimated_tokens: int = Field(ge=1)
    budget_tokens: int = Field(ge=1)
    compressed: bool = False
    summary: str | None = None
    compression_input_tokens: int | None = Field(default=None, ge=0)
    compression_output_tokens: int | None = Field(default=None, ge=0)
    retained_fragment_ids: tuple[IdentifierString, ...] = Field(
        default=(),
        max_length=MAX_CONTEXT_FRAGMENTS,
    )
    dropped_fragment_ids: tuple[IdentifierString, ...] = Field(
        default=(),
        max_length=MAX_CONTEXT_FRAGMENTS,
    )

    @model_validator(mode="after")
    def validate_budget(self) -> Self:
        if self.estimated_tokens > self.budget_tokens:
            raise ValueError("context build exceeds its declared route budget")
        if self.summary is not None:
            _require_utf8_limit("context summary", self.summary, MAX_CONTEXT_SUMMARY_BYTES)
        usage = (self.compression_input_tokens, self.compression_output_tokens)
        if self.compressed and self.summary is not None:
            if any(value is None for value in usage):
                raise ValueError("compressed context with a summary requires compression usage")
        elif any(value is not None for value in usage):
            raise ValueError("compression usage requires a compressed context summary")
        return self


class SystemInstructionsContributor:
    async def contribute(self, request: ContextBuildRequest) -> tuple[ContextFragment, ...]:
        return _system_fragment(
            "system-instructions",
            ContextSource.SYSTEM_INSTRUCTIONS,
            request.system_instructions,
            critical=True,
            priority=100,
            ordinal=0,
        )


class ProjectInstructionsContributor:
    async def contribute(self, request: ContextBuildRequest) -> tuple[ContextFragment, ...]:
        return _system_fragment(
            "project-instructions",
            ContextSource.PROJECT_INSTRUCTIONS,
            request.project_instructions,
            critical=True,
            priority=90,
            ordinal=0,
        )


class ConversationHistoryContributor:
    def __init__(self, *, recent_messages: int = 12) -> None:
        if (
            type(recent_messages) is not int
            or not 1 <= recent_messages <= MAX_RECENT_CONTEXT_MESSAGES
        ):
            raise ValueError("recent_messages must be in [1, 512]")
        self._recent_messages = recent_messages

    async def contribute(self, request: ContextBuildRequest) -> tuple[ContextFragment, ...]:
        recent_start = _recent_conversation_start(
            request.conversation,
            self._recent_messages,
        )
        return tuple(
            ContextFragment(
                id=f"conversation-{index + 1}",
                source=ContextSource.CONVERSATION_HISTORY,
                message=message,
                critical=(index >= recent_start or message.role is MessageRole.SYSTEM),
                priority=80 if index >= recent_start else 10,
                ordinal=index,
            )
            for index, message in enumerate(request.conversation)
        )


class ReferencedFilesContributor:
    async def contribute(self, request: ContextBuildRequest) -> tuple[ContextFragment, ...]:
        return tuple(
            ContextFragment(
                id=f"referenced-file-{index + 1}",
                source=ContextSource.REFERENCED_FILES,
                message=GatewayMessage(
                    role=MessageRole.SYSTEM,
                    content=f"Referenced file `{item.path}`:\n{item.content}",
                ),
                critical=item.active,
                priority=85 if item.active else 30,
                ordinal=index,
            )
            for index, item in enumerate(request.referenced_files)
        )


class ActiveTaskPlanContributor:
    async def contribute(self, request: ContextBuildRequest) -> tuple[ContextFragment, ...]:
        plan = request.task_plan.to_json_object()
        if not plan:
            return ()
        return _task_plan_fragments(plan)


class RecentToolResultsContributor:
    async def contribute(self, request: ContextBuildRequest) -> tuple[ContextFragment, ...]:
        return tuple(
            ContextFragment(
                id=f"tool-result-{item.tool_call_id}",
                source=ContextSource.RECENT_TOOL_RESULTS,
                message=GatewayMessage(
                    role=MessageRole.SYSTEM,
                    content=f"Recent {item.tool_name} result:\n{item.content}",
                ),
                critical=item.is_error,
                priority=90 if item.is_error else 40,
                ordinal=index,
            )
            for index, item in enumerate(request.recent_tool_results)
        )


class LongTermMemoryContributor:
    async def contribute(self, request: ContextBuildRequest) -> tuple[ContextFragment, ...]:
        return tuple(
            ContextFragment(
                id=f"memory-{item.memory_id}",
                source=ContextSource.LONG_TERM_MEMORY,
                message=GatewayMessage(
                    role=MessageRole.SYSTEM,
                    content=f"Relevant durable memory:\n{item.content}",
                ),
                priority=20,
                ordinal=index,
            )
            for index, item in enumerate(request.memories)
        )


class CurrentGitDiffContributor:
    async def contribute(self, request: ContextBuildRequest) -> tuple[ContextFragment, ...]:
        return _system_fragment(
            "current-git-diff",
            ContextSource.CURRENT_GIT_DIFF,
            request.current_git_diff,
            critical=False,
            priority=50,
            ordinal=0,
        )


class ContextPipeline:
    """Assemble context deterministically and compact only noncritical history."""

    def __init__(
        self,
        *,
        budgets: ContextBudgetRegistry,
        compressor: ContextCompressor,
        contributors: Sequence[ContextContributor] | None = None,
        estimator: TokenEstimator | None = None,
        max_compression_input_bytes: int = MAX_COMPRESSION_INPUT_BYTES,
        max_summary_bytes: int = MAX_CONTEXT_SUMMARY_BYTES,
    ) -> None:
        if not 1 <= max_compression_input_bytes <= MAX_COMPRESSION_INPUT_BYTES:
            raise ValueError("max_compression_input_bytes is outside its supported range")
        if not 1 <= max_summary_bytes <= MAX_CONTEXT_SUMMARY_BYTES:
            raise ValueError("max_summary_bytes is outside its supported range")
        self._budgets = budgets
        self._compressor = compressor
        self._contributors = tuple(
            contributors
            or (
                SystemInstructionsContributor(),
                ProjectInstructionsContributor(),
                ConversationHistoryContributor(),
                ReferencedFilesContributor(),
                ActiveTaskPlanContributor(),
                RecentToolResultsContributor(),
                LongTermMemoryContributor(),
                CurrentGitDiffContributor(),
            )
        )
        if not self._contributors:
            raise ValueError("context pipeline requires at least one contributor")
        self._estimator = estimator or Utf8TokenEstimator()
        self._max_compression_input_bytes = max_compression_input_bytes
        self._max_summary_bytes = max_summary_bytes

    async def build(self, request: ContextBuildRequest) -> ContextBuildResult:
        budget = self._budgets.get(request.route_name)
        fragments: list[ContextFragment] = []
        for contributor in self._contributors:
            values = await contributor.contribute(request)
            fragments.extend(values)
            if len(fragments) > MAX_CONTEXT_FRAGMENTS:
                raise DomainOperationError(
                    code="context_fragment_limit",
                    message="context contributors exceeded the fragment limit",
                    details={"limit": MAX_CONTEXT_FRAGMENTS},
                )
        identifiers = [fragment.id for fragment in fragments]
        if len(identifiers) != len(set(identifiers)):
            raise DomainOperationError(
                code="context_fragment_conflict",
                message="context contributors returned duplicate fragment IDs",
            )
        if request.previous_summary:
            if "previous-compacted-summary" in identifiers:
                raise DomainOperationError(
                    code="context_fragment_conflict",
                    message="a contributor used a reserved context fragment ID",
                )
            fragments.insert(
                0,
                ContextFragment(
                    id="previous-compacted-summary",
                    source=ContextSource.COMPACTED_SUMMARY,
                    message=GatewayMessage(
                        role=MessageRole.SYSTEM,
                        content=f"Previous compacted context:\n{request.previous_summary}",
                    ),
                    priority=60,
                    ordinal=0,
                ),
            )

        all_messages = _assemble_messages(fragments)
        all_tokens = self._estimate(all_messages)
        available = budget.available_input_tokens
        if all_tokens <= available and not request.force_compaction:
            return ContextBuildResult(
                messages=all_messages,
                estimated_tokens=all_tokens,
                budget_tokens=available,
                retained_fragment_ids=tuple(fragment.id for fragment in fragments),
            )

        critical = [fragment for fragment in fragments if fragment.critical]
        critical_tokens = self._estimate(_assemble_messages(critical)) if critical else 0
        if critical_tokens > available:
            raise DomainOperationError(
                code="context_critical_limit",
                message="critical active context exceeds the configured route budget",
                details={
                    "critical_tokens": critical_tokens,
                    "limit_tokens": available,
                    "route_name": request.route_name,
                },
            )

        noncritical = [fragment for fragment in fragments if not fragment.critical]
        summary: str | None = None
        compression: ContextCompressionResult | None = None
        if noncritical:
            source_text = _bounded_compression_source(
                noncritical,
                limit=self._max_compression_input_bytes,
            )
            compression = await self._compressor.compress(
                ContextCompressionRequest(
                    tenant_id=request.tenant_id,
                    session_id=request.session_id,
                    run_id=request.run_id,
                    source_text=source_text,
                    max_summary_bytes=self._max_summary_bytes,
                )
            )
            summary = compression.summary

        retained = list(critical)
        if summary:
            summary = self._fit_summary(summary, retained, available=available)
            if summary:
                retained.insert(
                    0,
                    ContextFragment(
                        id="new-compacted-summary",
                        source=ContextSource.COMPACTED_SUMMARY,
                        message=GatewayMessage(
                            role=MessageRole.SYSTEM,
                            content=f"Compacted prior context:\n{summary}",
                        ),
                        priority=60,
                        ordinal=0,
                    ),
                )
        messages = _assemble_messages(retained)
        estimated = self._estimate(messages)
        retained_ids = {fragment.id for fragment in critical}
        return ContextBuildResult(
            messages=messages,
            estimated_tokens=estimated,
            budget_tokens=available,
            compressed=bool(noncritical),
            summary=summary,
            compression_input_tokens=(
                compression.input_tokens if summary is not None and compression else None
            ),
            compression_output_tokens=(
                compression.output_tokens if summary is not None and compression else None
            ),
            retained_fragment_ids=tuple(fragment.id for fragment in retained),
            dropped_fragment_ids=tuple(
                fragment.id for fragment in fragments if fragment.id not in retained_ids
            ),
        )

    def _fit_summary(
        self,
        summary: str,
        critical: list[ContextFragment],
        *,
        available: int,
    ) -> str | None:
        low = 0
        high = len(summary)
        best = ""
        while low <= high:
            midpoint = (low + high) // 2
            candidate = _utf8_prefix(summary, midpoint)
            fragment = ContextFragment(
                id="summary-fit",
                source=ContextSource.COMPACTED_SUMMARY,
                message=GatewayMessage(
                    role=MessageRole.SYSTEM,
                    content=f"Compacted prior context:\n{candidate}",
                ),
                ordinal=0,
            )
            if self._estimate(_assemble_messages([fragment, *critical])) <= available:
                best = candidate
                low = midpoint + 1
            else:
                high = midpoint - 1
        return best or None

    def _estimate(self, messages: tuple[GatewayMessage, ...]) -> int:
        estimate = self._estimator.estimate_messages(messages)
        if type(estimate) is not int or estimate < 0:
            raise DomainOperationError(
                code="context_estimator_invalid",
                message="the token estimator returned an invalid value",
            )
        return max(1, estimate)


class GatewayContextCompressor:
    """Compress context only through the existing normalized model gateway."""

    def __init__(
        self,
        *,
        gateway: ModelGateway,
        id_generator: IdGenerator,
        route_name: str = CONTEXT_COMPACTION_ROUTE,
        redactor: Redactor | None = None,
    ) -> None:
        self._gateway = gateway
        self._ids = id_generator
        self._route_name = route_name
        self._redactor = redactor or Redactor()

    async def compress(self, request: ContextCompressionRequest) -> ContextCompressionResult:
        source = self._redactor.redact_text(request.source_text)
        gateway_request = GatewayRequest(
            tenant_id=request.tenant_id,
            session_id=request.session_id,
            run_id=request.run_id,
            turn_number=1,
            model_call_id=self._ids.new_id("context-model-call"),
            request_id=self._ids.new_id("context-request"),
            route_name=self._route_name,
            messages=(
                GatewayMessage(
                    role=MessageRole.SYSTEM,
                    content=(
                        "Compress coding-agent context. Preserve active files, unresolved "
                        "tasks, decisions, constraints, and recent errors. Return plain text."
                    ),
                ),
                GatewayMessage(role=MessageRole.USER, content=source),
            ),
        )
        chunks: list[str] = []
        size = 0
        terminal: GatewayResponseCompleted | None = None
        stream = self._gateway.stream(gateway_request)
        primary_error: BaseException | None = None
        try:
            while True:
                event = await _next_context_gateway_event(stream)
                if event is None:
                    break
                if terminal is not None:
                    _raise_context_compression_error(
                        code="context_compression_invalid",
                        message="the summarization stream emitted data after completion",
                    )
                if isinstance(event, GatewayTextDelta):
                    size += len(event.delta.encode("utf-8"))
                    if size > request.max_summary_bytes:
                        _raise_context_compression_error(
                            code="context_summary_limit",
                            message="the compressed context exceeded its byte limit",
                            details={"limit_bytes": request.max_summary_bytes},
                        )
                    chunks.append(event.delta)
                elif isinstance(
                    event,
                    (GatewayToolCallEvent, GatewayInvalidToolCallEvent),
                ):
                    _raise_context_compression_error(
                        code="context_compression_invalid",
                        message="the summarization route attempted a tool call",
                    )
                elif isinstance(event, GatewayResponseCompleted):
                    terminal = event
        except BaseException as error:
            primary_error = error
            raise
        finally:
            await _close_context_gateway_stream(stream, primary_error=primary_error)
        if terminal is None or terminal.finish_reason not in {
            GatewayFinishReason.STOP,
            GatewayFinishReason.LENGTH,
        }:
            raise DomainOperationError(
                code="context_compression_invalid",
                message="the summarization route returned an unsupported terminal outcome",
            )
        summary = self._redactor.redact_text("".join(chunks)).strip()
        if not summary:
            raise DomainOperationError(
                code="context_compression_invalid",
                message="the summarization route returned an empty summary",
            )
        return ContextCompressionResult(
            summary=summary,
            input_tokens=terminal.input_tokens,
            output_tokens=terminal.output_tokens,
        )


def _system_fragment(
    identifier: str,
    source: ContextSource,
    content: str,
    *,
    critical: bool,
    priority: int,
    ordinal: int,
) -> tuple[ContextFragment, ...]:
    if not content:
        return ()
    return (
        ContextFragment(
            id=identifier,
            source=source,
            message=GatewayMessage(role=MessageRole.SYSTEM, content=content),
            critical=critical,
            priority=priority,
            ordinal=ordinal,
        ),
    )


def _recent_conversation_start(
    messages: tuple[GatewayMessage, ...],
    recent_messages: int,
) -> int:
    start = max(0, len(messages) - recent_messages)
    while start > 0 and messages[start].role is MessageRole.TOOL:
        tool_call_id = messages[start].tool_call_id
        start -= 1
        while start > 0 and not (
            messages[start].role is MessageRole.ASSISTANT
            and any(call.id == tool_call_id for call in messages[start].tool_calls)
        ):
            start -= 1
    return start


def _assemble_messages(fragments: Iterable[ContextFragment]) -> tuple[GatewayMessage, ...]:
    ordered = list(fragments)
    systems = [
        fragment.message.content
        for fragment in ordered
        if fragment.message.role is MessageRole.SYSTEM
    ]
    conversation = [
        fragment.message for fragment in ordered if fragment.message.role is not MessageRole.SYSTEM
    ]
    messages: list[GatewayMessage] = []
    if systems:
        messages.append(
            GatewayMessage(
                role=MessageRole.SYSTEM,
                content="\n\n".join(systems),
            )
        )
    messages.extend(conversation)
    if not messages:
        raise DomainOperationError(
            code="context_empty",
            message="context contributors produced no usable messages",
        )
    if len(messages) > MAX_CONTEXT_MESSAGES:
        raise DomainOperationError(
            code="context_message_limit",
            message="assembled context exceeded the message limit",
            details={"limit": MAX_CONTEXT_MESSAGES},
        )
    return tuple(messages)


def _bounded_compression_source(
    fragments: list[ContextFragment],
    *,
    limit: int,
) -> str:
    selected: list[str] = []
    size = 0
    ordered = sorted(
        fragments,
        key=lambda fragment: (fragment.priority, fragment.ordinal),
        reverse=True,
    )
    for fragment in ordered:
        rendered = f"[{fragment.source.value}:{fragment.id}]\n" + _canonical_json(
            fragment.message.model_dump(mode="json")
        )
        encoded_size = len(rendered.encode("utf-8")) + (2 if selected else 0)
        if size + encoded_size > limit:
            continue
        selected.append(rendered)
        size += encoded_size
    if not selected:
        raise DomainOperationError(
            code="context_compression_input_limit",
            message="no noncritical context fragment fits the compression input limit",
            details={"limit_bytes": limit},
        )
    return "\n\n".join(selected)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _task_plan_fragments(plan: JsonObject) -> tuple[ContextFragment, ...]:
    collection_name = next(
        (name for name in ("tasks", "steps") if isinstance(plan.get(name), list)),
        None,
    )
    if collection_name is None:
        return _chunked_task_fragment(
            _canonical_json(plan),
            critical=True,
            priority=100,
            ordinal_start=0,
            identifier_prefix="task-plan-legacy",
            label="Active task plan (unrecognized shape; preserve as unresolved)",
        )

    fragments: list[ContextFragment] = []
    metadata = {key: value for key, value in plan.items() if key != collection_name}
    if metadata:
        fragments.extend(
            _chunked_task_fragment(
                _canonical_json(metadata),
                critical=False,
                priority=25,
                ordinal_start=0,
                identifier_prefix="task-plan-metadata",
                label="Task plan metadata",
            )
        )
    items = plan[collection_name]
    if not isinstance(items, list):
        raise TypeError("validated task-plan collection changed type")
    for index, item in enumerate(items):
        critical = _task_item_is_unresolved(item)
        fragments.extend(
            _chunked_task_fragment(
                _canonical_json(item),
                critical=critical,
                priority=100 if critical else 20,
                ordinal_start=len(fragments),
                identifier_prefix=f"task-plan-item-{index + 1}",
                label=(
                    "Active task plan unresolved item" if critical else "Resolved task plan item"
                ),
            )
        )
    if not fragments:
        return _chunked_task_fragment(
            _canonical_json(plan),
            critical=False,
            priority=20,
            ordinal_start=0,
            identifier_prefix="task-plan-empty",
            label="Task plan with no active items",
        )
    return tuple(fragments)


def _task_item_is_unresolved(item: object) -> bool:
    if not isinstance(item, dict):
        return True
    status = item.get("status")
    if isinstance(status, str):
        return status.casefold() not in _TERMINAL_TASK_STATUSES
    return item.get("done") is not True


def _chunked_task_fragment(
    content: str,
    *,
    critical: bool,
    priority: int,
    ordinal_start: int,
    identifier_prefix: str,
    label: str,
) -> tuple[ContextFragment, ...]:
    chunks = _utf8_chunks(content, limit=_TASK_PLAN_CHUNK_BYTES)
    return tuple(
        ContextFragment(
            id=f"{identifier_prefix}-{index + 1}",
            source=ContextSource.ACTIVE_TASK_PLAN,
            message=GatewayMessage(
                role=MessageRole.SYSTEM,
                content=f"{label} ({index + 1}/{len(chunks)}):\n{chunk}",
            ),
            critical=critical,
            priority=priority,
            ordinal=ordinal_start + index,
        )
        for index, chunk in enumerate(chunks)
    )


def _utf8_chunks(value: str, *, limit: int) -> tuple[str, ...]:
    encoded = value.encode("utf-8")
    if not encoded:
        return ("",)
    chunks: list[str] = []
    offset = 0
    while offset < len(encoded):
        end = min(len(encoded), offset + limit)
        while end > offset:
            try:
                chunks.append(encoded[offset:end].decode("utf-8"))
                break
            except UnicodeDecodeError as error:
                end = offset + error.start
        if end <= offset:
            raise AssertionError("positive UTF-8 chunk limit must consume input")
        offset = end
    return tuple(chunks)


def _utf8_prefix(value: str, character_limit: int) -> str:
    return value[:character_limit].encode("utf-8").decode("utf-8")


def _require_utf8_limit(name: str, value: str, limit: int) -> None:
    if len(value.encode("utf-8")) > limit:
        raise ValueError(f"{name} exceeds its UTF-8 byte limit")


async def _next_context_gateway_event(
    stream: AsyncIterator[GatewayEvent],
) -> GatewayEvent | None:
    try:
        return await anext(stream)
    except StopAsyncIteration:
        return None
    except Exception as error:
        raise DomainOperationError(
            code="context_compression_failed",
            message="context compression could not be completed",
            retryable=True,
        ) from error


async def _close_context_gateway_stream(
    stream: AsyncIterator[GatewayEvent],
    *,
    primary_error: BaseException | None,
) -> None:
    closer = getattr(stream, "aclose", None)
    if closer is None:
        return
    try:
        await closer()
    except Exception as error:
        if primary_error is None:
            raise DomainOperationError(
                code="context_compression_failed",
                message="the summarization stream could not be closed",
                retryable=True,
            ) from error


def _raise_context_compression_error(
    *,
    code: str,
    message: str,
    details: JsonObject | None = None,
) -> Never:
    raise DomainOperationError(code=code, message=message, details=details or {})


__all__ = [
    "CONTEXT_COMPACTION_ROUTE",
    "MAX_COMPRESSION_INPUT_BYTES",
    "MAX_CONTEXT_FRAGMENTS",
    "MAX_CONTEXT_INPUT_BYTES",
    "MAX_CONTEXT_ITEM_BYTES",
    "MAX_CONTEXT_MESSAGES",
    "MAX_CONTEXT_SUMMARY_BYTES",
    "ActiveTaskPlanContributor",
    "ContextBudgetRegistry",
    "ContextBuildRequest",
    "ContextBuildResult",
    "ContextCompressionRequest",
    "ContextCompressionResult",
    "ContextCompressor",
    "ContextContributor",
    "ContextFragment",
    "ContextMemorySnippet",
    "ContextPipeline",
    "ContextRouteBudget",
    "ContextSource",
    "ContextToolResult",
    "ConversationHistoryContributor",
    "CurrentGitDiffContributor",
    "GatewayContextCompressor",
    "LongTermMemoryContributor",
    "ProjectInstructionsContributor",
    "RecentToolResultsContributor",
    "ReferencedContextFile",
    "ReferencedFilesContributor",
    "SystemInstructionsContributor",
    "TokenEstimator",
    "Utf8TokenEstimator",
]
