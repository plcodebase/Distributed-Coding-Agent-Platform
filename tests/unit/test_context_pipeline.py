from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from pydantic import ValidationError

from agent_core.context import (
    ActiveTaskPlanContributor,
    ContextBudgetRegistry,
    ContextBuildRequest,
    ContextCompressionRequest,
    ContextCompressionResult,
    ContextMemorySnippet,
    ContextPipeline,
    ContextRouteBudget,
    ContextToolResult,
    GatewayContextCompressor,
    ReferencedContextFile,
    Utf8TokenEstimator,
)
from agent_core.domain import DomainOperationError
from agent_core.fakes import ScriptedGatewayTurn, ScriptedModelGateway, SequentialIdGenerator
from agent_core.gateway import (
    GatewayFinishReason,
    GatewayMessage,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCall,
    GatewayToolCallEvent,
    MessageRole,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from agent_core.gateway import GatewayEvent, GatewayRequest


TENANT_ID = UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = UUID("20000000-0000-0000-0000-000000000002")
RUN_ID = UUID("30000000-0000-0000-0000-000000000003")
MEMORY_ID = UUID("40000000-0000-0000-0000-000000000004")


class RecordingCompressor:
    def __init__(self, summary: str = "bounded historical summary") -> None:
        self.summary = summary
        self.requests: list[ContextCompressionRequest] = []

    async def compress(self, request: ContextCompressionRequest) -> ContextCompressionResult:
        self.requests.append(request)
        return ContextCompressionResult(
            summary=self.summary,
            input_tokens=10,
            output_tokens=4,
        )


def _request(**updates: object) -> ContextBuildRequest:
    values: dict[str, object] = {
        "tenant_id": TENANT_ID,
        "session_id": SESSION_ID,
        "run_id": RUN_ID,
        "route_name": "coding-default",
        "system_instructions": "Follow the coding-agent policy.",
        "project_instructions": "Run tests after edits.",
        "conversation": (
            GatewayMessage(role=MessageRole.USER, content="Earlier request"),
            GatewayMessage(role=MessageRole.ASSISTANT, content="Earlier response"),
            GatewayMessage(role=MessageRole.USER, content="Current request"),
        ),
        "referenced_files": (
            ReferencedContextFile(path="old.py", content="old = True"),
            ReferencedContextFile(path="active.py", content="active = True", active=True),
        ),
        "task_plan": {"steps": [{"text": "finish sequence 23", "done": False}]},
        "recent_tool_results": (
            ContextToolResult(
                tool_call_id="call-ok",
                tool_name="read_file",
                content="read succeeded",
            ),
            ContextToolResult(
                tool_call_id="call-error",
                tool_name="run_command",
                content="tests failed",
                is_error=True,
            ),
        ),
        "memories": (ContextMemorySnippet(memory_id=MEMORY_ID, content="Uses uv."),),
        "current_git_diff": "+ changed",
        "previous_summary": "A previous durable compacted summary.",
    }
    values.update(updates)
    return ContextBuildRequest.model_validate(values)


def _pipeline(
    compressor: RecordingCompressor,
    *,
    maximum: int = 100_000,
    reservation: int = 1_000,
) -> ContextPipeline:
    return ContextPipeline(
        budgets=ContextBudgetRegistry(
            (
                ContextRouteBudget(
                    route_name="coding-default",
                    max_context_tokens=maximum,
                    reserved_output_tokens=reservation,
                ),
            )
        ),
        compressor=compressor,
    )


@pytest.mark.asyncio
async def test_context_pipeline_assembles_every_source_in_deterministic_order() -> None:
    compressor = RecordingCompressor()
    result = await _pipeline(compressor).build(_request())

    assert result.compressed is False
    assert compressor.requests == []
    assert result.messages[0].role is MessageRole.SYSTEM
    system = result.messages[0].content
    assert system.index("previous durable") < system.index("Follow the coding-agent policy")
    assert system.index("Follow the coding-agent policy") < system.index("Run tests after edits")
    assert "Referenced file `active.py`" in system
    assert "Active task plan" in system
    assert "Recent run_command result" in system
    assert "Relevant durable memory" in system
    assert "+ changed" in system
    assert tuple(message.content for message in result.messages[1:]) == (
        "Earlier request",
        "Earlier response",
        "Current request",
    )
    assert len(result.retained_fragment_ids) == 13


@pytest.mark.asyncio
async def test_compaction_preserves_critical_context_and_drops_only_noncritical() -> None:
    compressor = RecordingCompressor("old details summarized")
    result = await _pipeline(compressor).build(_request(force_compaction=True))

    assert result.compressed is True
    assert result.summary == "old details summarized"
    assert len(compressor.requests) == 1
    source = compressor.requests[0].source_text
    assert "memory-" in source
    assert "referenced-file-1" in source
    assert "active.py" not in source
    rendered = "\n".join(message.content for message in result.messages)
    assert "Compacted prior context:\nold details summarized" in rendered
    assert "active.py" in rendered
    assert "finish sequence 23" in rendered
    assert "tests failed" in rendered
    assert "Current request" in rendered
    assert "memory-" not in result.retained_fragment_ids
    assert "new-compacted-summary" in result.retained_fragment_ids
    assert "previous-compacted-summary" in result.dropped_fragment_ids


@pytest.mark.asyncio
async def test_task_plan_preserves_only_unresolved_items_as_critical() -> None:
    request = _request(
        task_plan={
            "version": 4,
            "tasks": [
                {"id": "done", "title": "Historical result", "status": "completed"},
                {"id": "cancelled", "title": "Discarded path", "status": "cancelled"},
                {"id": "pending", "title": "Ship hardening", "status": "pending"},
                {"id": "unknown", "title": "Conservative state", "status": "future"},
            ],
        }
    )
    fragments = await ActiveTaskPlanContributor().contribute(request)
    by_content = {fragment.message.content: fragment for fragment in fragments}
    assert (
        next(value for key, value in by_content.items() if '"id":"done"' in key).critical is False
    )
    assert (
        next(value for key, value in by_content.items() if '"id":"cancelled"' in key).critical
        is False
    )
    assert next(value for key, value in by_content.items() if '"id":"pending"' in key).critical
    assert next(value for key, value in by_content.items() if '"id":"unknown"' in key).critical

    compressor = RecordingCompressor("resolved task history")
    result = await _pipeline(compressor, maximum=700, reservation=128).build(
        _request(
            force_compaction=True,
            system_instructions="policy",
            project_instructions="",
            conversation=(),
            referenced_files=(),
            task_plan=request.task_plan,
            recent_tool_results=(),
            memories=(),
            current_git_diff="",
            previous_summary=None,
        )
    )
    rendered = "\n".join(message.content for message in result.messages)
    assert "Ship hardening" in rendered
    assert "Historical result" not in rendered
    assert "Historical result" in compressor.requests[0].source_text


@pytest.mark.asyncio
async def test_legacy_task_plan_material_is_chunked_below_fragment_ceiling() -> None:
    fragments = await ActiveTaskPlanContributor().contribute(
        _request(task_plan={"legacy": "🙂" * 100_000})
    )
    assert len(fragments) > 1
    assert all(fragment.critical for fragment in fragments)
    assert all(
        len(fragment.message.model_dump_json().encode("utf-8")) < 1024 * 1024
        for fragment in fragments
    )


@pytest.mark.asyncio
async def test_critical_context_overflow_fails_before_compressor() -> None:
    compressor = RecordingCompressor()
    pipeline = _pipeline(compressor, maximum=512, reservation=128)

    with pytest.raises(DomainOperationError) as caught:
        await pipeline.build(
            _request(
                system_instructions="x" * 800,
                project_instructions="",
                conversation=(),
                referenced_files=(),
                task_plan={},
                recent_tool_results=(),
                memories=(),
                current_git_diff="",
                previous_summary=None,
            )
        )

    assert caught.value.code == "context_critical_limit"
    assert compressor.requests == []


@pytest.mark.asyncio
async def test_context_route_must_be_explicitly_configured() -> None:
    compressor = RecordingCompressor()
    with pytest.raises(DomainOperationError) as caught:
        await _pipeline(compressor).build(_request(route_name="unknown"))
    assert caught.value.code == "context_route_unconfigured"


@pytest.mark.asyncio
async def test_recent_tool_pair_is_preserved_as_one_critical_conversation_unit() -> None:
    call = GatewayToolCall(id="call-1", name="read_file", arguments={"path": "a.py"})
    history = (
        *(GatewayMessage(role=MessageRole.USER, content=f"old-{index}") for index in range(12)),
        GatewayMessage(role=MessageRole.ASSISTANT, tool_calls=(call,)),
        GatewayMessage(role=MessageRole.TOOL, tool_call_id="call-1", content="source"),
    )
    compressor = RecordingCompressor()
    result = await _pipeline(compressor).build(
        _request(
            force_compaction=True,
            conversation=history,
            referenced_files=(),
            task_plan={},
            recent_tool_results=(),
            memories=(),
            current_git_diff="",
            previous_summary=None,
        )
    )
    assert result.messages[-2].tool_calls[0].id == "call-1"
    assert result.messages[-1].tool_call_id == "call-1"


def test_context_models_are_closed_bounded_and_use_utf8_accounting() -> None:
    with pytest.raises(ValidationError):
        ContextRouteBudget(
            route_name="coding-default",
            max_context_tokens=256,
            reserved_output_tokens=256,
        )
    with pytest.raises(ValidationError):
        ContextMemorySnippet.model_validate(
            {"memory_id": MEMORY_ID, "content": "ok", "unknown": True}
        )
    ascii_tokens = Utf8TokenEstimator().estimate_messages(
        (GatewayMessage(role=MessageRole.USER, content="aa"),)
    )
    emoji_tokens = Utf8TokenEstimator().estimate_messages(
        (GatewayMessage(role=MessageRole.USER, content="🙂🙂"),)
    )
    assert emoji_tokens > ascii_tokens


@pytest.mark.asyncio
async def test_gateway_context_compressor_uses_summarization_route_and_redacts() -> None:
    gateway = ScriptedModelGateway(
        [
            ScriptedGatewayTurn(
                events=(
                    GatewayTextDelta(delta="safe summary"),
                    GatewayResponseCompleted(
                        finish_reason=GatewayFinishReason.STOP,
                        input_tokens=9,
                        output_tokens=3,
                    ),
                )
            )
        ]
    )
    compressor = GatewayContextCompressor(
        gateway=gateway,
        id_generator=SequentialIdGenerator(),
    )
    result = await compressor.compress(
        ContextCompressionRequest(
            tenant_id=TENANT_ID,
            session_id=SESSION_ID,
            run_id=RUN_ID,
            source_text="token sk-abcdefghijklmnopqrstuvwxyz123456",
            max_summary_bytes=100,
        )
    )
    assert result.summary == "safe summary"
    assert result.input_tokens == 9
    request = gateway.requests[0]
    assert request.route_name == "summarization"
    assert "sk-abcdefghijklmnopqrstuvwxyz123456" not in request.messages[1].content


@pytest.mark.asyncio
async def test_gateway_context_compressor_rejects_tools_and_oversized_output() -> None:
    tool_gateway = ScriptedModelGateway(
        [
            ScriptedGatewayTurn(
                events=(
                    GatewayToolCallEvent(
                        tool_call=GatewayToolCall(id="call-1", name="read_file", arguments={})
                    ),
                    GatewayResponseCompleted(finish_reason=GatewayFinishReason.TOOL_CALLS),
                )
            )
        ]
    )
    compressor = GatewayContextCompressor(
        gateway=tool_gateway,
        id_generator=SequentialIdGenerator(),
    )
    request = ContextCompressionRequest(
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        run_id=RUN_ID,
        source_text="old context",
        max_summary_bytes=4,
    )
    with pytest.raises(DomainOperationError) as tool_error:
        await compressor.compress(request)
    assert tool_error.value.code == "context_compression_invalid"

    oversized = GatewayContextCompressor(
        gateway=ScriptedModelGateway([ScriptedGatewayTurn.text("🙂🙂")]),
        id_generator=SequentialIdGenerator(),
    )
    with pytest.raises(DomainOperationError) as size_error:
        await oversized.compress(request)
    assert size_error.value.code == "context_summary_limit"


class FailingClosingGateway:
    def __init__(self) -> None:
        self.closed = False

    async def stream(self, request: GatewayRequest) -> AsyncIterator[GatewayEvent]:
        assert request.route_name == "summarization"
        try:
            if request.turn_number > 0:
                raise RuntimeError("provider secret must remain opaque")
            yield GatewayTextDelta(delta="unreachable")
        finally:
            self.closed = True


@pytest.mark.asyncio
async def test_gateway_failure_is_opaque_and_closes_stream() -> None:
    gateway = FailingClosingGateway()
    compressor = GatewayContextCompressor(
        gateway=gateway,
        id_generator=SequentialIdGenerator(),
    )
    with pytest.raises(DomainOperationError) as caught:
        await compressor.compress(
            ContextCompressionRequest(
                tenant_id=TENANT_ID,
                session_id=SESSION_ID,
                run_id=RUN_ID,
                source_text="old context",
                max_summary_bytes=100,
            )
        )
    assert caught.value.code == "context_compression_failed"
    assert "provider secret" not in str(caught.value)
    assert gateway.closed is True
