from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import cast

import pytest

from agent_core.events import RunCompletedEvent
from agent_core.fakes import ScriptedGatewayTurn, ScriptedModelGateway, SequentialIdGenerator
from agent_core.gateway import GatewayMessage, MessageRole
from agent_core.local_session import InMemoryAgentSession, InMemoryTranscriptJournal
from agent_core.loop import AgentLoop, TranscriptJournal
from agent_core.tools import ToolRegistry


class _Clock:
    def now(self) -> datetime:
        return datetime(2026, 8, 20, 12, tzinfo=UTC)


@pytest.mark.asyncio
async def test_in_memory_session_retains_normalized_conversation_across_runs() -> None:
    gateway = ScriptedModelGateway(
        (
            ScriptedGatewayTurn.text("first answer"),
            ScriptedGatewayTurn.text("second answer"),
        )
    )
    run_ids = iter(
        (
            uuid.UUID("10000000-0000-0000-0000-000000000001"),
            uuid.UUID("10000000-0000-0000-0000-000000000002"),
        )
    )

    def loop_factory(run_id: uuid.UUID, journal: TranscriptJournal) -> AgentLoop:
        del run_id
        return AgentLoop(
            gateway=gateway,
            tools=ToolRegistry(),
            clock=_Clock(),
            id_generator=SequentialIdGenerator(),
            transcript_journal=journal,
        )

    session = InMemoryAgentSession(
        loop_factory=loop_factory,
        route_name="coding-default",
        system_message="Work carefully.",
        run_id_factory=lambda: next(run_ids),
    )

    first = [event async for event in session.run("first task")]
    second = [event async for event in session.run("second task")]

    assert isinstance(first[-1], RunCompletedEvent)
    assert isinstance(second[-1], RunCompletedEvent)
    assert [(message.role, message.content) for message in session.messages] == [
        (MessageRole.SYSTEM, "Work carefully."),
        (MessageRole.USER, "first task"),
        (MessageRole.ASSISTANT, "first answer"),
        (MessageRole.USER, "second task"),
        (MessageRole.ASSISTANT, "second answer"),
    ]
    assert gateway.requests[1].messages == session.messages[:-1]

    await session.reset()
    assert session.messages == (GatewayMessage(role=MessageRole.SYSTEM, content="Work carefully."),)


@pytest.mark.asyncio
async def test_in_memory_transcript_journal_rejects_wrong_run_gaps_and_limits() -> None:
    run_id = uuid.uuid4()
    initial = (GatewayMessage(role=MessageRole.USER, content="hello"),)
    journal = InMemoryTranscriptJournal(run_id, initial, max_messages=2, max_bytes=1024)

    with pytest.raises(ValueError, match="different local run"):
        await journal.append(
            uuid.uuid4(),
            start_index=1,
            messages=(GatewayMessage(role=MessageRole.ASSISTANT, content="answer"),),
        )
    with pytest.raises(ValueError, match="not contiguous"):
        await journal.append(
            run_id,
            start_index=0,
            messages=(GatewayMessage(role=MessageRole.ASSISTANT, content="answer"),),
        )
    await journal.append(
        run_id,
        start_index=1,
        messages=(GatewayMessage(role=MessageRole.ASSISTANT, content="answer"),),
    )
    with pytest.raises(ValueError, match="configured limit"):
        await journal.append(
            run_id,
            start_index=2,
            messages=(GatewayMessage(role=MessageRole.USER, content="too many"),),
        )


@pytest.mark.asyncio
async def test_in_memory_session_validates_task_and_factory_contracts() -> None:
    def invalid_factory(run_id: uuid.UUID, journal: TranscriptJournal) -> AgentLoop:
        del run_id, journal
        return cast("AgentLoop", object())

    session = InMemoryAgentSession(
        loop_factory=invalid_factory,
        route_name="coding-default",
        run_id_factory=uuid.uuid4,
    )
    with pytest.raises(ValueError, match="non-empty"):
        _ = [event async for event in session.run(" ")]
    with pytest.raises(TypeError, match="must return AgentLoop"):
        _ = [event async for event in session.run("task")]
