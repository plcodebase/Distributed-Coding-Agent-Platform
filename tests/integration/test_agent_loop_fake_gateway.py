from datetime import UTC, datetime
from uuid import UUID

import pytest

from agent_core.events import EventType, ToolCompletedEvent
from agent_core.fakes import (
    ScriptedGatewayTurn,
    ScriptedModelGateway,
    SequentialIdGenerator,
    SteppingClock,
)
from agent_core.gateway import GatewayMessage, GatewayToolCall, MessageRole
from agent_core.loop import AgentLoop, AgentLoopInput
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolExecutionResult,
    ToolRegistry,
)


class InspectFileArguments(ToolArguments):
    path: str


class InMemoryRepository:
    def __init__(self) -> None:
        self.files = {"src/main.py": "print('deterministic')\n"}
        self.inspected_paths: list[str] = []

    async def inspect_file(
        self,
        arguments: InspectFileArguments,
    ) -> ToolExecutionResult:
        self.inspected_paths.append(arguments.path)
        return ToolExecutionResult(
            result={
                "path": arguments.path,
                "content": self.files[arguments.path],
            }
        )


@pytest.mark.integration
async def test_scripted_model_drives_complete_typed_agent_loop_without_network() -> None:
    repository = InMemoryRepository()
    tool_call = GatewayToolCall(
        id="stable-tool-call-1",
        name="inspect_file",
        arguments={"path": "src/main.py"},
    )
    gateway = ScriptedModelGateway(
        [
            ScriptedGatewayTurn.tool_calls(tool_call),
            ScriptedGatewayTurn.text("The sample repository contains valid Python."),
        ]
    )
    registry = ToolRegistry(
        (
            RegisteredTool(
                name="inspect_file",
                description="Inspect a file in the current workspace",
                arguments_type=InspectFileArguments,
                handler=repository.inspect_file,
            ),
        )
    )
    loop = AgentLoop(
        gateway=gateway,
        tools=registry,
        clock=SteppingClock(datetime(2026, 7, 28, 12, tzinfo=UTC)),
        id_generator=SequentialIdGenerator(),
    )
    loop_input = AgentLoopInput(
        run_id=UUID("10000000-0000-0000-0000-000000000001"),
        attempt=1,
        worker_id="integration-worker",
        route_name="fake-coding",
        messages=(
            GatewayMessage(
                role=MessageRole.USER,
                content="Inspect src/main.py and summarize it.",
            ),
        ),
    )

    events = [event async for event in loop.run(loop_input)]

    assert repository.inspected_paths == ["src/main.py"]
    assert [event.sequence for event in events] == list(range(1, len(events) + 1))
    assert events[-1].event_type is EventType.RUN_COMPLETED
    assert any(isinstance(event, ToolCompletedEvent) for event in events)
    assert len(gateway.requests) == 2
    assert gateway.remaining_turns == 0
    assert [message.role for message in gateway.requests[1].messages] == [
        MessageRole.USER,
        MessageRole.ASSISTANT,
        MessageRole.TOOL,
    ]
