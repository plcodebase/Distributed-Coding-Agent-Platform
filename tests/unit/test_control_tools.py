from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from agent_core.control import PersistedTaskState, TaskPlanUpdate, TrackedTask
from agent_core.control_tools import (
    AgentControlToolset,
    AskUserArguments,
    UpdateTaskPlanArguments,
)
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.fakes import SteppingClock
from agent_core.tools import ToolExecutionCompleted, ToolExecutionContext

NOW = datetime(2026, 8, 20, tzinfo=UTC)
TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
RUN_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
PLAN_ID = uuid.UUID("30000000-0000-0000-0000-000000000003")


class _Tasks:
    def __init__(self, state: PersistedTaskState | None) -> None:
        self.state = state
        self.received: TaskPlanUpdate | None = None

    async def update(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        update: TaskPlanUpdate,
        *,
        plan_id: uuid.UUID,
        created_at: datetime,
    ) -> PersistedTaskState | None:
        assert (tenant_id, run_id, plan_id, created_at) == (TENANT_ID, RUN_ID, PLAN_ID, NOW)
        self.received = update
        return self.state


def _context(run_id: uuid.UUID = RUN_ID) -> ToolExecutionContext:
    return ToolExecutionContext(
        run_id=run_id,
        tool_call_id="plan-1",
        max_output_bytes=1024,
        max_result_bytes=4096,
    )


@pytest.mark.asyncio
async def test_control_tool_registry_updates_a_versioned_plan() -> None:
    task = TrackedTask(id="task-1", title="Implement persistence")
    store = _Tasks(
        PersistedTaskState(
            id=PLAN_ID,
            run_id=RUN_ID,
            version=2,
            tasks=(task,),
            created_at=NOW,
        )
    )
    tools = AgentControlToolset(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        tasks=store,
        clock=SteppingClock(NOW),
        id_factory=lambda: PLAN_ID,
    )
    registry = tools.registry()

    assert [definition.name for definition in registry.definitions] == [
        "ask_user",
        "update_task_plan",
    ]
    prepared = registry.prepare(
        "update_task_plan",
        FrozenJsonObject({"expected_version": 1, "tasks": [task.model_dump(mode="json")]}),
    )
    events = [event async for event in prepared.stream(_context())]

    assert len(events) == 1 and isinstance(events[0], ToolExecutionCompleted)
    assert events[0].result is not None
    assert events[0].result["version"] == 2
    assert store.received is not None and store.received.expected_version == 1


@pytest.mark.asyncio
async def test_control_tools_fail_closed_for_unresumed_or_mismatched_state() -> None:
    store = _Tasks(None)
    tools = AgentControlToolset(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        tasks=store,
        clock=SteppingClock(NOW),
        id_factory=lambda: PLAN_ID,
    )
    with pytest.raises(DomainOperationError) as interaction:
        async for _ in tools.ask_user(AskUserArguments(question="Proceed?"), _context()):
            pass
    assert interaction.value.code == "interaction_not_resumed"

    arguments = UpdateTaskPlanArguments(expected_version=1, tasks=())
    with pytest.raises(DomainOperationError) as mismatch:
        async for _ in tools.update_task_plan(arguments, _context(uuid.uuid4())):
            pass
    assert mismatch.value.code == "task_plan_run_mismatch"

    with pytest.raises(DomainOperationError) as missing:
        async for _ in tools.update_task_plan(arguments, _context()):
            pass
    assert missing.value.code == "task_plan_run_missing"

    invalid_ids = AgentControlToolset(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        tasks=store,
        clock=SteppingClock(NOW),
        id_factory=lambda: "not-a-uuid",  # type: ignore[arg-type,return-value]
    )
    with pytest.raises(DomainOperationError) as invalid:
        async for _ in invalid_ids.update_task_plan(arguments, _context()):
            pass
    assert invalid.value.code == "task_plan_id_invalid"

    with pytest.raises(TypeError, match="factory"):
        AgentControlToolset(
            tenant_id=TENANT_ID,
            run_id=RUN_ID,
            tasks=store,
            clock=SteppingClock(NOW),
            id_factory=None,  # type: ignore[arg-type]
        )
