"""Agent-facing interaction and durable task-plan tool registrations."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Annotated, Protocol

from pydantic import Field, StringConstraints

from agent_core.control import PersistedTaskState, TaskPlanUpdate, TrackedTask
from agent_core.domain.base import DomainModel, FrozenJsonObject
from agent_core.domain.errors import DomainOperationError
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolEffect,
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolExecutionEvent,
    ToolRegistry,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable
    from datetime import datetime

    from agent_core.loop import Clock


class AskUserArguments(ToolArguments):
    """One bounded question that always suspends for a human response."""

    question: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=4096),
    ]
    choices: tuple[
        Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=512)],
        ...,
    ] = Field(default=(), max_length=20)


class UpdateTaskPlanArguments(ToolArguments):
    """Compare-and-set replacement of the current durable task plan."""

    expected_version: int = Field(ge=1)
    tasks: tuple[TrackedTask, ...] = Field(max_length=500)


class UpdateTaskPlanResult(DomainModel):
    run_id: uuid.UUID
    version: int = Field(ge=1)
    task_plan: FrozenJsonObject


class AgentTaskPlanStore(Protocol):
    async def update(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        update: TaskPlanUpdate,
        *,
        plan_id: uuid.UUID,
        created_at: datetime,
    ) -> PersistedTaskState | None: ...


class AgentControlToolset:
    """Bind one run's interaction and plan tools to durable state."""

    def __init__(
        self,
        *,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        tasks: AgentTaskPlanStore,
        clock: Clock,
        id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        if not callable(id_factory):
            raise TypeError("task-plan ID factory must be callable")
        self._tenant_id = tenant_id
        self._run_id = run_id
        self._tasks = tasks
        self._clock = clock
        self._id_factory = id_factory

    def registry(self) -> ToolRegistry:
        return ToolRegistry(
            (
                RegisteredTool(
                    name="ask_user",
                    description=(
                        "Ask one bounded question and suspend until the user supplies a response."
                    ),
                    arguments_type=AskUserArguments,
                    handler=self.ask_user,
                    effect=ToolEffect.INTERACTION,
                ),
                RegisteredTool(
                    name="update_task_plan",
                    description="Replace the durable task plan using its expected version.",
                    arguments_type=UpdateTaskPlanArguments,
                    handler=self.update_task_plan,
                    effect=ToolEffect.CONTROL_MUTATION,
                ),
            )
        )

    async def ask_user(
        self,
        arguments: AskUserArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        del context
        if not arguments.question:  # pragma: no cover - rejected by the closed schema
            yield ToolExecutionCompleted()
        raise DomainOperationError(
            code="interaction_not_resumed",
            message="ask_user must be resumed from a durable human response",
        )

    async def update_task_plan(
        self,
        arguments: UpdateTaskPlanArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        if context.run_id != self._run_id:
            raise DomainOperationError(
                code="task_plan_run_mismatch",
                message="the task-plan tool is bound to another run",
            )
        plan_id = self._id_factory()
        if not isinstance(plan_id, uuid.UUID):
            raise DomainOperationError(
                code="task_plan_id_invalid",
                message="the task-plan identifier factory returned an invalid value",
            )
        state = await self._tasks.update(
            self._tenant_id,
            self._run_id,
            TaskPlanUpdate(
                expected_version=arguments.expected_version,
                tasks=arguments.tasks,
            ),
            plan_id=plan_id,
            created_at=self._clock.now(),
        )
        if state is None:
            raise DomainOperationError(
                code="task_plan_run_missing",
                message="the task-plan run no longer exists",
            )
        result = UpdateTaskPlanResult(
            run_id=state.run_id,
            version=state.version,
            task_plan={
                "tasks": [task.model_dump(mode="json") for task in state.tasks],
            },
        )
        yield ToolExecutionCompleted(result=result.model_dump(mode="json"))


__all__ = [
    "AgentControlToolset",
    "AgentTaskPlanStore",
    "AskUserArguments",
    "UpdateTaskPlanArguments",
    "UpdateTaskPlanResult",
]
