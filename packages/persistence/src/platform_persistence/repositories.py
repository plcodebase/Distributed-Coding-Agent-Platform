"""Tenant-scoped PostgreSQL repositories for sessions, runs, and control actions."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from agent_core.control import (
    ApprovalDecision,
    ApprovalStatus,
    IdempotencyKey,
    PersistedApproval,
    PersistedMessage,
    PersistedTaskPlan,
    RunCreationResult,
    run_creation_hash,
)
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.models import Checkpoint, ModelCall, Run, Session, ToolCall
from agent_core.domain.status import (
    ApprovalMode,
    RunStatus,
    SessionStatus,
    ToolCallStatus,
)
from agent_core.domain.transitions import transition_run
from platform_persistence.fencing import assert_active_run_lease
from platform_persistence.models import (
    ApprovalRecord,
    CheckpointRecord,
    MessageRecord,
    ModelCallRecord,
    RunRecord,
    SessionRecord,
    TaskPlanRecord,
    ToolCallRecord,
)

_IDEMPOTENCY_KEY_ADAPTER: TypeAdapter[IdempotencyKey] = TypeAdapter(IdempotencyKey)
_TOOL_CALL_TRANSITIONS: dict[ToolCallStatus, frozenset[ToolCallStatus]] = {
    ToolCallStatus.RECEIVED: frozenset(
        {
            ToolCallStatus.WAITING_APPROVAL,
            ToolCallStatus.RUNNING,
            ToolCallStatus.FAILED,
            ToolCallStatus.CANCELLED,
        }
    ),
    ToolCallStatus.WAITING_APPROVAL: frozenset(
        {
            ToolCallStatus.RUNNING,
            ToolCallStatus.FAILED,
            ToolCallStatus.CANCELLED,
        }
    ),
    ToolCallStatus.RUNNING: frozenset(
        {
            ToolCallStatus.COMPLETED,
            ToolCallStatus.FAILED,
            ToolCallStatus.CANCELLED,
        }
    ),
    ToolCallStatus.COMPLETED: frozenset(),
    ToolCallStatus.FAILED: frozenset(),
    ToolCallStatus.CANCELLED: frozenset(),
}
_TOOL_CALL_PREDECESSORS: dict[ToolCallStatus, frozenset[ToolCallStatus]] = {
    ToolCallStatus.RECEIVED: frozenset(),
    ToolCallStatus.WAITING_APPROVAL: frozenset({ToolCallStatus.RECEIVED}),
    ToolCallStatus.RUNNING: frozenset(
        {
            ToolCallStatus.RECEIVED,
            ToolCallStatus.WAITING_APPROVAL,
        }
    ),
    ToolCallStatus.COMPLETED: frozenset(
        {
            ToolCallStatus.RECEIVED,
            ToolCallStatus.WAITING_APPROVAL,
            ToolCallStatus.RUNNING,
        }
    ),
    ToolCallStatus.FAILED: frozenset(
        {
            ToolCallStatus.RECEIVED,
            ToolCallStatus.WAITING_APPROVAL,
            ToolCallStatus.RUNNING,
        }
    ),
    ToolCallStatus.CANCELLED: frozenset(
        {
            ToolCallStatus.RECEIVED,
            ToolCallStatus.WAITING_APPROVAL,
            ToolCallStatus.RUNNING,
        }
    ),
}

if TYPE_CHECKING:
    from datetime import datetime


    from agent_core.distributed import RunLease
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class PostgresSessionRepository:
    """Tenant-scoped session persistence."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def create(self, session: Session) -> Session:
        async with self._sessions() as database, database.begin():
            database.add(_session_record(session))
        return session

    async def get(self, tenant_id: uuid.UUID, session_id: uuid.UUID) -> Session | None:
        async with self._sessions() as database:
            row = await database.scalar(
                select(SessionRecord).where(
                    SessionRecord.tenant_id == tenant_id,
                    SessionRecord.id == session_id,
                )
            )
        return _session_domain(row) if row is not None else None

    async def exists(
        self,
        database: AsyncSession,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> bool:
        session_id_value = await database.scalar(
            select(SessionRecord.id).where(
                SessionRecord.tenant_id == tenant_id,
                SessionRecord.id == session_id,
            )
        )
        return session_id_value is not None


class PostgresRunRepository:
    """Tenant-scoped run storage with compare-and-set lifecycle operations."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def create_idempotent(
        self,
        tenant_id: uuid.UUID,
        run: Run,
        *,
        idempotency_key: IdempotencyKey,
        creation_hash: str,
    ) -> RunCreationResult:
        try:
            idempotency_key = _IDEMPOTENCY_KEY_ADAPTER.validate_python(idempotency_key)
        except ValidationError:
            raise DomainOperationError(
                code="invalid_run_creation",
                message="the run idempotency key is invalid",
            ) from None
        expected_creation_hash = run_creation_hash(priority=run.priority)
        if creation_hash != expected_creation_hash:
            raise DomainOperationError(
                code="invalid_run_creation",
                message="the run creation hash does not match the run payload",
            )
        values = _run_values(
            tenant_id,
            run,
            idempotency_key=idempotency_key,
            creation_hash=creation_hash,
        )
        async with self._sessions() as database, database.begin():
            inserted = await database.scalar(
                insert(RunRecord)
                .values(**values)
                .on_conflict_do_nothing(
                    index_elements=(
                        RunRecord.tenant_id,
                        RunRecord.session_id,
                        RunRecord.idempotency_key,
                    )
                )
                .returning(RunRecord.id)
            )
            if inserted is not None:
                return RunCreationResult(run=run, created=True)
            existing = await database.scalar(
                select(RunRecord).where(
                    RunRecord.tenant_id == tenant_id,
                    RunRecord.session_id == run.session_id,
                    RunRecord.idempotency_key == idempotency_key,
                )
            )
            if existing is None:
                raise _persistence_conflict("run creation lost its idempotency record")
            if existing.creation_hash != creation_hash:
                raise DomainOperationError(
                    code="run_idempotency_conflict",
                    message="the idempotency key belongs to a different run payload",
                    details={
                        "idempotency_key": idempotency_key,
                        "session_id": str(run.session_id),
                    },
                )
            return RunCreationResult(run=_run_domain(existing), created=False)

    async def get(self, tenant_id: uuid.UUID, run_id: uuid.UUID) -> Run | None:
        async with self._sessions() as database:
            row = await database.scalar(
                select(RunRecord).where(
                    RunRecord.tenant_id == tenant_id,
                    RunRecord.id == run_id,
                )
            )
        return _run_domain(row) if row is not None else None

    async def transition(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        expected_status: RunStatus,
        new_status: RunStatus,
        *,
        occurred_at: datetime,
        worker_id: str | None = None,
        lease_expires_at: datetime | None = None,
    ) -> bool:
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(RunRecord)
                .where(
                    RunRecord.tenant_id == tenant_id,
                    RunRecord.id == run_id,
                )
                .with_for_update()
            )
            if row is None or row.status != expected_status.value:
                return False
            transitioned = transition_run(
                _run_domain(row),
                new_status,
                occurred_at=occurred_at,
                worker_id=worker_id,
                lease_expires_at=lease_expires_at,
            )
            _apply_run(row, transitioned)
        return True

    async def request_cancel(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        *,
        occurred_at: datetime,
    ) -> Run | None:
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(RunRecord)
                .where(
                    RunRecord.tenant_id == tenant_id,
                    RunRecord.id == run_id,
                )
                .with_for_update()
            )
            if row is None:
                return None
            run = _run_domain(row)
            if run.status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}:
                return run
            if run.status in {
                RunStatus.QUEUED,
                RunStatus.WAITING_APPROVAL,
                RunStatus.RETRY_PENDING,
                RunStatus.LOST,
            }:
                run = transition_run(run, RunStatus.CANCELLED, occurred_at=occurred_at)
            else:
                run = run.model_copy(update={"cancellation_requested": True})
            _apply_run(row, run)
            return run

    async def rewind(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        checkpoint_id: uuid.UUID,
    ) -> Run | None:
        async with self._sessions() as database, database.begin():
            checkpoint_exists = await database.scalar(
                select(CheckpointRecord.id).where(
                    CheckpointRecord.tenant_id == tenant_id,
                    CheckpointRecord.run_id == run_id,
                    CheckpointRecord.id == checkpoint_id,
                )
            )
            if checkpoint_exists is None:
                return None
            row = await database.scalar(
                select(RunRecord)
                .where(
                    RunRecord.tenant_id == tenant_id,
                    RunRecord.id == run_id,
                )
                .with_for_update()
            )
            if row is None:
                return None
            row.last_checkpoint_id = checkpoint_id
            return _run_domain(row)


class PostgresApprovalRepository:
    """Durable tenant-scoped approval decisions."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def decide(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        approval_id: uuid.UUID,
        decision: ApprovalDecision,
    ) -> PersistedApproval | None:
        status = ApprovalStatus.APPROVED if decision.approved else ApprovalStatus.REJECTED
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(ApprovalRecord)
                .where(
                    ApprovalRecord.tenant_id == tenant_id,
                    ApprovalRecord.run_id == run_id,
                    ApprovalRecord.id == approval_id,
                )
                .with_for_update()
            )
            if row is None:
                return None
            if row.status != ApprovalStatus.PENDING.value:
                if row.status != status.value:
                    raise DomainOperationError(
                        code="approval_decision_conflict",
                        message="the approval already has a different durable decision",
                        details={"approval_id": str(approval_id)},
                    )
                return _approval_domain(row)
            row.status = status.value
            row.decided_by = decision.decided_by
            row.decided_at = decision.decided_at
            run_row = await database.scalar(
                select(RunRecord)
                .where(
                    RunRecord.tenant_id == tenant_id,
                    RunRecord.id == run_id,
                )
                .with_for_update()
            )
            if run_row is None:
                raise _persistence_conflict("approval run disappeared")
            if run_row.status == RunStatus.WAITING_APPROVAL.value:
                resumed = transition_run(
                    _run_domain(run_row),
                    RunStatus.QUEUED,
                    occurred_at=decision.decided_at,
                )
                _apply_run(run_row, resumed)
            return _approval_domain(row)


class PostgresExecutionRepository:
    """Persistence for messages, plans, tools, approvals, checkpoints, and model calls."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def append_message(
        self,
        tenant_id: uuid.UUID,
        message: PersistedMessage,
    ) -> PersistedMessage:
        async with self._sessions() as database, database.begin():
            database.add(
                MessageRecord(
                    id=message.id,
                    tenant_id=tenant_id,
                    session_id=message.session_id,
                    run_id=message.run_id,
                    sequence=message.sequence,
                    role=message.role.value,
                    content=message.content,
                    metadata_json=message.metadata.to_json_object(),
                    created_at=message.created_at,
                )
            )
        return message

    async def save_task_plan(
        self,
        tenant_id: uuid.UUID,
        task_plan: PersistedTaskPlan,
    ) -> PersistedTaskPlan:
        async with self._sessions() as database, database.begin():
            database.add(
                TaskPlanRecord(
                    id=task_plan.id,
                    tenant_id=tenant_id,
                    run_id=task_plan.run_id,
                    version=task_plan.version,
                    plan=task_plan.plan.to_json_object(),
                    created_at=task_plan.created_at,
                )
            )
        return task_plan

    async def save_tool_call(self, tenant_id: uuid.UUID, tool_call: ToolCall) -> ToolCall:
        async with self._sessions() as database, database.begin():
            return await self._save_tool_call(database, tenant_id, tool_call)

    async def save_tool_call_fenced(
        self,
        lease: RunLease,
        tool_call: ToolCall,
    ) -> ToolCall:
        """Persist worker-owned tool state under the exact active run fence."""

        if tool_call.run_id != lease.run_id:
            raise DomainOperationError(
                code="tool_call_run_mismatch",
                message="the tool call does not belong to the fenced run",
                details={"run_id": str(lease.run_id), "tool_call_id": tool_call.id},
            )
        async with self._sessions() as database, database.begin():
            await assert_active_run_lease(database, lease)
            return await self._save_tool_call(database, lease.tenant_id, tool_call)

    @staticmethod
    async def _save_tool_call(
        database: AsyncSession,
        tenant_id: uuid.UUID,
        tool_call: ToolCall,
    ) -> ToolCall:
        row = await database.scalar(
            select(ToolCallRecord)
            .where(
                ToolCallRecord.tenant_id == tenant_id,
                ToolCallRecord.run_id == tool_call.run_id,
                ToolCallRecord.tool_call_id == tool_call.id,
            )
            .with_for_update()
        )
        if row is None:
            row = ToolCallRecord(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                run_id=tool_call.run_id,
                tool_call_id=tool_call.id,
                turn_number=tool_call.turn_number,
                tool_name=tool_call.tool_name,
                arguments=tool_call.arguments.to_json_object(),
                argument_hash=tool_call.argument_hash,
                status=tool_call.status.value,
                workspace_version=tool_call.workspace_version,
                result=(
                    tool_call.result.to_json_object() if tool_call.result is not None else None
                ),
                error=tool_call.error.model_dump(mode="json") if tool_call.error else None,
                started_at=tool_call.started_at,
                completed_at=tool_call.completed_at,
            )
            database.add(row)
        elif (
            row.argument_hash != tool_call.argument_hash
            or row.tool_name != tool_call.tool_name
            or row.turn_number != tool_call.turn_number
            or row.arguments != tool_call.arguments.to_json_object()
        ):
            raise DomainOperationError(
                code="tool_call_id_conflict",
                message="the tool-call ID belongs to a different logical invocation",
                details={"run_id": str(tool_call.run_id), "tool_call_id": tool_call.id},
            )
        elif _tool_call_row_matches(row, tool_call):
            return _tool_call_domain(row)
        else:
            current_status = ToolCallStatus(row.status)
            if tool_call.status in _TOOL_CALL_PREDECESSORS[current_status]:
                return _tool_call_domain(row)
            if tool_call.status not in _TOOL_CALL_TRANSITIONS[current_status]:
                raise DomainOperationError(
                    code="tool_call_state_conflict",
                    message="the durable tool-call state cannot be overwritten",
                    details={
                        "run_id": str(tool_call.run_id),
                        "tool_call_id": tool_call.id,
                        "current_status": current_status.value,
                        "requested_status": tool_call.status.value,
                    },
                )
            row.status = tool_call.status.value
            row.workspace_version = tool_call.workspace_version
            row.result = tool_call.result.to_json_object() if tool_call.result is not None else None
            row.error = (
                tool_call.error.model_dump(mode="json") if tool_call.error is not None else None
            )
            row.started_at = tool_call.started_at
            row.completed_at = tool_call.completed_at
        return tool_call

    async def create_approval(
        self,
        tenant_id: uuid.UUID,
        approval: PersistedApproval,
        *,
        tool_call_id: str | None = None,
    ) -> PersistedApproval:
        async with self._sessions() as database, database.begin():
            database.add(
                ApprovalRecord(
                    id=approval.id,
                    tenant_id=tenant_id,
                    run_id=approval.run_id,
                    tool_call_id=tool_call_id,
                    status=approval.status.value,
                    reason=approval.reason,
                    arguments=approval.arguments.to_json_object(),
                    decided_by=approval.decided_by,
                    requested_at=approval.requested_at,
                    decided_at=approval.decided_at,
                )
            )
        return approval

    async def create_checkpoint(
        self,
        tenant_id: uuid.UUID,
        checkpoint: Checkpoint,
    ) -> Checkpoint:
        async with self._sessions() as database, database.begin():
            database.add(
                CheckpointRecord(
                    id=checkpoint.id,
                    tenant_id=tenant_id,
                    run_id=checkpoint.run_id,
                    session_id=checkpoint.session_id,
                    message_sequence=checkpoint.message_sequence,
                    workspace_snapshot_uri=checkpoint.workspace_snapshot_uri,
                    workspace_revision=checkpoint.workspace_revision,
                    task_plan=checkpoint.task_plan.to_json_object(),
                    context_summary=checkpoint.context_summary,
                    created_at=checkpoint.created_at,
                )
            )
        return checkpoint

    async def save_model_call(
        self,
        tenant_id: uuid.UUID,
        model_call: ModelCall,
    ) -> ModelCall:
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(ModelCallRecord)
                .where(
                    ModelCallRecord.tenant_id == tenant_id,
                    ModelCallRecord.run_id == model_call.run_id,
                    ModelCallRecord.model_call_id == model_call.id,
                )
                .with_for_update()
            )
            if row is None:
                database.add(_model_call_record(tenant_id, model_call))
            elif (
                row.request_id != model_call.request_id
                or row.route_name != model_call.route_name
                or row.started_at != model_call.started_at
            ):
                raise DomainOperationError(
                    code="model_call_id_conflict",
                    message="the model-call ID belongs to another logical request",
                    details={"model_call_id": model_call.id},
                )
            else:
                _apply_model_call(row, model_call)
        return model_call


def _session_record(session: Session) -> SessionRecord:
    return SessionRecord(
        id=session.id,
        tenant_id=session.tenant_id,
        workspace_id=session.workspace_id,
        status=session.status.value,
        approval_mode=session.approval_mode.value,
        model_route=session.model_route,
        created_at=session.created_at,
        updated_at=session.updated_at,
    )


def _session_domain(record: SessionRecord) -> Session:
    return Session(
        id=record.id,
        tenant_id=record.tenant_id,
        workspace_id=record.workspace_id,
        status=SessionStatus(record.status),
        approval_mode=ApprovalMode(record.approval_mode),
        model_route=record.model_route,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def _run_values(
    tenant_id: uuid.UUID,
    run: Run,
    *,
    idempotency_key: str,
    creation_hash: str,
) -> dict[str, object]:
    return {
        "id": run.id,
        "tenant_id": tenant_id,
        "session_id": run.session_id,
        "workspace_id": run.workspace_id,
        "status": run.status.value,
        "priority": run.priority,
        "attempt": run.attempt,
        "assigned_worker_id": run.assigned_worker_id,
        "lease_expires_at": run.lease_expires_at,
        "last_checkpoint_id": run.last_checkpoint_id,
        "cancellation_requested": run.cancellation_requested,
        "idempotency_key": idempotency_key,
        "creation_hash": creation_hash,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
    }


def _run_domain(record: RunRecord) -> Run:
    return Run(
        id=record.id,
        session_id=record.session_id,
        workspace_id=record.workspace_id,
        status=RunStatus(record.status),
        priority=record.priority,
        attempt=record.attempt,
        assigned_worker_id=record.assigned_worker_id,
        lease_expires_at=record.lease_expires_at,
        last_checkpoint_id=record.last_checkpoint_id,
        cancellation_requested=record.cancellation_requested,
        created_at=record.created_at,
        started_at=record.started_at,
        completed_at=record.completed_at,
    )


def _apply_run(record: RunRecord, run: Run) -> None:
    record.status = run.status.value
    record.attempt = run.attempt
    record.assigned_worker_id = run.assigned_worker_id
    record.lease_expires_at = run.lease_expires_at
    record.last_checkpoint_id = run.last_checkpoint_id
    record.cancellation_requested = run.cancellation_requested
    record.started_at = run.started_at
    record.completed_at = run.completed_at


def _approval_domain(record: ApprovalRecord) -> PersistedApproval:
    return PersistedApproval(
        id=record.id,
        run_id=record.run_id,
        status=ApprovalStatus(record.status),
        reason=record.reason,
        arguments=record.arguments,
        decided_by=record.decided_by,
        requested_at=record.requested_at,
        decided_at=record.decided_at,
    )


def _model_call_record(tenant_id: uuid.UUID, model_call: ModelCall) -> ModelCallRecord:
    return ModelCallRecord(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        run_id=model_call.run_id,
        model_call_id=model_call.id,
        request_id=model_call.request_id,
        route_name=model_call.route_name,
        provider=model_call.provider,
        model=model_call.model,
        status=model_call.status.value,
        input_tokens=model_call.input_tokens,
        output_tokens=model_call.output_tokens,
        cached_tokens=model_call.cached_tokens,
        estimated_cost_usd=model_call.estimated_cost_usd,
        retry_count=model_call.retry_count,
        fallback_count=model_call.fallback_count,
        started_at=model_call.started_at,
        first_token_at=model_call.first_token_at,
        completed_at=model_call.completed_at,
    )


def _apply_model_call(record: ModelCallRecord, model_call: ModelCall) -> None:
    record.provider = model_call.provider
    record.model = model_call.model
    record.status = model_call.status.value
    record.input_tokens = model_call.input_tokens
    record.output_tokens = model_call.output_tokens
    record.cached_tokens = model_call.cached_tokens
    record.estimated_cost_usd = model_call.estimated_cost_usd
    record.retry_count = model_call.retry_count
    record.fallback_count = model_call.fallback_count
    record.first_token_at = model_call.first_token_at
    record.completed_at = model_call.completed_at


def _tool_call_row_matches(record: ToolCallRecord, tool_call: ToolCall) -> bool:
    return (
        record.status == tool_call.status.value
        and record.workspace_version == tool_call.workspace_version
        and record.result
        == (tool_call.result.to_json_object() if tool_call.result is not None else None)
        and getattr(record, "error", None)
        == (tool_call.error.model_dump(mode="json") if tool_call.error is not None else None)
    )


def _tool_call_domain(record: ToolCallRecord) -> ToolCall:
    return ToolCall(
        id=record.tool_call_id,
        run_id=record.run_id,
        turn_number=record.turn_number,
        tool_name=record.tool_name,
        arguments=record.arguments,
        argument_hash=record.argument_hash,
        status=ToolCallStatus(record.status),
        workspace_version=record.workspace_version,
        result=record.result,
        error=(
            ErrorDetail.model_validate(record.error)
            if getattr(record, "error", None) is not None
            else None
        ),
        started_at=record.started_at,
        completed_at=record.completed_at,
    )


def _persistence_conflict(message: str) -> DomainOperationError:
    return DomainOperationError(
        code="persistence_conflict",
        message=message,
        retryable=True,
    )


__all__ = [
    "ApprovalDecision",
    "ApprovalStatus",
    "IdempotencyKey",
    "PersistedApproval",
    "PersistedMessage",
    "PersistedTaskPlan",
    "PostgresApprovalRepository",
    "PostgresExecutionRepository",
    "PostgresRunRepository",
    "PostgresSessionRepository",
    "RunCreationResult",
    "run_creation_hash",
]
