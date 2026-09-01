"""PostgreSQL queue, recovery, and fenced workspace-lease adapters."""

from __future__ import annotations

import json
import math
import uuid
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, cast

from pydantic import ValidationError
from sqlalchemy import Text, case, func, literal, select
from sqlalchemy import cast as sql_cast
from sqlalchemy.dialects.postgresql import insert

from agent_core.capacity import QueueDepth, QueueSnapshot, TenantQuota
from agent_core.control import ApprovalStatus
from agent_core.distributed import (
    MAX_RECOVERY_STATE_BYTES,
    DurablePendingToolCall,
    DurableToolOutcome,
    RunExecutionResult,
    RunLease,
    RunLeaseHeartbeat,
    RunRecoveryState,
    WorkerRegistration,
    WorkerStatus,
    WorkspaceWriterLease,
)
from agent_core.domain.base import FrozenJsonObject, normalize_timestamp
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.models import Checkpoint, Run
from agent_core.domain.status import ApprovalMode, RunStatus, ToolCallStatus
from agent_core.domain.transitions import transition_run
from agent_core.gateway import GatewayMessage
from agent_core.scheduling import QueueAdmissionPolicy, RunPriorityClass
from platform_persistence.capacity import ensure_tenant_quota
from platform_persistence.fencing import assert_active_run_lease
from platform_persistence.models import (
    ApprovalRecord,
    CheckpointRecord,
    MemoryExtractionJobRecord,
    MessageRecord,
    RunLeaseRecord,
    RunRecord,
    SessionRecord,
    TaskPlanRecord,
    TenantQuotaRecord,
    ToolCallRecord,
    WorkerRecord,
    WorkspaceLeaseRecord,
)
from platform_persistence.repositories import _apply_run, _run_domain

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

MAX_RECOVERY_RUNS = 1000
MAX_LEASE_SECONDS = 3600.0
MAX_RECOVERY_MESSAGES = 4096
MAX_RECOVERY_TOOL_OUTCOMES = 100
MAX_RECOVERY_CONTEXT_BYTES = 8 * 1024 * 1024
MAX_RECOVERY_TOOL_BYTES = 8 * 1024 * 1024
MAX_CLAIM_CANDIDATES = 1000


def _operation_time(value: datetime, *, code: str) -> datetime:
    try:
        return normalize_timestamp(value)
    except ValueError as error:
        raise DomainOperationError(code=code, message=str(error)) from error


def _expiry(
    occurred_at: datetime,
    duration: timedelta,
    *,
    code: str,
) -> datetime:
    seconds = duration.total_seconds()
    if not math.isfinite(seconds) or not 0 < seconds <= MAX_LEASE_SECONDS:
        raise DomainOperationError(
            code=code,
            message=f"lease duration must be in (0, {MAX_LEASE_SECONDS:g}] seconds",
        )
    return occurred_at + duration


def _worker_domain(record: WorkerRecord) -> WorkerRegistration:
    return WorkerRegistration(
        worker_id=record.worker_id,
        supported_sandbox_types=tuple(record.supported_sandbox_types),
        total_slots=record.total_slots,
        available_slots=record.available_slots,
        status=WorkerStatus(record.status),
        registered_at=record.registered_at,
        last_heartbeat_at=record.last_heartbeat_at,
    )


def _run_lease_domain(
    record: RunLeaseRecord,
    run: RunRecord,
    route_name: str,
) -> RunLease:
    return RunLease(
        tenant_id=record.tenant_id,
        run_id=record.run_id,
        session_id=run.session_id,
        workspace_id=run.workspace_id,
        worker_id=record.worker_id,
        route_name=route_name,
        lease_token=record.lease_token,
        generation=record.generation,
        attempt=run.attempt,
        execution_epoch=run.execution_epoch,
        priority=run.priority,
        priority_class=RunPriorityClass(run.priority_class),
        queued_at=run.created_at,
        acquired_at=record.acquired_at,
        expires_at=record.expires_at,
        checkpoint_id=run.last_checkpoint_id,
        cancellation_requested=run.cancellation_requested,
        traceparent=run.traceparent,
        tracestate=run.tracestate,
    )


def _assert_matching_run_lease(record: RunLeaseRecord | None, lease: RunLease) -> RunLeaseRecord:
    if (
        record is None
        or record.tenant_id != lease.tenant_id
        or record.run_id != lease.run_id
        or record.worker_id != lease.worker_id
        or record.lease_token != lease.lease_token
        or record.generation != lease.generation
    ):
        raise DomainOperationError(
            code="run_lease_lost",
            message="the run lease is no longer owned by this worker",
            retryable=True,
            details={"run_id": str(lease.run_id)},
        )
    return record


class PostgresRunQueue:
    """PostgreSQL task queue using row locks, SKIP LOCKED, and fencing tokens."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        token_factory: Callable[[], uuid.UUID] = uuid.uuid4,
        default_quota: TenantQuota | None = None,
        admission_policy: QueueAdmissionPolicy | None = None,
        wakeup: Callable[[uuid.UUID], Awaitable[None]] | None = None,
    ) -> None:
        if not callable(token_factory):
            raise TypeError("token_factory must be callable")
        self._sessions = sessions
        self._token_factory = token_factory
        self._default_quota = default_quota or TenantQuota()
        self._admission_policy = admission_policy or QueueAdmissionPolicy()
        self._wakeup = wakeup

    async def register_worker(self, registration: WorkerRegistration) -> WorkerRegistration:
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(WorkerRecord)
                .where(WorkerRecord.worker_id == registration.worker_id)
                .with_for_update()
            )
            if row is None:
                row = WorkerRecord(
                    worker_id=registration.worker_id,
                    supported_sandbox_types=list(registration.supported_sandbox_types),
                    total_slots=registration.total_slots,
                    available_slots=registration.available_slots,
                    status=registration.status.value,
                    registered_at=registration.registered_at,
                    last_heartbeat_at=registration.last_heartbeat_at,
                )
                database.add(row)
            else:
                if registration.last_heartbeat_at < row.last_heartbeat_at:
                    raise DomainOperationError(
                        code="worker_heartbeat_stale",
                        message="worker registration may not move liveness backward",
                        details={"worker_id": row.worker_id},
                    )
                active_count = await self._active_worker_leases(database, row.worker_id)
                if registration.total_slots < active_count:
                    raise DomainOperationError(
                        code="worker_capacity_conflict",
                        message="worker capacity may not be reduced below active leases",
                        details={"worker_id": row.worker_id, "active_leases": active_count},
                    )
                row.supported_sandbox_types = list(registration.supported_sandbox_types)
                row.total_slots = registration.total_slots
                row.available_slots = min(
                    registration.available_slots,
                    registration.total_slots - active_count,
                )
                row.status = registration.status.value
                row.last_heartbeat_at = registration.last_heartbeat_at
            await database.flush()
            return _worker_domain(row)

    async def heartbeat_worker(
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        available_slots: int,
    ) -> WorkerRegistration:
        timestamp = _operation_time(occurred_at, code="worker_heartbeat_invalid")
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(WorkerRecord).where(WorkerRecord.worker_id == worker_id).with_for_update()
            )
            if row is None:
                raise DomainOperationError(
                    code="worker_not_registered",
                    message="the worker is not registered",
                    details={"worker_id": worker_id},
                )
            if timestamp < row.last_heartbeat_at:
                raise DomainOperationError(
                    code="worker_heartbeat_stale",
                    message="worker heartbeat may not move liveness backward",
                    details={"worker_id": worker_id},
                )
            if type(available_slots) is not int or not 0 <= available_slots <= row.total_slots:
                raise DomainOperationError(
                    code="worker_capacity_invalid",
                    message="available worker slots are outside the registered capacity",
                    details={"worker_id": worker_id},
                )
            active_count = await self._active_worker_leases(database, worker_id)
            row.available_slots = min(available_slots, row.total_slots - active_count)
            row.last_heartbeat_at = timestamp
            await database.flush()
            return _worker_domain(row)

    async def set_worker_draining(
        self,
        worker_id: str,
        *,
        draining: bool,
        occurred_at: datetime,
    ) -> WorkerRegistration:
        timestamp = _operation_time(occurred_at, code="worker_drain_invalid")
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(WorkerRecord).where(WorkerRecord.worker_id == worker_id).with_for_update()
            )
            if row is None:
                raise DomainOperationError(
                    code="worker_not_registered",
                    message="the worker is not registered",
                    details={"worker_id": worker_id},
                )
            if timestamp < row.last_heartbeat_at:
                raise DomainOperationError(
                    code="worker_heartbeat_stale",
                    message="worker drain state may not move liveness backward",
                    details={"worker_id": worker_id},
                )
            row.status = WorkerStatus.DRAINING.value if draining else WorkerStatus.ACTIVE.value
            row.last_heartbeat_at = timestamp
            await database.flush()
            return _worker_domain(row)

    async def claim(  # noqa: PLR0915 - one auditable atomic claim transaction
        self,
        worker_id: str,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> RunLease | None:
        timestamp = _operation_time(occurred_at, code="run_claim_invalid")
        expires_at = _expiry(timestamp, lease_duration, code="run_claim_invalid")
        async with self._sessions() as database, database.begin():
            worker = await database.scalar(
                select(WorkerRecord).where(WorkerRecord.worker_id == worker_id).with_for_update()
            )
            if worker is None:
                raise DomainOperationError(
                    code="worker_not_registered",
                    message="the worker is not registered",
                    details={"worker_id": worker_id},
                )
            if worker.status != WorkerStatus.ACTIVE.value or worker.available_slots <= 0:
                return None

            claimed: tuple[RunRecord, str] | None = None
            workspace_row: WorkspaceLeaseRecord | None = None
            skipped_run_ids: list[uuid.UUID] = []
            class_rank = case(
                (RunRecord.priority_class == RunPriorityClass.INTERACTIVE.value, 2),
                (RunRecord.priority_class == RunPriorityClass.BACKGROUND.value, 1),
                else_=0,
            )
            age_boost = func.greatest(
                0,
                func.least(
                    2,
                    func.floor(
                        func.extract("epoch", timestamp - RunRecord.created_at)
                        / self._admission_policy.priority_aging_seconds
                    ),
                ),
            )
            for _ in range(MAX_CLAIM_CANDIDATES):
                statement = (
                    select(RunRecord, SessionRecord.model_route)
                    .join(
                        SessionRecord,
                        (SessionRecord.tenant_id == RunRecord.tenant_id)
                        & (SessionRecord.id == RunRecord.session_id),
                    )
                    .where(
                        RunRecord.status == RunStatus.QUEUED.value,
                        RunRecord.cancellation_requested.is_(False),
                        ~select(WorkspaceLeaseRecord.workspace_id)
                        .where(
                            WorkspaceLeaseRecord.tenant_id == RunRecord.tenant_id,
                            WorkspaceLeaseRecord.workspace_id == RunRecord.workspace_id,
                            WorkspaceLeaseRecord.lease_token.is_not(None),
                        )
                        .exists(),
                    )
                    .order_by(
                        (class_rank + age_boost).desc(),
                        RunRecord.priority.desc(),
                        RunRecord.created_at,
                        RunRecord.id,
                    )
                    .limit(1)
                    .with_for_update(skip_locked=True, of=RunRecord)
                )
                if skipped_run_ids:
                    statement = statement.where(RunRecord.id.not_in(skipped_run_ids))
                candidate = (await database.execute(statement)).first()
                if candidate is None:
                    break
                candidate_run, candidate_route = candidate
                quota = await ensure_tenant_quota(
                    database,
                    candidate_run.tenant_id,
                    default=self._default_quota,
                    occurred_at=timestamp,
                    lock=True,
                )
                tenant_active = int(
                    await database.scalar(
                        select(func.count())
                        .select_from(RunLeaseRecord)
                        .where(RunLeaseRecord.tenant_id == candidate_run.tenant_id)
                    )
                    or 0
                )
                if tenant_active >= quota.max_active_runs:
                    skipped_run_ids.append(candidate_run.id)
                    continue
                await database.execute(
                    insert(WorkspaceLeaseRecord)
                    .values(
                        tenant_id=candidate_run.tenant_id,
                        workspace_id=candidate_run.workspace_id,
                        generation=0,
                    )
                    .on_conflict_do_nothing(
                        index_elements=(
                            WorkspaceLeaseRecord.tenant_id,
                            WorkspaceLeaseRecord.workspace_id,
                        )
                    )
                )
                candidate_workspace = await database.scalar(
                    select(WorkspaceLeaseRecord)
                    .where(
                        WorkspaceLeaseRecord.tenant_id == candidate_run.tenant_id,
                        WorkspaceLeaseRecord.workspace_id == candidate_run.workspace_id,
                    )
                    .with_for_update(skip_locked=True)
                )
                if candidate_workspace is None or candidate_workspace.lease_token is not None:
                    skipped_run_ids.append(candidate_run.id)
                    continue
                claimed = (candidate_run, str(candidate_route))
                workspace_row = candidate_workspace
                break
            if claimed is None or workspace_row is None:
                return None
            run_row, route_name = claimed
            generation = run_row.lease_generation + 1
            lease_token = self._new_token()
            workspace_token = self._new_token()
            transitioned = transition_run(
                _run_domain(run_row),
                RunStatus.LEASED,
                occurred_at=timestamp,
                worker_id=worker_id,
                lease_expires_at=expires_at,
            )
            _apply_run(run_row, transitioned)
            run_row.lease_generation = generation
            lease_row = RunLeaseRecord(
                run_id=run_row.id,
                tenant_id=run_row.tenant_id,
                worker_id=worker_id,
                lease_token=lease_token,
                generation=generation,
                acquired_at=timestamp,
                last_heartbeat_at=timestamp,
                expires_at=expires_at,
            )
            database.add(lease_row)
            workspace_row.run_id = run_row.id
            workspace_row.worker_id = worker_id
            workspace_row.run_lease_token = lease_token
            workspace_row.lease_token = workspace_token
            workspace_row.generation += 1
            workspace_row.acquired_at = timestamp
            workspace_row.expires_at = expires_at
            worker.available_slots -= 1
            await database.flush()
            return _run_lease_domain(lease_row, run_row, str(route_name))

    async def snapshot(self, *, occurred_at: datetime) -> QueueSnapshot:
        """Return bounded queue depth and age for admission and later metrics."""

        timestamp = _operation_time(occurred_at, code="queue_snapshot_invalid")
        statuses = (
            RunStatus.QUEUED.value,
            RunStatus.WAITING_APPROVAL.value,
            RunStatus.RETRY_PENDING.value,
            RunStatus.LOST.value,
        )
        async with self._sessions() as database:
            counts: dict[RunPriorityClass, int] = {}
            for priority_class in RunPriorityClass:
                counts[priority_class] = int(
                    await database.scalar(
                        select(func.count())
                        .select_from(RunRecord)
                        .where(
                            RunRecord.status.in_(statuses),
                            RunRecord.priority_class == priority_class.value,
                        )
                    )
                    or 0
                )
            oldest = await database.scalar(
                select(func.min(RunRecord.created_at)).where(RunRecord.status.in_(statuses))
            )
        oldest_age = 0.0
        if isinstance(oldest, datetime):
            normalized_oldest = _operation_time(oldest, code="queue_snapshot_invalid")
            oldest_age = max(0.0, (timestamp - normalized_oldest).total_seconds())
        elif oldest is not None:
            raise DomainOperationError(
                code="queue_snapshot_invalid",
                message="the queue returned an invalid oldest-run timestamp",
                retryable=True,
            )
        return QueueSnapshot(
            depth=QueueDepth(
                interactive=counts[RunPriorityClass.INTERACTIVE],
                background=counts[RunPriorityClass.BACKGROUND],
                evaluation=counts[RunPriorityClass.EVALUATION],
            ),
            oldest_age_seconds=oldest_age,
            captured_at=timestamp,
        )

    async def authorize_sandbox_lease(
        self,
        *,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        session_id: uuid.UUID,
        workspace_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
        generation: int,
        occurred_at: datetime,
        execution_epoch: int = 1,
    ) -> None:
        """Fail closed unless both run and workspace writer leases are current."""

        timestamp = _operation_time(occurred_at, code="sandbox_lease_invalid")
        async with self._sessions() as database:
            row = (
                await database.execute(
                    select(RunLeaseRecord, RunRecord, WorkspaceLeaseRecord)
                    .join(
                        RunRecord,
                        (RunRecord.tenant_id == RunLeaseRecord.tenant_id)
                        & (RunRecord.id == RunLeaseRecord.run_id),
                    )
                    .join(
                        WorkspaceLeaseRecord,
                        (WorkspaceLeaseRecord.tenant_id == RunRecord.tenant_id)
                        & (WorkspaceLeaseRecord.workspace_id == RunRecord.workspace_id),
                    )
                    .where(
                        RunLeaseRecord.tenant_id == tenant_id,
                        RunLeaseRecord.run_id == run_id,
                        RunLeaseRecord.worker_id == worker_id,
                        RunLeaseRecord.lease_token == lease_token,
                        RunLeaseRecord.generation == generation,
                        RunLeaseRecord.expires_at > timestamp,
                        RunRecord.session_id == session_id,
                        RunRecord.workspace_id == workspace_id,
                        RunRecord.execution_epoch == execution_epoch,
                        RunRecord.status == RunStatus.RUNNING.value,
                        RunRecord.assigned_worker_id == worker_id,
                        RunRecord.cancellation_requested.is_(False),
                        WorkspaceLeaseRecord.run_id == run_id,
                        WorkspaceLeaseRecord.worker_id == worker_id,
                        WorkspaceLeaseRecord.run_lease_token == lease_token,
                        WorkspaceLeaseRecord.lease_token.is_not(None),
                        WorkspaceLeaseRecord.expires_at > timestamp,
                    )
                )
            ).first()
        if row is None:
            raise DomainOperationError(
                code="sandbox_lease_unauthorized",
                message="the sandbox request does not own current run and workspace leases",
                retryable=True,
                details={"run_id": str(run_id)},
            )

    async def start(self, lease: RunLease, *, occurred_at: datetime) -> RunLease:
        timestamp = _operation_time(occurred_at, code="run_start_invalid")
        async with self._sessions() as database, database.begin():
            lease_row, run_row, route_name = await self._locked_lease_state(database, lease)
            self._require_unexpired(lease_row, timestamp, lease.run_id)
            if run_row.status != RunStatus.LEASED.value:
                raise DomainOperationError(
                    code="run_state_conflict",
                    message="only a leased run may start",
                    details={"run_id": str(lease.run_id), "status": run_row.status},
                )
            transitioned = transition_run(
                _run_domain(run_row),
                RunStatus.RUNNING,
                occurred_at=timestamp,
            )
            _apply_run(run_row, transitioned)
            await database.flush()
            return _run_lease_domain(lease_row, run_row, route_name)

    async def heartbeat(
        self,
        lease: RunLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> RunLeaseHeartbeat:
        timestamp = _operation_time(occurred_at, code="run_heartbeat_invalid")
        expires_at = _expiry(timestamp, lease_duration, code="run_heartbeat_invalid")
        async with self._sessions() as database, database.begin():
            lease_row, run_row, _ = await self._locked_lease_state(database, lease)
            self._require_unexpired(lease_row, timestamp, lease.run_id)
            if run_row.status not in {RunStatus.LEASED.value, RunStatus.RUNNING.value}:
                raise DomainOperationError(
                    code="run_lease_lost",
                    message="the run no longer accepts lease heartbeats",
                    retryable=True,
                    details={"run_id": str(lease.run_id)},
                )
            lease_row.last_heartbeat_at = timestamp
            lease_row.expires_at = expires_at
            run_row.lease_expires_at = expires_at
            await database.flush()
            return RunLeaseHeartbeat(
                lease_token=lease_row.lease_token,
                generation=lease_row.generation,
                expires_at=expires_at,
                cancellation_requested=run_row.cancellation_requested,
            )

    async def finish(
        self,
        lease: RunLease,
        result: RunExecutionResult,
        *,
        occurred_at: datetime,
    ) -> Run:
        timestamp = _operation_time(occurred_at, code="run_finish_invalid")
        async with self._sessions() as database, database.begin():
            lease_row, run_row, _ = await self._locked_lease_state(database, lease)
            self._require_unexpired(lease_row, timestamp, lease.run_id)
            desired_status = (
                RunStatus.CANCELLED if run_row.cancellation_requested else result.status
            )
            transitioned = transition_run(
                _run_domain(run_row),
                desired_status,
                occurred_at=timestamp,
            )
            if result.last_checkpoint_id is not None:
                transitioned = transitioned.model_copy(
                    update={"last_checkpoint_id": result.last_checkpoint_id}
                )
            _apply_run(run_row, transitioned)
            run_row.retry_ready_at = (
                timestamp + timedelta(seconds=result.retry_delay_seconds)
                if desired_status is RunStatus.RETRY_PENDING
                and result.retry_delay_seconds is not None
                else None
            )
            if desired_status is RunStatus.COMPLETED:
                await self._enqueue_memory_extraction(database, run_row, timestamp)
            await self._release_workspace_for_run(database, lease_row)
            await database.delete(lease_row)
            await self._return_worker_slot(database, lease.worker_id)
            await database.flush()
            return _run_domain(run_row)

    async def _enqueue_memory_extraction(
        self,
        database: AsyncSession,
        run: RunRecord,
        created_at: datetime,
    ) -> None:
        """Atomically enqueue an enabled run's bounded memory extraction."""

        source_sequence = (
            select(func.coalesce(func.max(MessageRecord.sequence), 0))
            .join(
                RunRecord,
                (RunRecord.tenant_id == MessageRecord.tenant_id)
                & (RunRecord.id == MessageRecord.run_id),
            )
            .where(
                MessageRecord.tenant_id == run.tenant_id,
                MessageRecord.session_id == run.session_id,
                MessageRecord.execution_epoch == RunRecord.execution_epoch,
            )
            .scalar_subquery()
        )
        eligible = (
            select(
                literal(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"agent-platform:memory:{run.tenant_id}:{run.id}:{run.execution_epoch}",
                    )
                ),
                literal(run.tenant_id),
                literal(run.session_id),
                literal(run.id),
                literal(run.execution_epoch),
                literal("pending"),
                source_sequence,
                literal(1),
                literal(created_at),
            )
            .select_from(SessionRecord)
            .join(
                TenantQuotaRecord,
                TenantQuotaRecord.tenant_id == SessionRecord.tenant_id,
            )
            .where(
                SessionRecord.tenant_id == run.tenant_id,
                SessionRecord.id == run.session_id,
                SessionRecord.memory_enabled.is_(True),
                TenantQuotaRecord.memory_enabled.is_(True),
            )
        )
        await database.execute(
            insert(MemoryExtractionJobRecord)
            .from_select(
                (
                    MemoryExtractionJobRecord.id,
                    MemoryExtractionJobRecord.tenant_id,
                    MemoryExtractionJobRecord.session_id,
                    MemoryExtractionJobRecord.run_id,
                    MemoryExtractionJobRecord.execution_epoch,
                    MemoryExtractionJobRecord.status,
                    MemoryExtractionJobRecord.source_message_sequence,
                    MemoryExtractionJobRecord.attempt,
                    MemoryExtractionJobRecord.created_at,
                ),
                eligible,
            )
            .on_conflict_do_nothing(
                index_elements=(
                    MemoryExtractionJobRecord.tenant_id,
                    MemoryExtractionJobRecord.run_id,
                    MemoryExtractionJobRecord.execution_epoch,
                )
            )
        )

    async def recover_expired(
        self,
        *,
        occurred_at: datetime,
        limit: int,
    ) -> tuple[Run, ...]:
        timestamp = _operation_time(occurred_at, code="run_recovery_invalid")
        if type(limit) is not int or not 1 <= limit <= MAX_RECOVERY_RUNS:
            raise ValueError(f"limit must be in [1, {MAX_RECOVERY_RUNS}]")
        recovered: list[Run] = []
        async with self._sessions() as database, database.begin():
            retry_rows = tuple(
                (
                    await database.scalars(
                        select(RunRecord)
                        .where(
                            RunRecord.status == RunStatus.RETRY_PENDING.value,
                            RunRecord.retry_ready_at <= timestamp,
                        )
                        .order_by(RunRecord.retry_ready_at, RunRecord.id)
                        .limit(limit)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            for retry_row in retry_rows:
                queued = transition_run(
                    _run_domain(retry_row),
                    RunStatus.QUEUED,
                    occurred_at=timestamp,
                )
                _apply_run(retry_row, queued)
                retry_row.retry_ready_at = None
                recovered.append(queued)
            lease_rows = tuple(
                (
                    await database.scalars(
                        select(RunLeaseRecord)
                        .where(RunLeaseRecord.expires_at <= timestamp)
                        .order_by(RunLeaseRecord.expires_at, RunLeaseRecord.run_id)
                        .limit(max(0, limit - len(recovered)))
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            for lease_row in lease_rows:
                run_row = await database.scalar(
                    select(RunRecord)
                    .where(
                        RunRecord.tenant_id == lease_row.tenant_id,
                        RunRecord.id == lease_row.run_id,
                    )
                    .with_for_update()
                )
                if run_row is not None and (
                    run_row.lease_generation == lease_row.generation
                    and run_row.assigned_worker_id == lease_row.worker_id
                    and run_row.status in {RunStatus.LEASED.value, RunStatus.RUNNING.value}
                ):
                    current = _run_domain(run_row)
                    if current.cancellation_requested:
                        next_run = transition_run(
                            current,
                            RunStatus.CANCELLED,
                            occurred_at=timestamp,
                        )
                    else:
                        lost = transition_run(
                            current,
                            RunStatus.LOST,
                            occurred_at=timestamp,
                        )
                        next_run = transition_run(
                            lost,
                            RunStatus.QUEUED,
                            occurred_at=timestamp,
                        )
                    _apply_run(run_row, next_run)
                    recovered.append(next_run)
                await self._release_workspace_for_run(database, lease_row)
                await database.delete(lease_row)
                await self._return_worker_slot(database, lease_row.worker_id)
            await database.flush()
        for run in recovered:
            if run.status is RunStatus.QUEUED and self._wakeup is not None:
                await self._wakeup(run.id)
        return tuple(recovered)

    async def _locked_lease_state(
        self,
        database: AsyncSession,
        lease: RunLease,
    ) -> tuple[RunLeaseRecord, RunRecord, str]:
        lease_row = _assert_matching_run_lease(
            await database.scalar(
                select(RunLeaseRecord)
                .where(RunLeaseRecord.lease_token == lease.lease_token)
                .with_for_update()
            ),
            lease,
        )
        result = await database.execute(
            select(RunRecord, SessionRecord.model_route)
            .join(
                SessionRecord,
                (SessionRecord.tenant_id == RunRecord.tenant_id)
                & (SessionRecord.id == RunRecord.session_id),
            )
            .where(
                RunRecord.tenant_id == lease.tenant_id,
                RunRecord.id == lease.run_id,
                RunRecord.lease_generation == lease.generation,
                RunRecord.assigned_worker_id == lease.worker_id,
                RunRecord.execution_epoch == lease.execution_epoch,
            )
            .with_for_update(of=RunRecord)
        )
        pair = result.first()
        if pair is None:
            raise DomainOperationError(
                code="run_lease_lost",
                message="the run ownership fence no longer matches",
                retryable=True,
                details={"run_id": str(lease.run_id)},
            )
        run_row, route_name = pair
        return lease_row, run_row, str(route_name)

    @staticmethod
    def _require_unexpired(
        lease_row: RunLeaseRecord,
        occurred_at: datetime,
        run_id: uuid.UUID,
    ) -> None:
        if lease_row.expires_at <= occurred_at:
            raise DomainOperationError(
                code="run_lease_expired",
                message="the run lease expired before the operation",
                retryable=True,
                details={"run_id": str(run_id)},
            )

    async def _active_worker_leases(self, database: AsyncSession, worker_id: str) -> int:
        rows = await database.scalars(
            select(RunLeaseRecord.run_id).where(RunLeaseRecord.worker_id == worker_id)
        )
        return len(tuple(rows))

    @staticmethod
    async def _return_worker_slot(database: AsyncSession, worker_id: str) -> None:
        worker = await database.scalar(
            select(WorkerRecord).where(WorkerRecord.worker_id == worker_id).with_for_update()
        )
        if worker is not None:
            worker.available_slots = min(worker.total_slots, worker.available_slots + 1)

    @staticmethod
    async def _release_workspace_for_run(
        database: AsyncSession,
        lease: RunLeaseRecord,
    ) -> None:
        workspace = await database.scalar(
            select(WorkspaceLeaseRecord)
            .where(WorkspaceLeaseRecord.run_lease_token == lease.lease_token)
            .with_for_update()
        )
        if workspace is not None:
            _clear_workspace_owner(workspace)

    def _new_token(self) -> uuid.UUID:
        token = self._token_factory()
        if not isinstance(token, uuid.UUID):
            raise DomainOperationError(
                code="lease_token_invalid",
                message="the lease token factory returned an invalid value",
            )
        return token


class PostgresWorkspaceLeaseStore:
    """Exclusive workspace writer ownership fenced by the active run lease."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        token_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        if not callable(token_factory):
            raise TypeError("token_factory must be callable")
        self._sessions = sessions
        self._token_factory = token_factory

    async def acquire(
        self,
        run_lease: RunLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> WorkspaceWriterLease | None:
        timestamp = _operation_time(occurred_at, code="workspace_lease_invalid")
        requested_expiry = _expiry(
            timestamp,
            lease_duration,
            code="workspace_lease_invalid",
        )
        async with self._sessions() as database, database.begin():
            active_run_lease = _assert_matching_run_lease(
                await database.scalar(
                    select(RunLeaseRecord)
                    .where(RunLeaseRecord.lease_token == run_lease.lease_token)
                    .with_for_update()
                ),
                run_lease,
            )
            PostgresRunQueue._require_unexpired(
                active_run_lease,
                timestamp,
                run_lease.run_id,
            )
            expires_at = min(requested_expiry, active_run_lease.expires_at)
            if expires_at <= timestamp:
                raise DomainOperationError(
                    code="workspace_lease_invalid",
                    message="workspace lease cannot outlive an expired run lease",
                )
            await database.execute(
                insert(WorkspaceLeaseRecord)
                .values(
                    tenant_id=run_lease.tenant_id,
                    workspace_id=run_lease.workspace_id,
                    generation=0,
                )
                .on_conflict_do_nothing(
                    index_elements=(
                        WorkspaceLeaseRecord.tenant_id,
                        WorkspaceLeaseRecord.workspace_id,
                    )
                )
            )
            row = await database.scalar(
                select(WorkspaceLeaseRecord)
                .where(
                    WorkspaceLeaseRecord.tenant_id == run_lease.tenant_id,
                    WorkspaceLeaseRecord.workspace_id == run_lease.workspace_id,
                )
                .with_for_update()
            )
            if row is None:
                raise DomainOperationError(
                    code="workspace_lease_conflict",
                    message="workspace lease allocation lost its durable row",
                    retryable=True,
                )
            if row.lease_token is not None:
                if (
                    row.run_lease_token == run_lease.lease_token
                    and row.run_id == run_lease.run_id
                    and row.worker_id == run_lease.worker_id
                    and row.tenant_id == run_lease.tenant_id
                    and row.workspace_id == run_lease.workspace_id
                ):
                    row.expires_at = expires_at
                    await database.flush()
                    return _workspace_lease_domain(row)
                return None
            token = self._new_token()
            row.run_id = run_lease.run_id
            row.worker_id = run_lease.worker_id
            row.run_lease_token = run_lease.lease_token
            row.lease_token = token
            row.generation += 1
            row.acquired_at = timestamp
            row.expires_at = expires_at
            await database.flush()
            return _workspace_lease_domain(row)

    async def heartbeat(
        self,
        lease: WorkspaceWriterLease,
        *,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> WorkspaceWriterLease:
        timestamp = _operation_time(occurred_at, code="workspace_heartbeat_invalid")
        requested_expiry = _expiry(
            timestamp,
            lease_duration,
            code="workspace_heartbeat_invalid",
        )
        async with self._sessions() as database, database.begin():
            run_lease = await database.scalar(
                select(RunLeaseRecord)
                .where(RunLeaseRecord.lease_token == lease.run_lease_token)
                .with_for_update()
            )
            row = await database.scalar(
                select(WorkspaceLeaseRecord)
                .where(
                    WorkspaceLeaseRecord.tenant_id == lease.tenant_id,
                    WorkspaceLeaseRecord.workspace_id == lease.workspace_id,
                )
                .with_for_update()
            )
            if (
                run_lease is None
                or run_lease.tenant_id != lease.tenant_id
                or run_lease.worker_id != lease.worker_id
                or run_lease.run_id != lease.run_id
                or run_lease.expires_at <= timestamp
                or row is None
                or row.tenant_id != lease.tenant_id
                or row.workspace_id != lease.workspace_id
                or row.run_id != lease.run_id
                or row.worker_id != lease.worker_id
                or row.lease_token != lease.lease_token
                or row.generation != lease.generation
                or row.run_lease_token != lease.run_lease_token
            ):
                raise DomainOperationError(
                    code="workspace_lease_lost",
                    message="the workspace writer lease is no longer owned",
                    retryable=True,
                    details={"workspace_id": str(lease.workspace_id)},
                )
            expires_at = min(requested_expiry, run_lease.expires_at)
            if expires_at <= timestamp:
                raise DomainOperationError(
                    code="workspace_lease_lost",
                    message="the owning run lease has expired",
                    retryable=True,
                )
            row.expires_at = expires_at
            await database.flush()
            return _workspace_lease_domain(row)

    async def release(self, lease: WorkspaceWriterLease) -> None:
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(WorkspaceLeaseRecord)
                .where(
                    WorkspaceLeaseRecord.tenant_id == lease.tenant_id,
                    WorkspaceLeaseRecord.workspace_id == lease.workspace_id,
                )
                .with_for_update()
            )
            if row is None:
                raise _workspace_lease_lost(lease)
            if row.lease_token is None:
                if (
                    row.generation == lease.generation
                    and row.run_id is None
                    and row.worker_id is None
                    and row.run_lease_token is None
                    and row.acquired_at is None
                    and row.expires_at is None
                ):
                    return
                raise _workspace_lease_lost(lease)
            if (
                row.lease_token != lease.lease_token
                or row.generation != lease.generation
                or row.tenant_id != lease.tenant_id
                or row.workspace_id != lease.workspace_id
                or row.run_id != lease.run_id
                or row.worker_id != lease.worker_id
                or row.run_lease_token != lease.run_lease_token
            ):
                raise DomainOperationError(
                    code="workspace_lease_lost",
                    message="a stale worker may not release a successor workspace lease",
                    retryable=True,
                    details={"workspace_id": str(lease.workspace_id)},
                )
            _clear_workspace_owner(row)

    def _new_token(self) -> uuid.UUID:
        token = self._token_factory()
        if not isinstance(token, uuid.UUID):
            raise DomainOperationError(
                code="lease_token_invalid",
                message="the lease token factory returned an invalid value",
            )
        return token


class PostgresRecoveryStore:
    """Load a bounded recovery context and terminal tool outcomes from PostgreSQL."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def load(self, lease: RunLease) -> RunRecoveryState:
        async with self._sessions() as database:
            await assert_active_run_lease(database, lease)
            checkpoint_row = await self._checkpoint(database, lease)
            if lease.checkpoint_id is not None and checkpoint_row is None:
                raise DomainOperationError(
                    code="recovery_checkpoint_missing",
                    message="the run's selected checkpoint does not exist",
                    details={
                        "run_id": str(lease.run_id),
                        "checkpoint_id": str(lease.checkpoint_id),
                    },
                )
            checkpoint_messages = (
                getattr(checkpoint_row, "checkpoint_messages", [])
                if checkpoint_row is not None
                else []
            )
            checkpoint_is_active_branch = (
                checkpoint_row is None or checkpoint_row.execution_epoch == lease.execution_epoch
            )
            messages = (
                await self._messages(database, lease, message_limit=None)
                if not checkpoint_is_active_branch
                else await self._checkpoint_messages_with_suffix(
                    database,
                    lease,
                    checkpoint_messages,
                    checkpoint_sequence=(
                        checkpoint_row.message_sequence if checkpoint_row is not None else 0
                    ),
                )
                if checkpoint_messages
                else await self._messages(
                    database,
                    lease,
                    message_limit=(
                        checkpoint_row.message_sequence if checkpoint_row is not None else None
                    ),
                )
            )
            if not messages:
                raise DomainOperationError(
                    code="recovery_context_missing",
                    message="the run has no durable messages to execute",
                    details={"run_id": str(lease.run_id)},
                )
            checkpoint = (
                _checkpoint_domain(
                    checkpoint_row,
                    prefer_completed=checkpoint_is_active_branch,
                )
                if checkpoint_row is not None
                else None
            )
            if checkpoint is not None:
                task_plan = checkpoint.task_plan
                context_summary = checkpoint.context_summary
            else:
                plan_row = await database.scalar(
                    select(TaskPlanRecord)
                    .where(
                        TaskPlanRecord.tenant_id == lease.tenant_id,
                        TaskPlanRecord.run_id == lease.run_id,
                        TaskPlanRecord.execution_epoch == lease.execution_epoch,
                    )
                    .order_by(TaskPlanRecord.version.desc())
                    .limit(1)
                )
                task_plan = FrozenJsonObject(plan_row.plan if plan_row is not None else {})
                context_summary = None
            checkpoint_has_completed_state = (
                checkpoint_row is not None
                and getattr(
                    checkpoint_row,
                    "completed_snapshot_uri",
                    None,
                )
                is not None
            )
            outcomes = (
                await self._tool_outcomes(
                    database,
                    lease,
                    completed_at_or_after=(
                        checkpoint.created_at if checkpoint is not None else None
                    ),
                )
                if not checkpoint_is_active_branch
                or checkpoint is None
                or checkpoint_has_completed_state
                else ()
            )
            pending_tool_calls = (
                await self._pending_tool_calls(database, lease)
                if not checkpoint_is_active_branch
                or checkpoint is None
                or checkpoint_has_completed_state
                else ()
            )
            approval_mode_value = await database.scalar(
                select(SessionRecord.approval_mode).where(
                    SessionRecord.tenant_id == lease.tenant_id,
                    SessionRecord.id == lease.session_id,
                )
            )
            if not isinstance(approval_mode_value, str):
                raise DomainOperationError(
                    code="recovery_session_missing",
                    message="the durable recovery session is unavailable",
                )
            workspace_restore_revision = (
                checkpoint.workspace_revision if checkpoint is not None else None
            )
            return RunRecoveryState(
                checkpoint=checkpoint,
                workspace_restore_revision=workspace_restore_revision,
                messages=messages,
                task_plan=task_plan,
                context_summary=context_summary,
                prior_tool_outcomes=outcomes,
                approval_mode=ApprovalMode(approval_mode_value),
                pending_tool_calls=pending_tool_calls,
            )

    @staticmethod
    async def _checkpoint(
        database: AsyncSession,
        lease: RunLease,
    ) -> CheckpointRecord | None:
        if lease.checkpoint_id is not None:
            return cast(
                "CheckpointRecord | None",
                await database.scalar(
                    select(CheckpointRecord).where(
                        CheckpointRecord.tenant_id == lease.tenant_id,
                        CheckpointRecord.run_id == lease.run_id,
                        CheckpointRecord.id == lease.checkpoint_id,
                    )
                ),
            )
        return cast(
            "CheckpointRecord | None",
            await database.scalar(
                select(CheckpointRecord)
                .where(
                    CheckpointRecord.tenant_id == lease.tenant_id,
                    CheckpointRecord.run_id == lease.run_id,
                    CheckpointRecord.execution_epoch == lease.execution_epoch,
                )
                .order_by(CheckpointRecord.created_at.desc(), CheckpointRecord.id.desc())
                .limit(1)
            ),
        )

    @staticmethod
    def _checkpoint_messages(
        lease: RunLease,
        values: object,
    ) -> tuple[GatewayMessage, ...]:
        if not isinstance(values, list) or len(values) > MAX_RECOVERY_MESSAGES:
            raise DomainOperationError(
                code="recovery_context_limit",
                message="durable checkpoint messages exceed the message limit",
                details={"run_id": str(lease.run_id)},
            )
        try:
            encoded = json.dumps(
                values,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError):
            raise DomainOperationError(
                code="recovery_message_invalid",
                message="durable checkpoint messages are invalid",
                details={"run_id": str(lease.run_id)},
            ) from None
        if len(encoded) > MAX_RECOVERY_CONTEXT_BYTES:
            raise DomainOperationError(
                code="recovery_context_limit",
                message="durable checkpoint messages exceed the byte limit",
                details={"run_id": str(lease.run_id)},
            )
        try:
            return tuple(GatewayMessage.model_validate(value) for value in values)
        except ValidationError as error:
            raise DomainOperationError(
                code="recovery_message_invalid",
                message="a durable checkpoint message is invalid",
                details={"run_id": str(lease.run_id)},
            ) from error

    @classmethod
    async def _checkpoint_messages_with_suffix(
        cls,
        database: AsyncSession,
        lease: RunLease,
        values: object,
        *,
        checkpoint_sequence: int,
    ) -> tuple[GatewayMessage, ...]:
        """Merge the immutable checkpoint context with its journaled run suffix."""

        base = cls._checkpoint_messages(lease, values)
        if checkpoint_sequence != len(base):
            raise DomainOperationError(
                code="recovery_message_invalid",
                message="durable checkpoint message metadata is inconsistent",
                details={"run_id": str(lease.run_id)},
            )
        result = await database.stream_scalars(
            select(MessageRecord)
            .where(
                MessageRecord.tenant_id == lease.tenant_id,
                MessageRecord.session_id == lease.session_id,
                MessageRecord.run_id == lease.run_id,
                MessageRecord.execution_epoch == lease.execution_epoch,
            )
            .order_by(MessageRecord.sequence)
            .limit(MAX_RECOVERY_MESSAGES + 1)
            .execution_options(yield_per=1)
        )
        suffix: dict[int, GatewayMessage] = {}
        row_count = 0
        try:
            async for row in result:
                row_count += 1
                if row_count > MAX_RECOVERY_MESSAGES:
                    raise DomainOperationError(
                        code="recovery_context_limit",
                        message="durable recovery context exceeds the message limit",
                        details={"run_id": str(lease.run_id)},
                    )
                metadata = row.metadata_json
                if not isinstance(metadata, dict):
                    raise DomainOperationError(
                        code="recovery_message_invalid",
                        message="durable message metadata is invalid",
                        details={"run_id": str(lease.run_id)},
                    )
                transcript_index = metadata.get("transcript_index")
                if transcript_index is None:
                    continue
                if type(transcript_index) is not int or transcript_index < 1:
                    raise DomainOperationError(
                        code="recovery_message_invalid",
                        message="durable transcript position is invalid",
                        details={"run_id": str(lease.run_id)},
                    )
                if transcript_index <= checkpoint_sequence:
                    continue
                if transcript_index in suffix:
                    raise DomainOperationError(
                        code="recovery_message_invalid",
                        message="durable transcript positions are not unique",
                        details={"run_id": str(lease.run_id)},
                    )
                suffix[transcript_index] = cls._gateway_message(lease, row)
        finally:
            await result.close()

        positions = sorted(suffix)
        if positions and positions != list(
            range(checkpoint_sequence + 1, checkpoint_sequence + len(positions) + 1)
        ):
            raise DomainOperationError(
                code="recovery_message_invalid",
                message="durable transcript positions are not contiguous",
                details={"run_id": str(lease.run_id)},
            )
        merged = (*base, *(suffix[position] for position in positions))
        if len(merged) > MAX_RECOVERY_MESSAGES:
            raise DomainOperationError(
                code="recovery_context_limit",
                message="durable recovery context exceeds the message limit",
                details={"run_id": str(lease.run_id)},
            )
        encoded_bytes = sum(len(message.model_dump_json().encode("utf-8")) for message in merged)
        if encoded_bytes > MAX_RECOVERY_CONTEXT_BYTES:
            raise DomainOperationError(
                code="recovery_context_limit",
                message="durable recovery context exceeds the byte limit",
                details={
                    "run_id": str(lease.run_id),
                    "limit_bytes": MAX_RECOVERY_CONTEXT_BYTES,
                },
            )
        return merged

    @staticmethod
    def _gateway_message(lease: RunLease, row: MessageRecord) -> GatewayMessage:
        metadata = row.metadata_json
        if "role" in metadata or "content" in metadata:
            raise DomainOperationError(
                code="recovery_message_invalid",
                message="durable message metadata contains a reserved field",
                details={"run_id": str(lease.run_id), "sequence": row.sequence},
            )
        values: dict[str, object] = {
            **{
                key: value
                for key, value in metadata.items()
                if key in {"tool_call_id", "tool_calls"}
            },
            "role": row.role,
            "content": row.content,
        }
        try:
            return GatewayMessage.model_validate(values)
        except ValidationError as error:
            raise DomainOperationError(
                code="recovery_message_invalid",
                message="a durable recovery message is invalid",
                details={"run_id": str(lease.run_id), "sequence": row.sequence},
            ) from error

    @staticmethod
    async def _messages(
        database: AsyncSession,
        lease: RunLease,
        *,
        message_limit: int | None,
    ) -> tuple[GatewayMessage, ...]:
        cutoff: object
        if message_limit is None:
            cutoff = (
                select(func.max(MessageRecord.sequence))
                .where(
                    MessageRecord.tenant_id == lease.tenant_id,
                    MessageRecord.run_id == lease.run_id,
                    MessageRecord.execution_epoch == lease.execution_epoch,
                )
                .scalar_subquery()
            )
        else:
            cutoff = message_limit
        active_branch = (
            select(RunRecord.id)
            .where(
                RunRecord.tenant_id == MessageRecord.tenant_id,
                RunRecord.id == MessageRecord.run_id,
                RunRecord.execution_epoch == MessageRecord.execution_epoch,
            )
            .exists()
        )
        filters = (
            MessageRecord.tenant_id == lease.tenant_id,
            MessageRecord.session_id == lease.session_id,
            MessageRecord.sequence <= cutoff,
            active_branch,
        )
        serialized_bytes = await database.scalar(
            select(
                func.coalesce(
                    func.sum(
                        func.octet_length(MessageRecord.content)
                        + func.octet_length(sql_cast(MessageRecord.metadata_json, Text))
                    ),
                    0,
                )
            ).where(*filters)
        )
        if int(serialized_bytes or 0) > MAX_RECOVERY_CONTEXT_BYTES:
            raise DomainOperationError(
                code="recovery_context_limit",
                message="durable recovery context exceeds the byte limit",
                details={
                    "run_id": str(lease.run_id),
                    "limit_bytes": MAX_RECOVERY_CONTEXT_BYTES,
                },
            )
        result = await database.stream_scalars(
            select(MessageRecord)
            .where(*filters)
            .order_by(MessageRecord.sequence)
            .limit(MAX_RECOVERY_MESSAGES + 1)
            .execution_options(yield_per=1)
        )
        converted: list[GatewayMessage] = []
        try:
            async for row in result:
                if len(converted) >= MAX_RECOVERY_MESSAGES:
                    raise DomainOperationError(
                        code="recovery_context_limit",
                        message="durable recovery context exceeds the message limit",
                        details={"run_id": str(lease.run_id)},
                    )
                if not isinstance(row.metadata_json, dict):
                    raise DomainOperationError(
                        code="recovery_message_invalid",
                        message="durable message metadata is invalid",
                        details={"run_id": str(lease.run_id), "sequence": row.sequence},
                    )
                converted.append(PostgresRecoveryStore._gateway_message(lease, row))
        finally:
            await result.close()
        return tuple(converted)

    @staticmethod
    async def _tool_outcomes(
        database: AsyncSession,
        lease: RunLease,
        *,
        completed_at_or_after: datetime | None,
    ) -> tuple[DurableToolOutcome, ...]:
        filters = (
            ToolCallRecord.tenant_id == lease.tenant_id,
            ToolCallRecord.run_id == lease.run_id,
            ToolCallRecord.execution_epoch == lease.execution_epoch,
            ToolCallRecord.status.in_(
                (
                    ToolCallStatus.COMPLETED.value,
                    ToolCallStatus.FAILED.value,
                    ToolCallStatus.CANCELLED.value,
                )
            ),
        )
        statement = select(ToolCallRecord).where(*filters)
        size_statement = select(
            func.coalesce(
                func.sum(
                    func.octet_length(ToolCallRecord.tool_call_id)
                    + func.octet_length(ToolCallRecord.tool_name)
                    + func.coalesce(
                        func.octet_length(sql_cast(ToolCallRecord.result, Text)),
                        0,
                    )
                    + func.coalesce(
                        func.octet_length(sql_cast(ToolCallRecord.error, Text)),
                        0,
                    )
                ),
                0,
            )
        ).where(*filters)
        if completed_at_or_after is not None:
            statement = statement.where(ToolCallRecord.completed_at >= completed_at_or_after)
            size_statement = size_statement.where(
                ToolCallRecord.completed_at >= completed_at_or_after
            )
        serialized_bytes = await database.scalar(size_statement)
        if int(serialized_bytes or 0) > MAX_RECOVERY_TOOL_BYTES:
            raise DomainOperationError(
                code="recovery_tool_limit",
                message="durable recovery state exceeds the tool-outcome byte limit",
                details={
                    "run_id": str(lease.run_id),
                    "limit_bytes": MAX_RECOVERY_TOOL_BYTES,
                },
            )
        result = await database.stream_scalars(
            statement.order_by(
                ToolCallRecord.completed_at,
                ToolCallRecord.turn_number,
                ToolCallRecord.tool_call_id,
            )
            .limit(MAX_RECOVERY_TOOL_OUTCOMES + 1)
            .execution_options(yield_per=1)
        )
        outcomes: list[DurableToolOutcome] = []
        try:
            async for row in result:
                if len(outcomes) >= MAX_RECOVERY_TOOL_OUTCOMES:
                    raise DomainOperationError(
                        code="recovery_tool_limit",
                        message="durable recovery state exceeds the tool-call limit",
                        details={"run_id": str(lease.run_id)},
                    )
                outcomes.append(
                    DurableToolOutcome(
                        tool_call_id=row.tool_call_id,
                        tool_name=row.tool_name,
                        turn_number=row.turn_number,
                        argument_hash=row.argument_hash,
                        status=ToolCallStatus(row.status),
                        workspace_version=row.workspace_version,
                        result=row.result,
                        error=(
                            ErrorDetail.model_validate(row.error) if row.error is not None else None
                        ),
                    )
                )
        finally:
            await result.close()
        return tuple(outcomes)

    @staticmethod
    async def _pending_tool_calls(
        database: AsyncSession,
        lease: RunLease,
    ) -> tuple[DurablePendingToolCall, ...]:
        rows = (
            await database.execute(
                select(ToolCallRecord, ApprovalRecord)
                .outerjoin(
                    ApprovalRecord,
                    (ApprovalRecord.tenant_id == ToolCallRecord.tenant_id)
                    & (ApprovalRecord.run_id == ToolCallRecord.run_id)
                    & (ApprovalRecord.execution_epoch == ToolCallRecord.execution_epoch)
                    & (ApprovalRecord.tool_call_id == ToolCallRecord.tool_call_id),
                )
                .where(
                    ToolCallRecord.tenant_id == lease.tenant_id,
                    ToolCallRecord.run_id == lease.run_id,
                    ToolCallRecord.execution_epoch == lease.execution_epoch,
                    ToolCallRecord.status.in_(
                        (
                            ToolCallStatus.RECEIVED.value,
                            ToolCallStatus.WAITING_APPROVAL.value,
                        )
                    ),
                )
                .order_by(ToolCallRecord.turn_number, ToolCallRecord.tool_call_id)
                .limit(MAX_RECOVERY_TOOL_OUTCOMES + 1)
            )
        ).all()
        if len(rows) > MAX_RECOVERY_TOOL_OUTCOMES:
            raise DomainOperationError(
                code="recovery_tool_limit",
                message="durable recovery contains too many pending tool calls",
            )
        pending: list[DurablePendingToolCall] = []
        for tool, approval in rows:
            approval_id: uuid.UUID | None = None
            approved: bool | None = None
            if tool.status == ToolCallStatus.WAITING_APPROVAL.value:
                if approval is None or approval.status == ApprovalStatus.PENDING.value:
                    raise DomainOperationError(
                        code="recovery_approval_incomplete",
                        message="a resumed run contains an undecided approval",
                        retryable=True,
                    )
                approval_id = approval.id
                approved = approval.status == ApprovalStatus.APPROVED.value
            elif approval is not None:
                raise DomainOperationError(
                    code="recovery_approval_invalid",
                    message="a durable approval is attached to a non-waiting tool call",
                )
            pending.append(
                DurablePendingToolCall(
                    tool_call_id=tool.tool_call_id,
                    tool_name=tool.tool_name,
                    turn_number=tool.turn_number,
                    arguments=tool.arguments,
                    argument_hash=tool.argument_hash,
                    approval_id=approval_id,
                    approval_approved=approved,
                    approval_response=(
                        getattr(approval, "response", None) if approval is not None else None
                    ),
                )
            )
        return tuple(pending)


def _workspace_lease_domain(record: WorkspaceLeaseRecord) -> WorkspaceWriterLease:
    if (
        record.run_id is None
        or record.worker_id is None
        or record.run_lease_token is None
        or record.lease_token is None
        or record.acquired_at is None
        or record.expires_at is None
    ):
        raise DomainOperationError(
            code="workspace_lease_corrupt",
            message="the durable workspace writer lease is incomplete",
        )
    return WorkspaceWriterLease(
        tenant_id=record.tenant_id,
        workspace_id=record.workspace_id,
        run_id=record.run_id,
        worker_id=record.worker_id,
        run_lease_token=record.run_lease_token,
        lease_token=record.lease_token,
        generation=record.generation,
        acquired_at=record.acquired_at,
        expires_at=record.expires_at,
    )


def _workspace_lease_lost(lease: WorkspaceWriterLease) -> DomainOperationError:
    return DomainOperationError(
        code="workspace_lease_lost",
        message="the workspace writer lease is no longer owned",
        retryable=True,
        details={"workspace_id": str(lease.workspace_id)},
    )


def _clear_workspace_owner(record: WorkspaceLeaseRecord) -> None:
    record.run_id = None
    record.worker_id = None
    record.run_lease_token = None
    record.lease_token = None
    record.acquired_at = None
    record.expires_at = None


def _checkpoint_domain(
    record: CheckpointRecord,
    *,
    prefer_completed: bool = True,
) -> Checkpoint:
    """Select post-tool state for recovery and pre-tool state for a rewind fork."""

    completed_uri = getattr(record, "completed_snapshot_uri", None)
    completed_revision = getattr(record, "completed_revision", None)
    if (completed_uri is None) != (completed_revision is None):
        raise DomainOperationError(
            code="recovery_checkpoint_invalid",
            message="the durable checkpoint completion state is incomplete",
        )
    return Checkpoint(
        id=record.id,
        run_id=record.run_id,
        session_id=record.session_id,
        execution_epoch=record.execution_epoch,
        tool_call_id=record.tool_call_id,
        message_sequence=record.message_sequence,
        messages=tuple(
            FrozenJsonObject(message) for message in getattr(record, "checkpoint_messages", [])
        ),
        workspace_snapshot_uri=(
            completed_uri
            if prefer_completed and completed_uri is not None
            else record.workspace_snapshot_uri
        ),
        workspace_revision=(
            completed_revision
            if prefer_completed and completed_revision is not None
            else record.workspace_revision
        ),
        task_plan=record.task_plan,
        context_summary=record.context_summary,
        created_at=record.created_at,
    )


__all__ = [
    "MAX_LEASE_SECONDS",
    "MAX_RECOVERY_CONTEXT_BYTES",
    "MAX_RECOVERY_MESSAGES",
    "MAX_RECOVERY_RUNS",
    "MAX_RECOVERY_STATE_BYTES",
    "MAX_RECOVERY_TOOL_BYTES",
    "MAX_RECOVERY_TOOL_OUTCOMES",
    "PostgresRecoveryStore",
    "PostgresRunQueue",
    "PostgresWorkspaceLeaseStore",
]
