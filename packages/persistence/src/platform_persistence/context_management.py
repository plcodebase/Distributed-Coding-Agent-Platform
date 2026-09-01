"""Durable context compaction, task tracking, and long-term memory adapters."""

from __future__ import annotations

import json
import uuid
from datetime import timedelta
from typing import TYPE_CHECKING, cast

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert

from agent_core.control import (
    MAX_REFERENCED_WORKSPACE_FILES,
    ContextCompactionStatus,
    IdempotencyKey,
    MemoryExtractionJob,
    MemoryExtractionStatus,
    MemoryKind,
    PersistedContextCompaction,
    PersistedMemory,
    PersistedMessage,
    PersistedTaskState,
    TaskPlanUpdate,
    TaskStatus,
    TrackedTask,
)
from agent_core.domain.base import FrozenJsonObject, normalize_timestamp
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.gateway import MessageRole
from agent_core.workspace_access import WorkspaceFileReference
from platform_persistence.models import (
    ContextCompactionRecord,
    MemoryExtractionJobRecord,
    MemoryRecord,
    MessageRecord,
    RunRecord,
    SessionRecord,
    TaskPlanRecord,
    TenantQuotaRecord,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import datetime

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


MAX_MEMORY_RESULTS = 500
MAX_CONTEXT_HISTORY_MESSAGES = 4096
MAX_MEMORIES_PER_EXTRACTION = 100
MAX_MEMORY_SOURCE_MESSAGES = 2000
MEMORY_SOURCE_BATCH_SIZE = 64
MAX_MEMORY_JOB_ATTEMPTS = 100
MAX_MEMORY_LEASE_SECONDS = 3600.0
MAX_MEMORY_WORKER_ID_LENGTH = 255


class PostgresContextRepository:
    """Tenant-scoped non-destructive transcript compaction state."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def list_messages(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        after_sequence: int | None = None,
        through_sequence: int | None = None,
        limit: int = MAX_CONTEXT_HISTORY_MESSAGES,
    ) -> tuple[PersistedMessage, ...]:
        """Load one chronological, bounded conversation slice for context assembly."""

        for name, value in (
            ("after_sequence", after_sequence),
            ("through_sequence", through_sequence),
        ):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} must be a nonnegative integer")
        if (
            after_sequence is not None
            and through_sequence is not None
            and through_sequence < after_sequence
        ):
            raise ValueError("through_sequence may not precede after_sequence")
        if type(limit) is not int or not 1 <= limit <= MAX_CONTEXT_HISTORY_MESSAGES:
            raise ValueError("context history limit must be in [1, 4096]")
        statement = (
            select(MessageRecord)
            .join(
                RunRecord,
                (RunRecord.tenant_id == MessageRecord.tenant_id)
                & (RunRecord.id == MessageRecord.run_id),
            )
            .where(
                MessageRecord.tenant_id == tenant_id,
                MessageRecord.session_id == session_id,
                MessageRecord.execution_epoch == RunRecord.execution_epoch,
            )
        )
        if after_sequence is not None:
            statement = statement.where(MessageRecord.sequence > after_sequence)
        if through_sequence is not None:
            statement = statement.where(MessageRecord.sequence <= through_sequence)
        async with self._sessions() as database:
            rows = tuple(
                await database.scalars(statement.order_by(MessageRecord.sequence).limit(limit + 1))
            )
        if len(rows) > limit:
            raise DomainOperationError(
                code="context_history_limit",
                message="durable conversation history exceeds the configured message limit",
                details={"limit": limit},
            )
        try:
            return tuple(
                PersistedMessage(
                    id=row.id,
                    session_id=row.session_id,
                    run_id=row.run_id,
                    execution_epoch=row.execution_epoch,
                    sequence=row.sequence,
                    role=MessageRole(row.role),
                    content=row.content,
                    metadata=FrozenJsonObject(row.metadata_json),
                    created_at=row.created_at,
                )
                for row in rows
            )
        except (TypeError, ValueError):
            raise DomainOperationError(
                code="context_history_invalid",
                message="durable conversation history contains invalid data",
            ) from None

    async def referenced_files_for_run(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
    ) -> tuple[WorkspaceFileReference, ...]:
        """Load the immutable explicit references from a run's original submission."""

        async with self._sessions() as database:
            metadata = await database.scalar(
                select(MessageRecord.metadata_json)
                .where(
                    MessageRecord.tenant_id == tenant_id,
                    MessageRecord.run_id == run_id,
                )
                .order_by(MessageRecord.execution_epoch, MessageRecord.sequence)
                .limit(1)
            )
        if metadata is None:
            return ()
        if not isinstance(metadata, dict):
            raise DomainOperationError(
                code="context_references_invalid",
                message="the durable run submission metadata is invalid",
            )
        if metadata.get("source") != "run_submission":
            # Runs created before explicit references were introduced have no
            # submission marker. They remain valid and simply contribute no files.
            return ()
        raw_references = metadata.get("referenced_files", [])
        if not isinstance(raw_references, list):
            raise DomainOperationError(
                code="context_references_invalid",
                message="the durable run file references are invalid",
            )
        try:
            references = tuple(
                WorkspaceFileReference.model_validate(item) for item in raw_references
            )
        except (TypeError, ValueError):
            raise DomainOperationError(
                code="context_references_invalid",
                message="the durable run file references are invalid",
            ) from None
        if len(references) > MAX_REFERENCED_WORKSPACE_FILES or len(
            {item.path for item in references}
        ) != len(references):
            raise DomainOperationError(
                code="context_references_invalid",
                message="the durable run file references are invalid",
            )
        return references

    async def request_compaction(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        compaction_id: uuid.UUID,
        idempotency_key: IdempotencyKey,
        route_name: str,
        requested_at: datetime,
    ) -> PersistedContextCompaction | None:
        async with self._sessions() as database, database.begin():
            session = await database.scalar(
                select(SessionRecord)
                .where(
                    SessionRecord.tenant_id == tenant_id,
                    SessionRecord.id == session_id,
                )
                .with_for_update()
            )
            if session is None:
                return None
            existing = await self._by_idempotency_key(
                database,
                tenant_id,
                session_id,
                idempotency_key,
            )
            if existing is not None:
                return _validate_compaction_replay(existing, route_name=route_name)
            pending = await database.scalar(
                select(ContextCompactionRecord)
                .where(
                    ContextCompactionRecord.tenant_id == tenant_id,
                    ContextCompactionRecord.session_id == session_id,
                    ContextCompactionRecord.context_generation == session.context_generation,
                    ContextCompactionRecord.status == ContextCompactionStatus.PENDING.value,
                )
                .order_by(ContextCompactionRecord.requested_at, ContextCompactionRecord.id)
                .limit(1)
            )
            if pending is not None:
                raise DomainOperationError(
                    code="context_compaction_in_progress",
                    message="the session already has a pending context compaction",
                    retryable=True,
                    details={"compaction_id": str(pending.id)},
                )
            source_sequence = int(
                await database.scalar(
                    select(func.coalesce(func.max(MessageRecord.sequence), 0))
                    .join(
                        RunRecord,
                        (RunRecord.tenant_id == MessageRecord.tenant_id)
                        & (RunRecord.id == MessageRecord.run_id),
                    )
                    .where(
                        MessageRecord.tenant_id == tenant_id,
                        MessageRecord.session_id == session_id,
                        MessageRecord.execution_epoch == RunRecord.execution_epoch,
                    )
                )
                or 0
            )
            await database.execute(
                insert(ContextCompactionRecord)
                .values(
                    id=compaction_id,
                    tenant_id=tenant_id,
                    session_id=session_id,
                    context_generation=session.context_generation,
                    status=ContextCompactionStatus.PENDING.value,
                    idempotency_key=idempotency_key,
                    source_message_sequence=source_sequence,
                    route_name=route_name,
                    requested_at=requested_at,
                )
                .on_conflict_do_nothing(
                    index_elements=(
                        ContextCompactionRecord.tenant_id,
                        ContextCompactionRecord.session_id,
                        ContextCompactionRecord.idempotency_key,
                    )
                )
            )
            row = await self._by_idempotency_key(
                database,
                tenant_id,
                session_id,
                idempotency_key,
            )
            if row is None:
                raise _state_conflict("compaction request disappeared after insertion")
            return _validate_compaction_replay(row, route_name=route_name)

    async def request_compaction_if_needed(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        after_message_sequence: int | None,
        threshold_messages: int,
        route_name: str,
        requested_at: datetime,
    ) -> PersistedContextCompaction | None:
        """Atomically schedule compaction before the bounded history query is exhausted."""

        if after_message_sequence is not None and (
            type(after_message_sequence) is not int or after_message_sequence < 0
        ):
            raise ValueError("after_message_sequence must be a nonnegative integer")
        if type(threshold_messages) is not int or not (
            1 <= threshold_messages < MAX_CONTEXT_HISTORY_MESSAGES
        ):
            raise ValueError("threshold_messages must be in [1, 4095]")
        requested = normalize_timestamp(requested_at)
        async with self._sessions() as database, database.begin():
            session = await database.scalar(
                select(SessionRecord)
                .where(
                    SessionRecord.tenant_id == tenant_id,
                    SessionRecord.id == session_id,
                )
                .with_for_update()
            )
            if session is None:
                return None
            pending = await database.scalar(
                select(ContextCompactionRecord)
                .where(
                    ContextCompactionRecord.tenant_id == tenant_id,
                    ContextCompactionRecord.session_id == session_id,
                    ContextCompactionRecord.context_generation == session.context_generation,
                    ContextCompactionRecord.status == ContextCompactionStatus.PENDING.value,
                )
                .order_by(ContextCompactionRecord.requested_at, ContextCompactionRecord.id)
                .limit(1)
            )
            if pending is not None:
                return _compaction_domain(pending)
            count_statement = (
                select(func.count(MessageRecord.id))
                .join(
                    RunRecord,
                    (RunRecord.tenant_id == MessageRecord.tenant_id)
                    & (RunRecord.id == MessageRecord.run_id),
                )
                .where(
                    MessageRecord.tenant_id == tenant_id,
                    MessageRecord.session_id == session_id,
                    MessageRecord.execution_epoch == RunRecord.execution_epoch,
                )
            )
            source_statement = (
                select(func.coalesce(func.max(MessageRecord.sequence), 0))
                .join(
                    RunRecord,
                    (RunRecord.tenant_id == MessageRecord.tenant_id)
                    & (RunRecord.id == MessageRecord.run_id),
                )
                .where(
                    MessageRecord.tenant_id == tenant_id,
                    MessageRecord.session_id == session_id,
                    MessageRecord.execution_epoch == RunRecord.execution_epoch,
                )
            )
            if after_message_sequence is not None:
                count_statement = count_statement.where(
                    MessageRecord.sequence > after_message_sequence
                )
                source_statement = source_statement.where(
                    MessageRecord.sequence > after_message_sequence
                )
            uncompacted_count = int(await database.scalar(count_statement) or 0)
            if uncompacted_count < threshold_messages:
                return None
            source_sequence = int(await database.scalar(source_statement) or 0)
            idempotency_key = f"auto-context-{session.context_generation}-{source_sequence}"
            existing = await self._by_idempotency_key(
                database,
                tenant_id,
                session_id,
                idempotency_key,
            )
            if existing is not None:
                current = _validate_compaction_replay(existing, route_name=route_name)
                return current if current.status is ContextCompactionStatus.PENDING else None
            compaction_id = uuid.uuid5(session_id, idempotency_key)
            await database.execute(
                insert(ContextCompactionRecord)
                .values(
                    id=compaction_id,
                    tenant_id=tenant_id,
                    session_id=session_id,
                    context_generation=session.context_generation,
                    status=ContextCompactionStatus.PENDING.value,
                    idempotency_key=idempotency_key,
                    source_message_sequence=source_sequence,
                    route_name=route_name,
                    requested_at=requested,
                )
                .on_conflict_do_nothing(
                    index_elements=(
                        ContextCompactionRecord.tenant_id,
                        ContextCompactionRecord.session_id,
                        ContextCompactionRecord.idempotency_key,
                    )
                )
            )
            row = await self._by_idempotency_key(
                database,
                tenant_id,
                session_id,
                idempotency_key,
            )
            if row is None:
                raise _state_conflict("proactive compaction disappeared after insertion")
            return _validate_compaction_replay(row, route_name=route_name)

    async def pending_for_session(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> PersistedContextCompaction | None:
        async with self._sessions() as database:
            row = await database.scalar(
                select(ContextCompactionRecord)
                .join(
                    SessionRecord,
                    (SessionRecord.tenant_id == ContextCompactionRecord.tenant_id)
                    & (SessionRecord.id == ContextCompactionRecord.session_id),
                )
                .where(
                    ContextCompactionRecord.tenant_id == tenant_id,
                    ContextCompactionRecord.session_id == session_id,
                    ContextCompactionRecord.context_generation == SessionRecord.context_generation,
                    ContextCompactionRecord.status == ContextCompactionStatus.PENDING.value,
                )
                .order_by(ContextCompactionRecord.requested_at, ContextCompactionRecord.id)
                .limit(1)
            )
        return _compaction_domain(row) if row is not None else None

    async def get(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        compaction_id: uuid.UUID,
    ) -> PersistedContextCompaction | None:
        """Read one compaction without permitting cross-tenant/session discovery."""

        async with self._sessions() as database:
            row = await database.scalar(
                select(ContextCompactionRecord).where(
                    ContextCompactionRecord.tenant_id == tenant_id,
                    ContextCompactionRecord.session_id == session_id,
                    ContextCompactionRecord.id == compaction_id,
                )
            )
        return _compaction_domain(row) if row is not None else None

    async def latest_completed(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> PersistedContextCompaction | None:
        async with self._sessions() as database:
            row = await database.scalar(
                select(ContextCompactionRecord)
                .join(
                    SessionRecord,
                    (SessionRecord.tenant_id == ContextCompactionRecord.tenant_id)
                    & (SessionRecord.id == ContextCompactionRecord.session_id),
                )
                .where(
                    ContextCompactionRecord.tenant_id == tenant_id,
                    ContextCompactionRecord.session_id == session_id,
                    ContextCompactionRecord.context_generation == SessionRecord.context_generation,
                    ContextCompactionRecord.status == ContextCompactionStatus.COMPLETED.value,
                )
                .order_by(ContextCompactionRecord.completed_at.desc())
                .limit(1)
            )
        return _compaction_domain(row) if row is not None else None

    async def complete(
        self,
        tenant_id: uuid.UUID,
        compaction_id: uuid.UUID,
        *,
        summary: str,
        input_tokens: int,
        output_tokens: int,
        completed_at: datetime,
    ) -> PersistedContextCompaction | None:
        return await self._finish(
            tenant_id,
            compaction_id,
            summary=summary,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            error=None,
            completed_at=completed_at,
        )

    async def fail(
        self,
        tenant_id: uuid.UUID,
        compaction_id: uuid.UUID,
        *,
        error: ErrorDetail,
        completed_at: datetime,
    ) -> PersistedContextCompaction | None:
        return await self._finish(
            tenant_id,
            compaction_id,
            summary=None,
            input_tokens=None,
            output_tokens=None,
            error=error,
            completed_at=completed_at,
        )

    async def _finish(
        self,
        tenant_id: uuid.UUID,
        compaction_id: uuid.UUID,
        *,
        summary: str | None,
        input_tokens: int | None,
        output_tokens: int | None,
        error: ErrorDetail | None,
        completed_at: datetime,
    ) -> PersistedContextCompaction | None:
        requested_status = (
            ContextCompactionStatus.FAILED
            if error is not None
            else ContextCompactionStatus.COMPLETED
        )
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(ContextCompactionRecord)
                .where(
                    ContextCompactionRecord.tenant_id == tenant_id,
                    ContextCompactionRecord.id == compaction_id,
                )
                .with_for_update()
            )
            if row is None:
                return None
            if row.status != ContextCompactionStatus.PENDING.value:
                current = _compaction_domain(row)
                if (
                    current.status is requested_status
                    and current.summary == summary
                    and current.input_tokens == input_tokens
                    and current.output_tokens == output_tokens
                    and current.error
                    == (FrozenJsonObject(error.model_dump(mode="json")) if error else None)
                ):
                    return current
                raise DomainOperationError(
                    code="context_compaction_conflict",
                    message="the compaction already has a different terminal outcome",
                )
            row.status = requested_status.value
            row.summary = summary
            row.input_tokens = input_tokens
            row.output_tokens = output_tokens
            row.error = error.model_dump(mode="json") if error else None
            row.completed_at = completed_at
            result = _compaction_domain(row)
        return result

    @staticmethod
    async def _by_idempotency_key(
        database: AsyncSession,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        idempotency_key: str,
    ) -> ContextCompactionRecord | None:
        return cast(
            "ContextCompactionRecord | None",
            await database.scalar(
                select(ContextCompactionRecord).where(
                    ContextCompactionRecord.tenant_id == tenant_id,
                    ContextCompactionRecord.session_id == session_id,
                    ContextCompactionRecord.idempotency_key == idempotency_key,
                )
            ),
        )


class PostgresTaskRepository:
    """Versioned task plans with run-row serialization and optimistic CAS."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def get(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
    ) -> PersistedTaskState | None:
        async with self._sessions() as database:
            row = await database.scalar(
                select(TaskPlanRecord)
                .join(
                    RunRecord,
                    (RunRecord.tenant_id == TaskPlanRecord.tenant_id)
                    & (RunRecord.id == TaskPlanRecord.run_id),
                )
                .where(
                    TaskPlanRecord.tenant_id == tenant_id,
                    TaskPlanRecord.run_id == run_id,
                    TaskPlanRecord.execution_epoch == RunRecord.execution_epoch,
                )
                .order_by(TaskPlanRecord.version.desc())
                .limit(1)
            )
        return _task_state_domain(row) if row is not None else None

    async def update(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
        update: TaskPlanUpdate,
        *,
        plan_id: uuid.UUID,
        created_at: datetime,
    ) -> PersistedTaskState | None:
        async with self._sessions() as database, database.begin():
            run_record = await database.scalar(
                select(RunRecord)
                .where(RunRecord.tenant_id == tenant_id, RunRecord.id == run_id)
                .with_for_update()
            )
            if run_record is None:
                return None
            row = await database.scalar(
                select(TaskPlanRecord)
                .where(
                    TaskPlanRecord.tenant_id == tenant_id,
                    TaskPlanRecord.run_id == run_id,
                    TaskPlanRecord.execution_epoch == run_record.execution_epoch,
                )
                .order_by(TaskPlanRecord.version.desc())
                .limit(1)
            )
            current_version = row.version if row is not None else 0
            if current_version != update.expected_version:
                raise DomainOperationError(
                    code="task_plan_version_conflict",
                    message="the task plan changed since it was read",
                    details={
                        "expected_version": update.expected_version,
                        "current_version": current_version,
                    },
                )
            state = PersistedTaskState(
                id=plan_id,
                run_id=run_id,
                execution_epoch=run_record.execution_epoch,
                version=current_version + 1,
                tasks=update.tasks,
                created_at=created_at,
            )
            database.add(
                TaskPlanRecord(
                    id=state.id,
                    tenant_id=tenant_id,
                    run_id=run_id,
                    execution_epoch=state.execution_epoch,
                    version=state.version,
                    plan={"tasks": [task.model_dump(mode="json") for task in state.tasks]},
                    created_at=state.created_at,
                )
            )
        return state


class PostgresMemoryRepository:
    """Provenance-preserving memory storage and asynchronous extraction queue."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        token_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        if not callable(token_factory):
            raise TypeError("memory lease token factory must be callable")
        self._sessions = sessions
        self._token_factory = token_factory

    async def set_session_enabled(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        enabled: bool,
        updated_at: datetime,
    ) -> bool:
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(SessionRecord)
                .where(
                    SessionRecord.tenant_id == tenant_id,
                    SessionRecord.id == session_id,
                )
                .with_for_update()
            )
            if row is None:
                return False
            row.memory_enabled = enabled
            row.updated_at = updated_at
        return True

    async def list_active(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        limit: int = 100,
    ) -> tuple[PersistedMemory, ...]:
        if type(limit) is not int or not 1 <= limit <= MAX_MEMORY_RESULTS:
            raise ValueError("memory result limit must be in [1, 500]")
        async with self._sessions() as database:
            enabled = await _memory_enabled(database, tenant_id, session_id)
            if not enabled:
                return ()
            rows = await database.scalars(
                select(MemoryRecord)
                .join(
                    RunRecord,
                    (RunRecord.tenant_id == MemoryRecord.tenant_id)
                    & (RunRecord.id == MemoryRecord.source_run_id),
                )
                .where(
                    MemoryRecord.tenant_id == tenant_id,
                    MemoryRecord.session_id == session_id,
                    MemoryRecord.archived_at.is_(None),
                    MemoryRecord.execution_epoch == RunRecord.execution_epoch,
                )
                .order_by(MemoryRecord.extracted_at.desc(), MemoryRecord.id)
                .limit(limit)
            )
            return tuple(_memory_domain(row) for row in rows)

    async def archive(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        memory_id: uuid.UUID,
        *,
        archived_at: datetime,
    ) -> PersistedMemory | None:
        """Soft-delete one active memory through a tenant/session scoped mutation."""

        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(MemoryRecord)
                .where(
                    MemoryRecord.tenant_id == tenant_id,
                    MemoryRecord.session_id == session_id,
                    MemoryRecord.id == memory_id,
                    MemoryRecord.archived_at.is_(None),
                )
                .with_for_update()
            )
            if row is None:
                return None
            row.archived_at = archived_at
            return _memory_domain(row)

    async def claim_pending(
        self,
        *,
        worker_id: str,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> MemoryExtractionJob | None:
        if (
            type(worker_id) is not str
            or not worker_id.strip()
            or len(worker_id) > MAX_MEMORY_WORKER_ID_LENGTH
        ):
            raise ValueError("memory extraction worker ID is invalid")
        if (
            not isinstance(lease_duration, timedelta)
            or not 0 < lease_duration.total_seconds() <= MAX_MEMORY_LEASE_SECONDS
        ):
            raise ValueError("memory extraction lease duration must be in (0, 3600] seconds")
        async with self._sessions() as database, database.begin():
            row = await database.scalar(
                select(MemoryExtractionJobRecord)
                .join(
                    RunRecord,
                    (RunRecord.tenant_id == MemoryExtractionJobRecord.tenant_id)
                    & (RunRecord.id == MemoryExtractionJobRecord.run_id),
                )
                .where(
                    MemoryExtractionJobRecord.execution_epoch == RunRecord.execution_epoch,
                    or_(
                        MemoryExtractionJobRecord.status == MemoryExtractionStatus.PENDING.value,
                        (
                            (
                                MemoryExtractionJobRecord.status
                                == MemoryExtractionStatus.RUNNING.value
                            )
                            & (MemoryExtractionJobRecord.lease_expires_at <= occurred_at)
                        ),
                    ),
                )
                .order_by(
                    func.coalesce(
                        MemoryExtractionJobRecord.lease_expires_at,
                        MemoryExtractionJobRecord.created_at,
                    ),
                    MemoryExtractionJobRecord.id,
                )
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if row is None:
                return None
            recovering = row.status == MemoryExtractionStatus.RUNNING.value
            if recovering and row.attempt >= MAX_MEMORY_JOB_ATTEMPTS:
                row.status = MemoryExtractionStatus.FAILED.value
                row.error = ErrorDetail(
                    code="memory_extraction_retry_exhausted",
                    message="memory extraction exhausted its retry limit",
                    retryable=False,
                ).model_dump(mode="json")
                row.completed_at = occurred_at
                _clear_memory_job_lease(row)
                return _memory_job_domain(row)
            if not await _memory_enabled(database, row.tenant_id, row.session_id):
                row.status = MemoryExtractionStatus.COMPLETED.value
                row.started_at = row.started_at or occurred_at
                row.completed_at = occurred_at
                _clear_memory_job_lease(row)
                return _memory_job_domain(row)
            token = self._token_factory()
            if not isinstance(token, uuid.UUID) or token.int == 0:
                raise TypeError("memory lease token factory returned an invalid UUID")
            row.status = MemoryExtractionStatus.RUNNING.value
            row.started_at = occurred_at
            row.attempt += int(recovering)
            row.worker_id = worker_id
            row.lease_token = token
            row.lease_generation += 1
            row.lease_expires_at = occurred_at + lease_duration
            return _memory_job_domain(row)

    async def source_for_job(
        self,
        job: MemoryExtractionJob,
        *,
        max_bytes: int,
        occurred_at: datetime,
    ) -> str:
        if type(max_bytes) is not int or not 1 <= max_bytes <= 4 * 1024 * 1024:
            raise ValueError("memory extraction source limit is invalid")
        timestamp = normalize_timestamp(occurred_at)
        async with self._sessions() as database:
            row = await database.scalar(
                select(MemoryExtractionJobRecord).where(
                    MemoryExtractionJobRecord.tenant_id == job.tenant_id,
                    MemoryExtractionJobRecord.id == job.id,
                )
            )
            if row is None or row.status != MemoryExtractionStatus.RUNNING.value:
                raise _memory_lease_lost()
            _assert_memory_job_lease(row, job, occurred_at=timestamp)
            result = await database.stream_scalars(
                select(MessageRecord)
                .where(
                    MessageRecord.tenant_id == job.tenant_id,
                    MessageRecord.session_id == job.session_id,
                    MessageRecord.run_id == job.run_id,
                    MessageRecord.execution_epoch == job.execution_epoch,
                    MessageRecord.sequence <= job.source_message_sequence,
                )
                .order_by(MessageRecord.sequence.desc())
                .limit(MAX_MEMORY_SOURCE_MESSAGES)
                .execution_options(yield_per=MEMORY_SOURCE_BATCH_SIZE)
            )
            selected: list[str] = []
            retained_bytes = 0
            try:
                async for message in result:
                    rendered = f"[{message.role}] {message.content}"
                    rendered_bytes = len(rendered.encode("utf-8")) + (1 if selected else 0)
                    if retained_bytes + rendered_bytes > max_bytes:
                        if selected:
                            break
                        rendered = _utf8_tail(rendered, max_bytes)
                        rendered_bytes = len(rendered.encode("utf-8"))
                    selected.append(rendered)
                    retained_bytes += rendered_bytes
            finally:
                await result.close()
            selected.reverse()
            return "\n".join(selected)

    async def complete(
        self,
        job: MemoryExtractionJob,
        memories: Sequence[PersistedMemory],
        *,
        completed_at: datetime,
    ) -> MemoryExtractionJob:
        if job.status is not MemoryExtractionStatus.RUNNING:
            raise ValueError("only a running memory extraction job can complete")
        if len(memories) > MAX_MEMORIES_PER_EXTRACTION:
            raise ValueError("one memory extraction job may produce at most 100 memories")
        async with self._sessions() as database, database.begin():
            row = await self._locked_job(database, job)
            if row.status == MemoryExtractionStatus.COMPLETED.value:
                _assert_terminal_generation(row, job)
                return _memory_job_domain(row)
            if row.status != MemoryExtractionStatus.RUNNING.value:
                raise _memory_lease_lost()
            _assert_memory_job_lease(row, job, occurred_at=completed_at)
            if await _memory_enabled(database, row.tenant_id, job.session_id):
                for memory in memories:
                    if (
                        memory.tenant_id != job.tenant_id
                        or memory.session_id != job.session_id
                        or memory.source_run_id != job.run_id
                        or memory.execution_epoch != job.execution_epoch
                    ):
                        raise ValueError("memory provenance does not match its extraction job")
                    await database.execute(
                        insert(MemoryRecord)
                        .values(
                            id=memory.id,
                            tenant_id=row.tenant_id,
                            session_id=memory.session_id,
                            source_run_id=memory.source_run_id,
                            execution_epoch=memory.execution_epoch,
                            kind=memory.kind.value,
                            content=memory.content,
                            content_hash=memory.content_hash,
                            memory_metadata=memory.metadata.to_json_object(),
                            extracted_at=memory.extracted_at,
                            archived_at=memory.archived_at,
                        )
                        .on_conflict_do_nothing(
                            index_elements=(
                                MemoryRecord.tenant_id,
                                MemoryRecord.session_id,
                                MemoryRecord.execution_epoch,
                                MemoryRecord.kind,
                                MemoryRecord.content_hash,
                            )
                        )
                    )
            row.status = MemoryExtractionStatus.COMPLETED.value
            row.completed_at = completed_at
            _clear_memory_job_lease(row)
            return _memory_job_domain(row)

    async def fail(
        self,
        job: MemoryExtractionJob,
        *,
        error: ErrorDetail,
        completed_at: datetime,
    ) -> MemoryExtractionJob:
        async with self._sessions() as database, database.begin():
            row = await self._locked_job(database, job)
            if row.status == MemoryExtractionStatus.FAILED.value:
                _assert_terminal_generation(row, job)
                return _memory_job_domain(row)
            if row.status != MemoryExtractionStatus.RUNNING.value:
                raise _memory_lease_lost()
            _assert_memory_job_lease(row, job, occurred_at=completed_at)
            row.status = MemoryExtractionStatus.FAILED.value
            row.error = error.model_dump(mode="json")
            row.completed_at = completed_at
            _clear_memory_job_lease(row)
            return _memory_job_domain(row)

    @staticmethod
    async def _locked_job(
        database: AsyncSession,
        job: MemoryExtractionJob,
    ) -> MemoryExtractionJobRecord:
        row = await database.scalar(
            select(MemoryExtractionJobRecord)
            .where(
                MemoryExtractionJobRecord.id == job.id,
                MemoryExtractionJobRecord.tenant_id == job.tenant_id,
                MemoryExtractionJobRecord.session_id == job.session_id,
                MemoryExtractionJobRecord.run_id == job.run_id,
                MemoryExtractionJobRecord.execution_epoch == job.execution_epoch,
                MemoryExtractionJobRecord.source_message_sequence == job.source_message_sequence,
            )
            .with_for_update()
        )
        if row is None:
            raise _state_conflict("memory extraction job no longer exists")
        return row


async def _memory_enabled(
    database: AsyncSession,
    tenant_id: uuid.UUID,
    session_id: uuid.UUID,
) -> bool:
    session_enabled = await database.scalar(
        select(SessionRecord.memory_enabled).where(
            SessionRecord.tenant_id == tenant_id,
            SessionRecord.id == session_id,
        )
    )
    if session_enabled is not True:
        return False
    tenant_enabled = await database.scalar(
        select(TenantQuotaRecord.memory_enabled).where(TenantQuotaRecord.tenant_id == tenant_id)
    )
    return tenant_enabled is not False


def _validate_compaction_replay(
    record: ContextCompactionRecord,
    *,
    route_name: str,
) -> PersistedContextCompaction:
    if record.route_name != route_name:
        raise DomainOperationError(
            code="context_compaction_idempotency_conflict",
            message="the idempotency key belongs to another compaction request",
        )
    return _compaction_domain(record)


def _compaction_domain(record: ContextCompactionRecord) -> PersistedContextCompaction:
    return PersistedContextCompaction(
        id=record.id,
        session_id=record.session_id,
        context_generation=record.context_generation,
        status=ContextCompactionStatus(record.status),
        idempotency_key=record.idempotency_key,
        source_message_sequence=record.source_message_sequence,
        route_name=record.route_name,
        summary=record.summary,
        input_tokens=record.input_tokens,
        output_tokens=record.output_tokens,
        error=record.error,
        requested_at=record.requested_at,
        completed_at=record.completed_at,
    )


def _task_state_domain(record: TaskPlanRecord) -> PersistedTaskState:
    raw_tasks = record.plan.get("tasks")
    if raw_tasks is None:
        raw_tasks = _legacy_tasks(record.plan)
    elif not isinstance(raw_tasks, list):
        raise _state_conflict("durable task plan contains a malformed task list")
    try:
        tasks = tuple(TrackedTask.model_validate(item) for item in raw_tasks)
        tasks = TaskPlanUpdate(expected_version=0, tasks=tasks).tasks
    except (TypeError, ValueError) as error:
        raise _state_conflict("durable task plan contains invalid task state") from error
    return PersistedTaskState(
        id=record.id,
        run_id=record.run_id,
        execution_epoch=record.execution_epoch,
        version=record.version,
        tasks=tasks,
        created_at=record.created_at,
    )


def _memory_domain(record: MemoryRecord) -> PersistedMemory:
    return PersistedMemory(
        id=record.id,
        tenant_id=record.tenant_id,
        session_id=record.session_id,
        source_run_id=record.source_run_id,
        execution_epoch=record.execution_epoch,
        kind=MemoryKind(record.kind),
        content=record.content,
        content_hash=record.content_hash,
        metadata=record.memory_metadata,
        extracted_at=record.extracted_at,
        archived_at=record.archived_at,
    )


def _memory_job_domain(record: MemoryExtractionJobRecord) -> MemoryExtractionJob:
    return MemoryExtractionJob(
        id=record.id,
        tenant_id=record.tenant_id,
        session_id=record.session_id,
        run_id=record.run_id,
        execution_epoch=record.execution_epoch,
        status=MemoryExtractionStatus(record.status),
        source_message_sequence=record.source_message_sequence,
        attempt=record.attempt,
        worker_id=record.worker_id,
        lease_token=record.lease_token,
        lease_generation=record.lease_generation,
        lease_expires_at=record.lease_expires_at,
        error=record.error,
        created_at=record.created_at,
        started_at=record.started_at,
        completed_at=record.completed_at,
    )


def _state_conflict(message: str) -> DomainOperationError:
    return DomainOperationError(
        code="persistence_state_conflict",
        message=message,
        retryable=True,
    )


def _legacy_tasks(plan: dict[str, object]) -> list[dict[str, object]]:
    """Expose pre-Sequence-24 task plans without rewriting durable history."""

    steps = plan.get("steps")
    if isinstance(steps, list):
        converted: list[dict[str, object]] = []
        for index, step in enumerate(steps, start=1):
            if isinstance(step, dict):
                raw_title = step.get("title", step.get("text"))
                title = raw_title.strip() if isinstance(raw_title, str) else f"Legacy step {index}"
                raw_status = step.get("status")
                status = raw_status if raw_status in {item.value for item in TaskStatus} else None
                if status is None:
                    status = (
                        TaskStatus.COMPLETED.value
                        if step.get("done") is True
                        else TaskStatus.PENDING.value
                    )
                details = json.dumps(
                    step,
                    allow_nan=False,
                    ensure_ascii=False,
                    sort_keys=True,
                )[:16_384]
            else:
                title = str(step).strip() or f"Legacy step {index}"
                status = TaskStatus.PENDING.value
                details = ""
            converted.append(
                {
                    "id": f"legacy-step-{index}",
                    "title": title[:1000],
                    "status": status,
                    "details": details,
                }
            )
        return converted
    details = json.dumps(plan, allow_nan=False, ensure_ascii=False, sort_keys=True)[:16_384]
    return [
        {
            "id": "legacy-plan",
            "title": "Legacy task plan",
            "status": TaskStatus.PENDING.value,
            "details": details,
        }
    ]


def _assert_memory_job_lease(
    record: MemoryExtractionJobRecord,
    job: MemoryExtractionJob,
    *,
    occurred_at: datetime | None = None,
) -> None:
    if (
        record.worker_id != job.worker_id
        or record.lease_token != job.lease_token
        or record.lease_generation != job.lease_generation
        or record.lease_expires_at != job.lease_expires_at
        or record.lease_token is None
        or record.lease_expires_at is None
        or (occurred_at is not None and occurred_at >= record.lease_expires_at)
    ):
        raise _memory_lease_lost()


def _assert_terminal_generation(
    record: MemoryExtractionJobRecord,
    job: MemoryExtractionJob,
) -> None:
    if record.lease_generation != job.lease_generation:
        raise _memory_lease_lost()


def _clear_memory_job_lease(record: MemoryExtractionJobRecord) -> None:
    record.worker_id = None
    record.lease_token = None
    record.lease_expires_at = None


def _memory_lease_lost() -> DomainOperationError:
    return DomainOperationError(
        code="memory_extraction_lease_lost",
        message="the memory extraction lease is no longer active",
        retryable=True,
    )


def _utf8_tail(value: str, limit: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    retained = encoded[-limit:]
    return retained.decode("utf-8", errors="ignore")


__all__ = [
    "PostgresContextRepository",
    "PostgresMemoryRepository",
    "PostgresTaskRepository",
]
