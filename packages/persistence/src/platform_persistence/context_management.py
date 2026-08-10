"""Durable context compaction adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from agent_core.control import (
    ContextCompactionStatus,
    IdempotencyKey,
    PersistedContextCompaction,
)
from agent_core.domain.base import FrozenJsonObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from platform_persistence.models import (
    ContextCompactionRecord,
    MessageRecord,
    SessionRecord,
)

if TYPE_CHECKING:
    import uuid
    from datetime import datetime

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class PostgresContextRepository:
    """Tenant-scoped non-destructive transcript compaction state."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

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
                    select(func.coalesce(func.max(MessageRecord.sequence), 0)).where(
                        MessageRecord.tenant_id == tenant_id,
                        MessageRecord.session_id == session_id,
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

    async def pending_for_session(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> PersistedContextCompaction | None:
        async with self._sessions() as database:
            row = await database.scalar(
                select(ContextCompactionRecord)
                .where(
                    ContextCompactionRecord.tenant_id == tenant_id,
                    ContextCompactionRecord.session_id == session_id,
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
                .where(
                    ContextCompactionRecord.tenant_id == tenant_id,
                    ContextCompactionRecord.session_id == session_id,
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


def _state_conflict(message: str) -> DomainOperationError:
    return DomainOperationError(
        code="persistence_state_conflict",
        message=message,
        retryable=True,
    )


__all__ = ["PostgresContextRepository"]
