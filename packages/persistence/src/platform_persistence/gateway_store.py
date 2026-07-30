"""PostgreSQL adapter for tenant-scoped gateway request idempotency."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert

from agent_core.domain.errors import ErrorDetail
from agent_core.gateway import GatewayEvent, parse_gateway_event
from agent_core.gateway_reliability import (
    GatewayRequestClaim,
    GatewayRequestClaimStatus,
)
from platform_persistence.models import GatewayRequestRecord

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from agent_core.domain.models import Sha256Hex
    from agent_core.gateway import GatewayRequestIdentifier


class PostgresGatewayRequestStore:
    """Durable compare-and-set store for logical model request results."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def claim(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
    ) -> GatewayRequestClaim:
        async with self._sessions() as database, database.begin():
            inserted = await database.scalar(
                insert(GatewayRequestRecord)
                .values(
                    tenant_id=tenant_id,
                    request_id=request_id,
                    request_hash=request_hash,
                    status=GatewayRequestClaimStatus.IN_PROGRESS.value,
                    events=[],
                )
                .on_conflict_do_nothing(
                    index_elements=(
                        GatewayRequestRecord.tenant_id,
                        GatewayRequestRecord.request_id,
                    )
                )
                .returning(GatewayRequestRecord.request_id)
            )
            if inserted is not None:
                return GatewayRequestClaim(
                    status=GatewayRequestClaimStatus.EXECUTE,
                    request_hash=request_hash,
                )
            row = await database.scalar(
                select(GatewayRequestRecord).where(
                    GatewayRequestRecord.tenant_id == tenant_id,
                    GatewayRequestRecord.request_id == request_id,
                )
            )
            if row is None:
                raise RuntimeError("gateway request conflict row disappeared")
            if row.request_hash != request_hash:
                return GatewayRequestClaim(
                    status=GatewayRequestClaimStatus.CONFLICT,
                    request_hash=row.request_hash,
                )
            return _claim_from_record(row)

    async def complete(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        events: tuple[GatewayEvent, ...],
    ) -> None:
        validated = GatewayRequestClaim(
            status=GatewayRequestClaimStatus.COMPLETED,
            request_hash=request_hash,
            events=events,
        )
        serialized = [event.model_dump(mode="json") for event in validated.events]
        async with self._sessions() as database, database.begin():
            updated = await database.scalar(
                update(GatewayRequestRecord)
                .where(
                    GatewayRequestRecord.tenant_id == tenant_id,
                    GatewayRequestRecord.request_id == request_id,
                    GatewayRequestRecord.request_hash == request_hash,
                    GatewayRequestRecord.status == GatewayRequestClaimStatus.IN_PROGRESS.value,
                )
                .values(
                    status=GatewayRequestClaimStatus.COMPLETED.value,
                    events=serialized,
                    error=None,
                    updated_at=datetime.now(UTC),
                )
                .returning(GatewayRequestRecord.request_id)
            )
            if updated is None:
                raise RuntimeError("gateway request completion lost its claim")

    async def fail(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
        error: ErrorDetail,
    ) -> None:
        async with self._sessions() as database, database.begin():
            updated = await database.scalar(
                update(GatewayRequestRecord)
                .where(
                    GatewayRequestRecord.tenant_id == tenant_id,
                    GatewayRequestRecord.request_id == request_id,
                    GatewayRequestRecord.request_hash == request_hash,
                    GatewayRequestRecord.status == GatewayRequestClaimStatus.IN_PROGRESS.value,
                )
                .values(
                    status=GatewayRequestClaimStatus.FAILED.value,
                    error=error.model_dump(mode="json"),
                    updated_at=datetime.now(UTC),
                )
                .returning(GatewayRequestRecord.request_id)
            )
            if updated is None:
                raise RuntimeError("gateway request failure lost its claim")

    async def release(
        self,
        tenant_id: uuid.UUID,
        request_id: GatewayRequestIdentifier,
        request_hash: Sha256Hex,
    ) -> None:
        async with self._sessions() as database, database.begin():
            released = await database.scalar(
                delete(GatewayRequestRecord)
                .where(
                    GatewayRequestRecord.tenant_id == tenant_id,
                    GatewayRequestRecord.request_id == request_id,
                    GatewayRequestRecord.request_hash == request_hash,
                    GatewayRequestRecord.status == GatewayRequestClaimStatus.IN_PROGRESS.value,
                )
                .returning(GatewayRequestRecord.request_id)
            )
            if released is None:
                raise RuntimeError("gateway request release lost its claim")


def _claim_from_record(record: GatewayRequestRecord) -> GatewayRequestClaim:
    status = GatewayRequestClaimStatus(record.status)
    events = tuple(parse_gateway_event(value) for value in record.events)
    error = ErrorDetail.model_validate(record.error) if record.error is not None else None
    return GatewayRequestClaim(
        status=status,
        request_hash=record.request_hash,
        events=events,
        error=error,
    )


__all__ = ["PostgresGatewayRequestStore"]
