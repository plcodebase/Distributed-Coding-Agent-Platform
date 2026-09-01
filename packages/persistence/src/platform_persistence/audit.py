"""PostgreSQL append-only administrative audit adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

from platform_persistence.models import AuditRecord

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from agent_core.audit import AuditEntry


class PostgresAuditSink:
    """Persist one immutable audit intent in an independent transaction."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def append(self, entry: AuditEntry) -> AuditEntry:
        async with self._sessions() as database, database.begin():
            database.add(
                AuditRecord(
                    id=entry.id,
                    tenant_id=entry.tenant_id,
                    subject=entry.subject,
                    method=entry.method,
                    resource=entry.resource,
                    action=entry.action,
                    request_id=entry.request_id,
                    details=entry.details.to_json_object(),
                    occurred_at=entry.occurred_at,
                )
            )
        return entry


__all__ = ["PostgresAuditSink"]
