"""Shared PostgreSQL validation for worker-owned run-lease writes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import func, select

from agent_core.domain.errors import DomainOperationError
from agent_core.domain.status import RunStatus
from platform_persistence.models import RunLeaseRecord, RunRecord

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from agent_core.distributed import RunLease


async def assert_active_run_lease(
    database: AsyncSession,
    lease: RunLease,
) -> RunRecord:
    """Lock and validate the exact unexpired lease and mirrored run ownership."""

    lease_row = await database.scalar(
        select(RunLeaseRecord)
        .where(
            RunLeaseRecord.tenant_id == lease.tenant_id,
            RunLeaseRecord.run_id == lease.run_id,
            RunLeaseRecord.worker_id == lease.worker_id,
            RunLeaseRecord.lease_token == lease.lease_token,
            RunLeaseRecord.generation == lease.generation,
            RunLeaseRecord.expires_at > func.clock_timestamp(),
        )
        .with_for_update()
    )
    if lease_row is None:
        raise _lease_lost(lease)

    run_row = await database.scalar(
        select(RunRecord)
        .where(
            RunRecord.tenant_id == lease.tenant_id,
            RunRecord.id == lease.run_id,
            RunRecord.session_id == lease.session_id,
            RunRecord.workspace_id == lease.workspace_id,
            RunRecord.execution_epoch == lease.execution_epoch,
            RunRecord.assigned_worker_id == lease.worker_id,
            RunRecord.lease_generation == lease.generation,
            RunRecord.lease_expires_at > func.clock_timestamp(),
            RunRecord.status.in_((RunStatus.LEASED.value, RunStatus.RUNNING.value)),
        )
        .with_for_update()
    )
    if run_row is None:
        raise _lease_lost(lease)
    return run_row


def _lease_lost(lease: RunLease) -> DomainOperationError:
    return DomainOperationError(
        code="run_lease_lost",
        message="the run lease is no longer active for this worker",
        retryable=True,
        details={"run_id": str(lease.run_id)},
    )


__all__ = ["assert_active_run_lease"]
