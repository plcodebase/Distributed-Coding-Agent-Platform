"""Durable worker composition for bounded context builds and explicit compaction."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from agent_core.domain.errors import DomainOperationError, ErrorDetail

if TYPE_CHECKING:
    import uuid
    from datetime import datetime

    from agent_core.context import ContextBuildRequest, ContextBuildResult, ContextPipeline
    from agent_core.control import PersistedContextCompaction
    from agent_core.distributed import RunLease, RunRecoveryState, WorkspaceWriterLease
    from agent_core.loop import Clock


class RunContextDataSource(Protocol):
    """Load already-contained context sources, optionally through a message watermark."""

    async def load(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
        *,
        source_message_sequence: int | None,
        previous_summary: str | None,
        force_compaction: bool,
    ) -> ContextBuildRequest: ...


class ContextCompactionStore(Protocol):
    """Durable explicit-compaction state used by the worker."""

    async def pending_for_session(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> PersistedContextCompaction | None: ...

    async def latest_completed(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
    ) -> PersistedContextCompaction | None: ...

    async def complete(
        self,
        tenant_id: uuid.UUID,
        compaction_id: uuid.UUID,
        *,
        summary: str,
        input_tokens: int,
        output_tokens: int,
        completed_at: datetime,
    ) -> PersistedContextCompaction | None: ...

    async def fail(
        self,
        tenant_id: uuid.UUID,
        compaction_id: uuid.UUID,
        *,
        error: ErrorDetail,
        completed_at: datetime,
    ) -> PersistedContextCompaction | None: ...


class DurableRunContextBuilder:
    """Join durable compaction requests with the composable context pipeline."""

    def __init__(
        self,
        *,
        pipeline: ContextPipeline,
        source: RunContextDataSource,
        compactions: ContextCompactionStore,
        clock: Clock,
    ) -> None:
        self._pipeline = pipeline
        self._source = source
        self._compactions = compactions
        self._clock = clock

    async def build(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> ContextBuildResult:
        pending = await self._compactions.pending_for_session(
            lease.tenant_id,
            lease.session_id,
        )
        latest = await self._compactions.latest_completed(
            lease.tenant_id,
            lease.session_id,
        )
        request = await self._source.load(
            lease,
            writer_lease,
            recovery,
            source_message_sequence=(
                pending.source_message_sequence if pending is not None else None
            ),
            previous_summary=(latest.summary if latest is not None else recovery.context_summary),
            force_compaction=pending is not None,
        )
        if (
            request.tenant_id != lease.tenant_id
            or request.session_id != lease.session_id
            or request.run_id != lease.run_id
            or request.route_name != lease.route_name
        ):
            raise DomainOperationError(
                code="context_source_mismatch",
                message="the context source does not match the active run lease",
                retryable=True,
            )
        try:
            result = await self._pipeline.build(request)
        except Exception:
            if pending is not None:
                await self._record_failure(lease.tenant_id, pending.id)
            raise
        if pending is not None:
            if (
                result.summary is None
                or result.compression_input_tokens is None
                or result.compression_output_tokens is None
            ):
                await self._compactions.fail(
                    lease.tenant_id,
                    pending.id,
                    error=ErrorDetail(
                        code="context_compaction_empty",
                        message="no noncritical context was available to compact",
                    ),
                    completed_at=self._clock.now(),
                )
            else:
                completed = await self._compactions.complete(
                    lease.tenant_id,
                    pending.id,
                    summary=result.summary,
                    input_tokens=result.compression_input_tokens,
                    output_tokens=result.compression_output_tokens,
                    completed_at=self._clock.now(),
                )
                if completed is None:
                    raise DomainOperationError(
                        code="context_compaction_missing",
                        message="the pending compaction request disappeared",
                        retryable=True,
                    )
        return result

    async def _record_failure(self, tenant_id: uuid.UUID, compaction_id: uuid.UUID) -> None:
        await self._compactions.fail(
            tenant_id,
            compaction_id,
            error=ErrorDetail(
                code="context_compaction_failed",
                message="context compaction could not be completed",
                retryable=True,
            ),
            completed_at=self._clock.now(),
        )


__all__ = [
    "ContextCompactionStore",
    "DurableRunContextBuilder",
    "RunContextDataSource",
]
