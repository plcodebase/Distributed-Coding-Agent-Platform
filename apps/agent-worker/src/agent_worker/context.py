"""Durable worker composition for bounded context builds and explicit compaction."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Protocol

from agent_core.context import (
    ContextBuildRequest,
    ContextMemorySnippet,
    ContextToolResult,
    ReferencedContextFile,
    WorkspaceContextSnapshot,
)
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.gateway import GatewayMessage
from platform_telemetry import Redactor

_MAX_SOURCE_MESSAGES = 4096
_MAX_SOURCE_MEMORIES = 500
_DEFAULT_PROACTIVE_COMPACTION_MESSAGES = 3072
_MAX_RECENT_TOOL_RESULT_BYTES = 64 * 1024

if TYPE_CHECKING:
    import uuid
    from datetime import datetime

    from agent_core.context import ContextBuildResult, ContextPipeline
    from agent_core.control import (
        PersistedContextCompaction,
        PersistedMemory,
        PersistedMessage,
    )
    from agent_core.distributed import (
        DurableToolOutcome,
        RunLease,
        RunRecoveryState,
        WorkspaceWriterLease,
    )
    from agent_core.loop import Clock
    from agent_core.workspace_access import WorkspaceFileReference


class RunContextDataSource(Protocol):
    """Load already-contained context sources, optionally through a message watermark."""

    async def load(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
        *,
        after_message_sequence: int | None,
        through_message_sequence: int | None,
        previous_summary: str | None,
        force_compaction: bool,
    ) -> ContextBuildRequest: ...


class ContextHistoryStore(Protocol):
    async def list_messages(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        after_sequence: int | None,
        through_sequence: int | None,
        limit: int,
    ) -> tuple[PersistedMessage, ...]: ...

    async def referenced_files_for_run(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
    ) -> tuple[WorkspaceFileReference, ...]: ...


class ContextMemoryStore(Protocol):
    async def list_active(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        limit: int,
    ) -> tuple[PersistedMemory, ...]: ...


class WorkspaceContextSource(Protocol):
    """Load a read-only context view through the active workspace capability."""

    async def load_workspace_context(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        referenced_files: tuple[WorkspaceFileReference, ...],
    ) -> WorkspaceContextSnapshot: ...


class PersistentRunContextSource:
    """Load bounded conversation, tool outcomes, and memory from durable adapters."""

    def __init__(
        self,
        *,
        history: ContextHistoryStore,
        memories: ContextMemoryStore,
        system_instructions: str,
        message_limit: int = 4096,
        memory_limit: int = 100,
        redactor: Redactor | None = None,
        workspace_context: WorkspaceContextSource | None = None,
    ) -> None:
        if not system_instructions or len(system_instructions.encode("utf-8")) > 64 * 1024:
            raise ValueError("system instructions must contain at most 64 KiB of UTF-8 text")
        if type(message_limit) is not int or not 1 <= message_limit <= _MAX_SOURCE_MESSAGES:
            raise ValueError("context message limit must be in [1, 4096]")
        if type(memory_limit) is not int or not 1 <= memory_limit <= _MAX_SOURCE_MEMORIES:
            raise ValueError("context memory limit must be in [1, 500]")
        self._history = history
        self._memories = memories
        self._system_instructions = system_instructions
        self._message_limit = message_limit
        self._memory_limit = memory_limit
        self._redactor = redactor or Redactor()
        self._workspace_context = workspace_context

    async def load(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
        *,
        after_message_sequence: int | None,
        through_message_sequence: int | None,
        previous_summary: str | None,
        force_compaction: bool,
    ) -> ContextBuildRequest:
        if (
            writer_lease.tenant_id != lease.tenant_id
            or writer_lease.run_id != lease.run_id
            or writer_lease.workspace_id != lease.workspace_id
            or writer_lease.run_lease_token != lease.lease_token
        ):
            raise DomainOperationError(
                code="context_workspace_lease_mismatch",
                message="the context source does not match the active workspace lease",
                retryable=True,
            )
        durable_messages = await self._history.list_messages(
            lease.tenant_id,
            lease.session_id,
            after_sequence=after_message_sequence,
            through_sequence=through_message_sequence,
            limit=self._message_limit,
        )
        conversation = tuple(_gateway_message(message) for message in durable_messages)
        if not conversation and previous_summary is None:
            conversation = recovery.messages
        memories = await self._memories.list_active(
            lease.tenant_id,
            lease.session_id,
            limit=self._memory_limit,
        )
        references = await self._history.referenced_files_for_run(
            lease.tenant_id,
            lease.run_id,
        )
        if self._workspace_context is None:
            if references:
                raise DomainOperationError(
                    code="context_workspace_source_unavailable",
                    message="explicit file references require an active workspace context source",
                    retryable=True,
                )
            workspace = WorkspaceContextSnapshot()
        else:
            workspace = await self._workspace_context.load_workspace_context(
                lease,
                writer_lease,
                references,
            )
        return ContextBuildRequest(
            tenant_id=lease.tenant_id,
            session_id=lease.session_id,
            run_id=lease.run_id,
            execution_epoch=lease.execution_epoch,
            route_name=lease.route_name,
            system_instructions=self._system_instructions,
            project_instructions=self._redactor.redact_text(workspace.project_instructions),
            conversation=conversation,
            referenced_files=tuple(
                ReferencedContextFile(
                    path=self._redactor.redact_text(item.path),
                    content=self._redactor.redact_text(item.content),
                    active=item.active,
                )
                for item in workspace.referenced_files
            ),
            task_plan=recovery.task_plan,
            recent_tool_results=tuple(
                ContextToolResult(
                    tool_call_id=outcome.tool_call_id,
                    tool_name=outcome.tool_name,
                    content=_recent_tool_result_content(outcome, self._redactor),
                    is_error=outcome.error is not None,
                )
                for outcome in recovery.prior_tool_outcomes
            ),
            memories=tuple(
                ContextMemorySnippet(memory_id=memory.id, content=memory.content)
                for memory in memories
            ),
            current_git_diff=self._redactor.redact_text(workspace.current_git_diff),
            previous_summary=previous_summary,
            force_compaction=force_compaction,
        )


def _recent_tool_result_content(outcome: DurableToolOutcome, redactor: Redactor) -> str:
    if outcome.result is not None:
        payload = outcome.result.to_json_object()
    elif outcome.error is not None:
        payload = outcome.error.model_dump(mode="json")
    else:  # Defensive only: DurableToolOutcome validation requires one terminal value.
        payload = {}
    serialized = json.dumps(
        redactor.redact(payload),
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    encoded = serialized.encode("utf-8")
    if len(encoded) <= _MAX_RECENT_TOOL_RESULT_BYTES:
        return serialized
    digest = hashlib.sha256(encoded).hexdigest()
    marker = (f"[TRUNCATED size_bytes={len(encoded)} sha256={digest}]\n").encode()
    prefix = encoded[: _MAX_RECENT_TOOL_RESULT_BYTES - len(marker)].decode(
        "utf-8",
        errors="ignore",
    )
    return marker.decode() + prefix


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


class ContextCompactionScheduler(Protocol):
    """Atomically create a compaction request once uncompacted history reaches a threshold."""

    async def request_compaction_if_needed(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        after_message_sequence: int | None,
        threshold_messages: int,
        route_name: str,
        requested_at: datetime,
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
        proactive_compactions: ContextCompactionScheduler | None = None,
        proactive_threshold_messages: int = _DEFAULT_PROACTIVE_COMPACTION_MESSAGES,
    ) -> None:
        if type(proactive_threshold_messages) is not int or not (
            1 <= proactive_threshold_messages < _MAX_SOURCE_MESSAGES
        ):
            raise ValueError("proactive compaction threshold must be in [1, 4095]")
        self._pipeline = pipeline
        self._source = source
        self._compactions = compactions
        self._clock = clock
        self._proactive_compactions = proactive_compactions
        self._proactive_threshold_messages = proactive_threshold_messages

    async def build(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        recovery: RunRecoveryState,
    ) -> ContextBuildResult:
        latest = await self._compactions.latest_completed(
            lease.tenant_id,
            lease.session_id,
        )
        pending = await self._compactions.pending_for_session(
            lease.tenant_id,
            lease.session_id,
        )
        if pending is None and self._proactive_compactions is not None:
            pending = await self._proactive_compactions.request_compaction_if_needed(
                lease.tenant_id,
                lease.session_id,
                after_message_sequence=(
                    latest.source_message_sequence if latest is not None else None
                ),
                threshold_messages=self._proactive_threshold_messages,
                route_name=lease.route_name,
                requested_at=self._clock.now(),
            )
        request = await self._source.load(
            lease,
            writer_lease,
            recovery,
            after_message_sequence=(latest.source_message_sequence if latest is not None else None),
            through_message_sequence=(
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


def _gateway_message(message: PersistedMessage) -> GatewayMessage:
    metadata = message.metadata.to_json_object()
    candidate = {
        "role": message.role.value,
        "content": message.content,
        "tool_call_id": metadata.get("tool_call_id"),
        "tool_calls": metadata.get("tool_calls", []),
    }
    try:
        return GatewayMessage.model_validate(candidate)
    except ValueError:
        raise DomainOperationError(
            code="context_message_invalid",
            message="durable conversation history contains an invalid normalized message",
        ) from None


__all__ = [
    "ContextCompactionScheduler",
    "ContextCompactionStore",
    "ContextHistoryStore",
    "ContextMemoryStore",
    "DurableRunContextBuilder",
    "PersistentRunContextSource",
    "RunContextDataSource",
    "WorkspaceContextSource",
]
