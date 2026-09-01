from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from agent_core.context import ReferencedContextFile, WorkspaceContextSnapshot
from agent_core.control import MemoryKind, PersistedMemory, PersistedMessage, memory_content_hash
from agent_core.distributed import (
    DurableToolOutcome,
    RunLease,
    RunRecoveryState,
    WorkspaceWriterLease,
)
from agent_core.domain.base import FrozenJsonObject, JsonObject
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.status import ToolCallStatus
from agent_core.gateway import GatewayMessage, MessageRole
from agent_core.workspace_access import WorkspaceFileReference
from agent_worker.context import PersistentRunContextSource
from platform_telemetry import Redactor

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
RUN_ID = uuid.UUID("30000000-0000-0000-0000-000000000003")
WORKSPACE_ID = uuid.UUID("40000000-0000-0000-0000-000000000004")
RUN_TOKEN = uuid.UUID("50000000-0000-0000-0000-000000000005")
WRITER_TOKEN = uuid.UUID("60000000-0000-0000-0000-000000000006")


class _History:
    def __init__(
        self,
        messages: tuple[PersistedMessage, ...],
        references: tuple[WorkspaceFileReference, ...] = (),
    ) -> None:
        self.messages = messages
        self.references = references
        self.range: tuple[int | None, int | None, int] | None = None

    async def list_messages(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        after_sequence: int | None,
        through_sequence: int | None,
        limit: int,
    ) -> tuple[PersistedMessage, ...]:
        assert tenant_id == TENANT_ID and session_id == SESSION_ID
        self.range = (after_sequence, through_sequence, limit)
        return self.messages

    async def referenced_files_for_run(
        self,
        tenant_id: uuid.UUID,
        run_id: uuid.UUID,
    ) -> tuple[WorkspaceFileReference, ...]:
        assert tenant_id == TENANT_ID and run_id == RUN_ID
        return self.references


class _Memories:
    def __init__(self, memories: tuple[PersistedMemory, ...]) -> None:
        self.memories = memories

    async def list_active(
        self,
        tenant_id: uuid.UUID,
        session_id: uuid.UUID,
        *,
        limit: int,
    ) -> tuple[PersistedMemory, ...]:
        assert tenant_id == TENANT_ID and session_id == SESSION_ID and limit == 100
        return self.memories


class _WorkspaceContext:
    def __init__(self, snapshot: WorkspaceContextSnapshot) -> None:
        self.snapshot = snapshot
        self.observed: (
            tuple[RunLease, WorkspaceWriterLease, tuple[WorkspaceFileReference, ...]] | None
        ) = None

    async def load_workspace_context(
        self,
        lease: RunLease,
        writer_lease: WorkspaceWriterLease,
        referenced_files: tuple[WorkspaceFileReference, ...],
    ) -> WorkspaceContextSnapshot:
        self.observed = (lease, writer_lease, referenced_files)
        return self.snapshot


def _lease() -> RunLease:
    return RunLease(
        tenant_id=TENANT_ID,
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace_id=WORKSPACE_ID,
        worker_id="worker-1",
        route_name="coding-default",
        lease_token=RUN_TOKEN,
        generation=1,
        attempt=1,
        priority=0,
        acquired_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )


def _writer() -> WorkspaceWriterLease:
    return WorkspaceWriterLease(
        tenant_id=TENANT_ID,
        workspace_id=WORKSPACE_ID,
        run_id=RUN_ID,
        worker_id="worker-1",
        run_lease_token=RUN_TOKEN,
        lease_token=WRITER_TOKEN,
        generation=1,
        acquired_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )


@pytest.mark.asyncio
async def test_persistent_context_source_loads_cross_run_messages_and_memory() -> None:
    calls: list[JsonObject] = [
        {"id": "call-1", "name": "read_file", "arguments": {"path": "README.md"}}
    ]
    messages = (
        PersistedMessage(
            id=uuid.uuid4(),
            session_id=SESSION_ID,
            run_id=uuid.uuid4(),
            sequence=8,
            role=MessageRole.ASSISTANT,
            content="",
            metadata=FrozenJsonObject(cast("JsonObject", {"tool_calls": calls})),
            created_at=NOW,
        ),
        PersistedMessage(
            id=uuid.uuid4(),
            session_id=SESSION_ID,
            run_id=RUN_ID,
            sequence=9,
            role=MessageRole.TOOL,
            content='{"path":"README.md"}',
            metadata=FrozenJsonObject({"tool_call_id": "call-1"}),
            created_at=NOW,
        ),
    )
    memory_content = "Use the repository formatter."
    memory = PersistedMemory(
        id=uuid.uuid4(),
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        source_run_id=RUN_ID,
        kind=MemoryKind.PREFERENCE,
        content=memory_content,
        content_hash=memory_content_hash(memory_content),
        extracted_at=NOW,
    )
    history = _History(messages)
    source = PersistentRunContextSource(
        history=history,
        memories=_Memories((memory,)),
        system_instructions="Work safely.",
        redactor=Redactor(("known-secret",)),
    )
    successful_outcome = DurableToolOutcome(
        tool_call_id="call-success",
        tool_name="read_file",
        turn_number=1,
        argument_hash="0" * 64,
        status=ToolCallStatus.COMPLETED,
        result=FrozenJsonObject({"content": "known-secret"}),
    )
    failed_outcome = DurableToolOutcome(
        tool_call_id="call-failed",
        tool_name="search",
        turn_number=2,
        argument_hash="1" * 64,
        status=ToolCallStatus.FAILED,
        error=ErrorDetail(
            code="search_failed",
            message="known-secret failed",
            details={"token": "another-secret"},
        ),
    )

    request = await source.load(
        _lease(),
        _writer(),
        RunRecoveryState(
            messages=(GatewayMessage(role=MessageRole.USER, content="fallback"),),
            prior_tool_outcomes=(successful_outcome, failed_outcome),
        ),
        after_message_sequence=7,
        through_message_sequence=9,
        previous_summary="earlier summary",
        force_compaction=True,
    )

    assert history.range == (7, 9, 4096)
    assert request.conversation[0].tool_calls[0].id == "call-1"
    assert request.conversation[1].tool_call_id == "call-1"
    assert request.memories[0].content == memory_content
    assert [item.tool_call_id for item in request.recent_tool_results] == [
        "call-success",
        "call-failed",
    ]
    assert all("known-secret" not in item.content for item in request.recent_tool_results)
    assert "[REDACTED]" in request.recent_tool_results[0].content
    assert '"token":"[REDACTED]"' in request.recent_tool_results[1].content
    assert request.recent_tool_results[1].is_error is True
    assert request.previous_summary == "earlier summary"
    assert request.force_compaction is True


@pytest.mark.asyncio
async def test_persistent_context_source_bounds_recent_tool_result_content() -> None:
    outcome = DurableToolOutcome(
        tool_call_id="large-result",
        tool_name="read_file",
        turn_number=1,
        argument_hash="0" * 64,
        status=ToolCallStatus.COMPLETED,
        result=FrozenJsonObject({"content": "🙂" * 40_000}),
    )
    source = PersistentRunContextSource(
        history=_History(()),
        memories=_Memories(()),
        system_instructions="Work safely.",
    )

    request = await source.load(
        _lease(),
        _writer(),
        RunRecoveryState(
            messages=(GatewayMessage(role=MessageRole.USER, content="fallback"),),
            prior_tool_outcomes=(outcome,),
        ),
        after_message_sequence=None,
        through_message_sequence=None,
        previous_summary=None,
        force_compaction=False,
    )

    content = request.recent_tool_results[0].content
    assert content.startswith("[TRUNCATED size_bytes=")
    assert len(content.encode("utf-8")) <= 64 * 1024


@pytest.mark.asyncio
async def test_persistent_context_source_loads_and_redacts_workspace_context() -> None:
    references = (WorkspaceFileReference(path="README.md"),)
    workspace = _WorkspaceContext(
        WorkspaceContextSnapshot(
            project_instructions="Never expose known-secret.",
            referenced_files=(
                ReferencedContextFile(
                    path="README.md",
                    content="configuration=known-secret",
                    active=True,
                ),
            ),
            current_git_diff="+token=known-secret",
        )
    )
    source = PersistentRunContextSource(
        history=_History((), references),
        memories=_Memories(()),
        system_instructions="Work safely.",
        workspace_context=workspace,
        redactor=Redactor(("known-secret",)),
    )

    request = await source.load(
        _lease(),
        _writer(),
        RunRecoveryState(messages=(GatewayMessage(role=MessageRole.USER, content="task"),)),
        after_message_sequence=None,
        through_message_sequence=None,
        previous_summary=None,
        force_compaction=False,
    )

    assert workspace.observed == (_lease(), _writer(), references)
    assert request.referenced_files[0].path == "README.md"
    assert request.referenced_files[0].active is True
    assert "known-secret" not in request.project_instructions
    assert "known-secret" not in request.referenced_files[0].content
    assert "known-secret" not in request.current_git_diff
    assert "[REDACTED]" in request.current_git_diff


@pytest.mark.asyncio
async def test_persistent_context_source_requires_workspace_for_explicit_references() -> None:
    source = PersistentRunContextSource(
        history=_History((), (WorkspaceFileReference(path="README.md"),)),
        memories=_Memories(()),
        system_instructions="Work safely.",
    )

    with pytest.raises(DomainOperationError) as failure:
        await source.load(
            _lease(),
            _writer(),
            RunRecoveryState(messages=(GatewayMessage(role=MessageRole.USER, content="task"),)),
            after_message_sequence=None,
            through_message_sequence=None,
            previous_summary=None,
            force_compaction=False,
        )
    assert failure.value.code == "context_workspace_source_unavailable"
    assert failure.value.retryable is True


@pytest.mark.asyncio
async def test_persistent_context_source_rejects_a_mismatched_writer() -> None:
    source = PersistentRunContextSource(
        history=_History(()),
        memories=_Memories(()),
        system_instructions="Work safely.",
    )
    mismatched = _writer().model_copy(update={"run_lease_token": uuid.uuid4()})

    with pytest.raises(DomainOperationError) as captured:
        await source.load(
            _lease(),
            mismatched,
            RunRecoveryState(messages=(GatewayMessage(role=MessageRole.USER, content="task"),)),
            after_message_sequence=None,
            through_message_sequence=None,
            previous_summary=None,
            force_compaction=False,
        )
    assert captured.value.code == "context_workspace_lease_mismatch"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"system_instructions": ""}, "system instructions"),
        ({"system_instructions": "x" * (64 * 1024 + 1)}, "system instructions"),
        ({"message_limit": 0}, "message limit"),
        ({"message_limit": True}, "message limit"),
        ({"memory_limit": 501}, "memory limit"),
        ({"memory_limit": False}, "memory limit"),
    ],
)
def test_persistent_context_source_rejects_unbounded_configuration(
    overrides: dict[str, object],
    message: str,
) -> None:
    arguments: dict[str, object] = {
        "history": _History(()),
        "memories": _Memories(()),
        "system_instructions": "Work safely.",
    }
    arguments.update(overrides)
    with pytest.raises(ValueError, match=message):
        PersistentRunContextSource(**cast("dict[str, Any]", arguments))


@pytest.mark.asyncio
async def test_persistent_context_source_uses_recovery_only_without_durable_summary() -> None:
    recovery_message = GatewayMessage(role=MessageRole.USER, content="fallback task")
    source = PersistentRunContextSource(
        history=_History(()),
        memories=_Memories(()),
        system_instructions="Work safely.",
    )

    request = await source.load(
        _lease(),
        _writer(),
        RunRecoveryState(messages=(recovery_message,)),
        after_message_sequence=None,
        through_message_sequence=None,
        previous_summary=None,
        force_compaction=False,
    )

    assert request.conversation == (recovery_message,)


@pytest.mark.asyncio
async def test_persistent_context_source_rejects_invalid_durable_message_metadata() -> None:
    invalid_message = PersistedMessage(
        id=uuid.uuid4(),
        session_id=SESSION_ID,
        run_id=RUN_ID,
        sequence=1,
        role=MessageRole.TOOL,
        content="result without a call identifier",
        created_at=NOW,
    )
    source = PersistentRunContextSource(
        history=_History((invalid_message,)),
        memories=_Memories(()),
        system_instructions="Work safely.",
    )

    with pytest.raises(DomainOperationError) as captured:
        await source.load(
            _lease(),
            _writer(),
            RunRecoveryState(messages=(GatewayMessage(role=MessageRole.USER, content="fallback"),)),
            after_message_sequence=None,
            through_message_sequence=None,
            previous_summary=None,
            force_compaction=False,
        )
    assert captured.value.code == "context_message_invalid"
