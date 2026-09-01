"""Bounded provider-neutral in-memory sessions for local development."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Protocol

from agent_core.domain.status import ApprovalMode
from agent_core.gateway import GatewayMessage, MessageRole
from agent_core.loop import AgentLoop, AgentLoopInput, TranscriptJournal

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from agent_core.events import AnyAgentEvent

_MAX_SESSION_MESSAGES = 4096
_MAX_SESSION_BYTES = 16 * 1024 * 1024
_MAX_TASK_BYTES = 1024 * 1024


class LocalLoopFactory(Protocol):
    """Create one loop whose transcript writes belong to the supplied local run."""

    def __call__(
        self,
        run_id: uuid.UUID,
        journal: TranscriptJournal,
    ) -> AgentLoop: ...


class InMemoryTranscriptJournal:
    """Append-only bounded transcript journal for one local run."""

    def __init__(
        self,
        run_id: uuid.UUID,
        messages: tuple[GatewayMessage, ...],
        *,
        max_messages: int = _MAX_SESSION_MESSAGES,
        max_bytes: int = _MAX_SESSION_BYTES,
    ) -> None:
        if not isinstance(run_id, uuid.UUID):
            raise TypeError("run_id must be a UUID")
        if type(max_messages) is not int or not 1 <= max_messages <= _MAX_SESSION_MESSAGES:
            raise ValueError(f"max_messages must be between 1 and {_MAX_SESSION_MESSAGES}")
        if type(max_bytes) is not int or not 1 <= max_bytes <= _MAX_SESSION_BYTES:
            raise ValueError(f"max_bytes must be between 1 and {_MAX_SESSION_BYTES}")
        self._run_id = run_id
        self._max_messages = max_messages
        self._max_bytes = max_bytes
        self._messages = list(messages)
        self._encoded_bytes = _messages_size(messages)
        self._require_within_limits()

    @property
    def messages(self) -> tuple[GatewayMessage, ...]:
        """Return an immutable snapshot of the locally retained conversation."""

        return tuple(self._messages)

    async def append(
        self,
        run_id: uuid.UUID,
        *,
        start_index: int,
        messages: tuple[GatewayMessage, ...],
    ) -> None:
        if run_id != self._run_id:
            raise ValueError("transcript append belongs to a different local run")
        if type(start_index) is not int or start_index != len(self._messages):
            raise ValueError("transcript append is not contiguous")
        candidate_count = len(self._messages) + len(messages)
        candidate_bytes = self._encoded_bytes + _messages_size(messages)
        if candidate_count > self._max_messages or candidate_bytes > self._max_bytes:
            raise ValueError("local transcript exceeds its configured limit")
        self._messages.extend(messages)
        self._encoded_bytes = candidate_bytes

    def _require_within_limits(self) -> None:
        if len(self._messages) > self._max_messages or self._encoded_bytes > self._max_bytes:
            raise ValueError("initial local transcript exceeds its configured limit")


class InMemoryAgentSession:
    """Serialize local turns while retaining only a bounded in-process transcript."""

    def __init__(
        self,
        *,
        loop_factory: LocalLoopFactory,
        route_name: str,
        worker_id: str = "local-worker",
        system_message: str = "You are a coding agent operating in one repository workspace.",
        tenant_id: uuid.UUID | None = None,
        session_id: uuid.UUID | None = None,
        run_id_factory: Callable[[], uuid.UUID] = uuid.uuid4,
        max_messages: int = _MAX_SESSION_MESSAGES,
        max_bytes: int = _MAX_SESSION_BYTES,
    ) -> None:
        if not callable(loop_factory):
            raise TypeError("loop_factory must be callable")
        if not callable(run_id_factory):
            raise TypeError("run_id_factory must be callable")
        if not isinstance(system_message, str) or not system_message.strip():
            raise ValueError("system_message must be non-empty text")
        self._loop_factory = loop_factory
        self._route_name = route_name
        self._worker_id = worker_id
        self._tenant_id = tenant_id or uuid.uuid4()
        self._session_id = session_id or uuid.uuid4()
        self._run_id_factory = run_id_factory
        self._max_messages = max_messages
        self._max_bytes = max_bytes
        self._messages: tuple[GatewayMessage, ...] = (
            GatewayMessage(role=MessageRole.SYSTEM, content=system_message.strip()),
        )
        InMemoryTranscriptJournal(
            uuid.uuid4(),
            self._messages,
            max_messages=max_messages,
            max_bytes=max_bytes,
        )
        self._lock = asyncio.Lock()

    @property
    def tenant_id(self) -> uuid.UUID:
        return self._tenant_id

    @property
    def session_id(self) -> uuid.UUID:
        return self._session_id

    @property
    def messages(self) -> tuple[GatewayMessage, ...]:
        return self._messages

    async def run(self, task: str) -> AsyncIterator[AnyAgentEvent]:
        """Execute one local run and commit its normalized transcript in memory."""

        if not isinstance(task, str) or not task.strip():
            raise ValueError("task must be non-empty text")
        normalized_task = task.strip()
        if len(normalized_task.encode("utf-8")) > _MAX_TASK_BYTES:
            raise ValueError("task exceeds the local input byte limit")
        async with self._lock:
            run_id = self._run_id_factory()
            if not isinstance(run_id, uuid.UUID):
                raise TypeError("run_id_factory must return UUID values")
            initial = (
                *self._messages,
                GatewayMessage(role=MessageRole.USER, content=normalized_task),
            )
            journal = InMemoryTranscriptJournal(
                run_id,
                initial,
                max_messages=self._max_messages,
                max_bytes=self._max_bytes,
            )
            loop = self._loop_factory(run_id, journal)
            if not isinstance(loop, AgentLoop):
                raise TypeError("loop_factory must return AgentLoop")
            async for event in loop.run(
                AgentLoopInput(
                    tenant_id=self._tenant_id,
                    session_id=self._session_id,
                    run_id=run_id,
                    attempt=1,
                    worker_id=self._worker_id,
                    route_name=self._route_name,
                    messages=initial,
                    approval_mode=ApprovalMode.AUTO_APPROVE,
                )
            ):
                yield event
            self._messages = journal.messages

    async def reset(self) -> None:
        """Drop conversation state except for the original system instruction."""

        async with self._lock:
            self._messages = self._messages[:1]


def _messages_size(messages: tuple[GatewayMessage, ...]) -> int:
    return sum(len(message.model_dump_json().encode("utf-8")) for message in messages)


__all__ = [
    "InMemoryAgentSession",
    "InMemoryTranscriptJournal",
    "LocalLoopFactory",
]
