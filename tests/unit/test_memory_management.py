from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from agent_core.control import (
    ContextCompactionStatus,
    MemoryExtractionJob,
    MemoryExtractionStatus,
    MemoryKind,
    PersistedContextCompaction,
    PersistedMemory,
    TaskPlanUpdate,
    memory_content_hash,
)
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.fakes import (
    ScriptedGatewayTurn,
    ScriptedModelGateway,
    SequentialIdGenerator,
    SteppingClock,
)
from agent_core.gateway import (
    GatewayFinishReason,
    GatewayResponseCompleted,
    GatewayTextDelta,
    GatewayToolCall,
)
from agent_core.memory import (
    ExtractedMemory,
    GatewayMemoryExtractor,
    MemoryExtractionInput,
    MemoryExtractionResult,
)
from agent_scheduler import MemoryExtractionProcessor

if TYPE_CHECKING:
    from collections.abc import Sequence


NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)
TENANT_ID = uuid.UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = uuid.UUID("20000000-0000-0000-0000-000000000002")
RUN_ID = uuid.UUID("30000000-0000-0000-0000-000000000003")
JOB_ID = uuid.UUID("40000000-0000-0000-0000-000000000004")
MEMORY_ID = uuid.UUID("50000000-0000-0000-0000-000000000005")
LEASE_TOKEN = uuid.UUID("60000000-0000-0000-0000-000000000006")


def _running_job() -> MemoryExtractionJob:
    return MemoryExtractionJob(
        id=JOB_ID,
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        run_id=RUN_ID,
        status=MemoryExtractionStatus.RUNNING,
        source_message_sequence=12,
        worker_id="memory-worker",
        lease_token=LEASE_TOKEN,
        lease_generation=1,
        lease_expires_at=NOW + timedelta(minutes=5),
        created_at=NOW,
        started_at=NOW + timedelta(seconds=1),
    )


def test_context_task_and_memory_contracts_are_closed_and_consistent() -> None:
    pending = PersistedContextCompaction(
        id=uuid.uuid4(),
        session_id=SESSION_ID,
        status=ContextCompactionStatus.PENDING,
        idempotency_key="compact-1",
        source_message_sequence=10,
        route_name="coding-default",
        requested_at=NOW,
    )
    with pytest.raises(ValidationError):
        pending.model_copy(update={"summary": "premature"})

    plan = TaskPlanUpdate.model_validate(
        {
            "expected_version": 0,
            "tasks": [
                {"id": "first", "title": "First", "status": "completed"},
                {
                    "id": "second",
                    "title": "Second",
                    "depends_on": ["first"],
                },
            ],
        }
    )
    assert plan.tasks[1].depends_on == ("first",)
    with pytest.raises(ValidationError, match="same plan"):
        TaskPlanUpdate.model_validate(
            {
                "expected_version": 0,
                "tasks": [{"id": "second", "title": "Second", "depends_on": ["missing"]}],
            }
        )
    with pytest.raises(ValidationError, match="acyclic"):
        TaskPlanUpdate.model_validate(
            {
                "expected_version": 0,
                "tasks": [
                    {"id": "first", "title": "First", "depends_on": ["second"]},
                    {"id": "second", "title": "Second", "depends_on": ["first"]},
                ],
            }
        )

    memory = PersistedMemory(
        id=MEMORY_ID,
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        source_run_id=RUN_ID,
        kind=MemoryKind.DECISION,
        content="Use PostgreSQL for durable state.",
        content_hash=memory_content_hash("Use PostgreSQL for durable state."),
        extracted_at=NOW,
    )
    assert memory.model_dump(mode="json")["source_run_id"] == str(RUN_ID)
    with pytest.raises(ValidationError, match="archive"):
        memory.model_copy(update={"archived_at": NOW - timedelta(seconds=1)})


@pytest.mark.asyncio
async def test_gateway_memory_extractor_validates_json_usage_and_redacts_source() -> None:
    gateway = ScriptedModelGateway(
        [
            ScriptedGatewayTurn(
                events=(
                    GatewayTextDelta(
                        delta=(
                            '{"memories":[{"kind":"decision","content":'
                            '"Use Podman for container isolation."}]}'
                        )
                    ),
                    GatewayResponseCompleted(
                        finish_reason=GatewayFinishReason.STOP,
                        input_tokens=50,
                        output_tokens=12,
                    ),
                )
            )
        ]
    )
    extractor = GatewayMemoryExtractor(
        gateway=gateway,
        id_generator=SequentialIdGenerator(),
    )
    result = await extractor.extract(
        MemoryExtractionInput(
            tenant_id=TENANT_ID,
            session_id=SESSION_ID,
            run_id=RUN_ID,
            transcript="credential sk-abcdefghijklmnopqrstuvwxyz123456; use Podman",
        )
    )

    assert result.memories[0].kind is MemoryKind.DECISION
    assert result.input_tokens == 50
    assert gateway.requests[0].route_name == "summarization"
    assert "sk-abcdefghijklmnopqrstuvwxyz123456" not in gateway.requests[0].messages[1].content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "turn",
    [
        ScriptedGatewayTurn.text('{"memories":[{"kind":"fact","content":NaN}]}'),
        ScriptedGatewayTurn.tool_calls(
            GatewayToolCall(id="call-1", name="read_file", arguments={})
        ),
        ScriptedGatewayTurn(
            events=(GatewayResponseCompleted(finish_reason=GatewayFinishReason.LENGTH),)
        ),
    ],
)
async def test_gateway_memory_extractor_fails_closed_for_invalid_output(
    turn: ScriptedGatewayTurn,
) -> None:
    extractor = GatewayMemoryExtractor(
        gateway=ScriptedModelGateway([turn]),
        id_generator=SequentialIdGenerator(),
    )
    with pytest.raises(DomainOperationError) as caught:
        await extractor.extract(
            MemoryExtractionInput(
                tenant_id=TENANT_ID,
                session_id=SESSION_ID,
                run_id=RUN_ID,
                transcript="bounded source",
            )
        )
    assert caught.value.code == "memory_extraction_invalid"


class MemoryStoreFake:
    def __init__(
        self,
        job: MemoryExtractionJob,
        source: str = "durable transcript",
        *,
        lease_duration: timedelta = timedelta(minutes=5),
    ) -> None:
        self.job = job
        self.source = source
        self.lease_duration = lease_duration
        self.completed: tuple[PersistedMemory, ...] | None = None
        self.failed_error: ErrorDetail | None = None
        self.claimed = False

    async def claim_pending(
        self,
        *,
        worker_id: str,
        occurred_at: datetime,
        lease_duration: timedelta,
    ) -> MemoryExtractionJob | None:
        assert worker_id == "memory-scheduler"
        assert lease_duration == self.lease_duration
        assert occurred_at >= NOW
        if self.claimed:
            return None
        self.claimed = True
        return self.job

    async def source_for_job(
        self,
        job: MemoryExtractionJob,
        *,
        max_bytes: int,
    ) -> str:
        assert job == self.job
        assert max_bytes >= len(self.source)
        return self.source

    async def complete(
        self,
        job: MemoryExtractionJob,
        memories: Sequence[PersistedMemory],
        *,
        completed_at: datetime,
    ) -> MemoryExtractionJob:
        assert job == self.job
        assert completed_at >= NOW
        self.completed = tuple(memories)
        return job

    async def fail(
        self,
        job: MemoryExtractionJob,
        *,
        error: ErrorDetail,
        completed_at: datetime,
    ) -> MemoryExtractionJob:
        assert job == self.job
        assert "provider" not in str(error)
        assert completed_at >= NOW
        self.failed_error = error
        return job


class MemoryExtractorFake:
    def __init__(self, *, failure: bool = False) -> None:
        self.failure = failure

    async def extract(self, request: MemoryExtractionInput) -> MemoryExtractionResult:
        assert request.run_id == RUN_ID
        if self.failure:
            raise RuntimeError("provider secret must remain opaque")
        return MemoryExtractionResult(
            memories=(ExtractedMemory(kind=MemoryKind.FACT, content="Repository uses uv."),),
            input_tokens=10,
            output_tokens=4,
        )


@pytest.mark.asyncio
async def test_memory_processor_persists_provenance_and_handles_failures_opaquely() -> None:
    store = MemoryStoreFake(_running_job())
    processor = MemoryExtractionProcessor(
        store=store,
        extractor=MemoryExtractorFake(),
        clock=SteppingClock(NOW + timedelta(seconds=2)),
        id_factory=lambda: MEMORY_ID,
    )
    assert await processor.process_once() is True
    assert store.completed is not None
    memory = store.completed[0]
    assert memory.id == MEMORY_ID
    assert memory.tenant_id == TENANT_ID
    assert memory.session_id == SESSION_ID
    assert memory.source_run_id == RUN_ID
    assert memory.content_hash == memory_content_hash(memory.content)
    assert await processor.process_once() is False

    failed_store = MemoryStoreFake(_running_job())
    failed = MemoryExtractionProcessor(
        store=failed_store,
        extractor=MemoryExtractorFake(failure=True),
        clock=SteppingClock(NOW + timedelta(seconds=2)),
    )
    assert await failed.process_once() is True
    assert failed_store.failed_error is not None
    assert failed_store.failed_error.code == "memory_extraction_failed"


@pytest.mark.asyncio
async def test_memory_processor_skips_jobs_completed_by_disable_policy() -> None:
    completed = MemoryExtractionJob(
        id=JOB_ID,
        tenant_id=TENANT_ID,
        session_id=SESSION_ID,
        run_id=RUN_ID,
        status=MemoryExtractionStatus.COMPLETED,
        source_message_sequence=12,
        created_at=NOW,
        started_at=NOW,
        completed_at=NOW,
    )
    store = MemoryStoreFake(completed)
    processor = MemoryExtractionProcessor(
        store=store,
        extractor=MemoryExtractorFake(failure=True),
        clock=SteppingClock(NOW),
    )
    assert await processor.process_once() is True
    assert store.completed is None
    assert store.failed_error is None


class HangingMemoryExtractor:
    async def extract(self, _request: MemoryExtractionInput) -> MemoryExtractionResult:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_memory_processor_times_out_before_its_lease_expires() -> None:
    store = MemoryStoreFake(_running_job(), lease_duration=timedelta(seconds=1))
    processor = MemoryExtractionProcessor(
        store=store,
        extractor=HangingMemoryExtractor(),
        clock=SteppingClock(NOW + timedelta(seconds=2)),
        lease_duration_seconds=1,
        extraction_timeout_seconds=0.01,
    )
    assert await processor.process_once() is True
    assert store.completed is None
    assert store.failed_error is not None
    assert store.failed_error.code == "memory_extraction_timeout"
    assert store.failed_error.retryable is True


def test_memory_processor_rejects_a_timeout_that_can_outlive_the_lease() -> None:
    with pytest.raises(ValueError, match="shorter than its lease"):
        MemoryExtractionProcessor(
            store=MemoryStoreFake(_running_job()),
            extractor=MemoryExtractorFake(),
            clock=SteppingClock(NOW),
            lease_duration_seconds=5,
            extraction_timeout_seconds=5,
        )
