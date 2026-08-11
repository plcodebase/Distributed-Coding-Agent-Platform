"""Provider-neutral durable control-plane values shared by API and persistence."""

from __future__ import annotations

import hashlib
import json
import uuid  # noqa: TC003 - Pydantic resolves identifiers at runtime
from enum import StrEnum
from typing import Annotated, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel, FrozenJsonObject
from agent_core.domain.models import (
    IdentifierString,  # noqa: TC001 - runtime Pydantic field
    Run,  # noqa: TC001 - Pydantic resolves run at runtime
    Sha256Hex,  # noqa: TC001 - runtime Pydantic field
)
from agent_core.gateway import MessageRole  # noqa: TC001 - Pydantic resolves role at runtime
from agent_core.scheduling import RunPriorityClass

type IdempotencyKey = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=255,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]
MAX_PERSISTED_MEMORY_BYTES = 65_536


class RunCreationResult(DomainModel):
    """Outcome of one API-idempotent run creation transaction."""

    run: Run
    created: bool


class ApprovalStatus(StrEnum):
    """Durable approval decision state."""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class ApprovalDecision(DomainModel):
    """Validated approval mutation supplied by an authenticated principal."""

    approved: bool
    decided_by: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ]
    decided_at: AwareTimestamp


class PersistedApproval(DomainModel):
    """Tenant-safe approval representation returned to the API."""

    id: uuid.UUID
    run_id: uuid.UUID
    status: ApprovalStatus
    reason: str
    arguments: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    decided_by: str | None = None
    requested_at: AwareTimestamp
    decided_at: AwareTimestamp | None = None

    @model_validator(mode="after")
    def validate_decision(self) -> Self:
        if self.status is ApprovalStatus.PENDING:
            if self.decided_by is not None or self.decided_at is not None:
                raise ValueError("pending approval may not contain decision metadata")
        elif self.decided_by is None or self.decided_at is None:
            raise ValueError("decided approval requires subject and timestamp")
        if self.decided_at is not None and self.decided_at < self.requested_at:
            raise ValueError("approval decision may not precede its request")
        return self


class PersistedMessage(DomainModel):
    """Validated durable conversation message."""

    id: uuid.UUID
    session_id: uuid.UUID
    run_id: uuid.UUID
    sequence: int = Field(ge=1)
    role: MessageRole
    content: str
    metadata: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    created_at: AwareTimestamp


class PersistedTaskPlan(DomainModel):
    """Validated versioned task plan."""

    id: uuid.UUID
    run_id: uuid.UUID
    version: int = Field(ge=1)
    plan: FrozenJsonObject
    created_at: AwareTimestamp


class ContextCompactionStatus(StrEnum):
    """Durable lifecycle for an explicit transcript compaction request."""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"


class PersistedContextCompaction(DomainModel):
    """Append-only compaction metadata; the source transcript remains untouched."""

    id: uuid.UUID
    session_id: uuid.UUID
    status: ContextCompactionStatus
    idempotency_key: IdempotencyKey
    source_message_sequence: int = Field(ge=0)
    route_name: IdentifierString
    summary: str | None = None
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    error: FrozenJsonObject | None = None
    requested_at: AwareTimestamp
    completed_at: AwareTimestamp | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.summary is not None and len(self.summary.encode("utf-8")) > 256 * 1024:
            raise ValueError("compaction summary exceeds its UTF-8 byte limit")
        if self.status is ContextCompactionStatus.PENDING:
            if any(
                value is not None
                for value in (
                    self.summary,
                    self.input_tokens,
                    self.output_tokens,
                    self.error,
                    self.completed_at,
                )
            ):
                raise ValueError("pending compaction may not contain a terminal outcome")
        elif self.status is ContextCompactionStatus.COMPLETED:
            if (
                not self.summary
                or self.input_tokens is None
                or self.output_tokens is None
                or self.error is not None
                or self.completed_at is None
            ):
                raise ValueError("completed compaction requires summary, usage, and timestamp")
        elif (
            self.error is None
            or self.summary is not None
            or self.input_tokens is not None
            or self.output_tokens is not None
            or self.completed_at is None
        ):
            raise ValueError("failed compaction requires only an error and timestamp")
        if self.completed_at is not None and self.completed_at < self.requested_at:
            raise ValueError("compaction completion may not precede its request")
        return self


class TaskStatus(StrEnum):
    """Explicit durable state for one tracked task item."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


class TrackedTask(DomainModel):
    """Closed task item suitable for compare-and-set plan updates."""

    id: IdentifierString
    title: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=1000),
    ]
    status: TaskStatus = TaskStatus.PENDING
    details: Annotated[str, StringConstraints(max_length=16_384)] = ""
    depends_on: tuple[IdentifierString, ...] = Field(default=(), max_length=100)

    @model_validator(mode="after")
    def validate_dependencies(self) -> Self:
        if self.id in self.depends_on:
            raise ValueError("a task may not depend on itself")
        if len(self.depends_on) != len(set(self.depends_on)):
            raise ValueError("task dependencies must be unique")
        return self


class TaskPlanUpdate(DomainModel):
    """Client-supplied compare-and-set replacement for a task plan."""

    expected_version: int = Field(ge=0)
    tasks: tuple[TrackedTask, ...] = Field(max_length=500)

    @model_validator(mode="after")
    def validate_graph(self) -> Self:
        identifiers = [task.id for task in self.tasks]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("task IDs must be unique")
        known = set(identifiers)
        if any(dependency not in known for task in self.tasks for dependency in task.depends_on):
            raise ValueError("task dependencies must reference tasks in the same plan")
        dependencies = {task.id: task.depends_on for task in self.tasks}
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(identifier: str) -> None:
            if identifier in visiting:
                raise ValueError("task dependencies must be acyclic")
            if identifier in visited:
                return
            visiting.add(identifier)
            for dependency in dependencies[identifier]:
                visit(dependency)
            visiting.remove(identifier)
            visited.add(identifier)

        for identifier in identifiers:
            visit(identifier)
        by_id = {task.id: task for task in self.tasks}
        for task in self.tasks:
            if task.status not in {TaskStatus.IN_PROGRESS, TaskStatus.COMPLETED}:
                continue
            incomplete = tuple(
                dependency
                for dependency in task.depends_on
                if by_id[dependency].status is not TaskStatus.COMPLETED
            )
            if incomplete:
                raise ValueError("in-progress and completed tasks require completed dependencies")
        return self


class PersistedTaskState(DomainModel):
    """Latest durable, versioned task plan returned by task APIs."""

    id: uuid.UUID
    run_id: uuid.UUID
    version: int = Field(ge=1)
    tasks: tuple[TrackedTask, ...] = Field(max_length=500)
    created_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_tasks(self) -> Self:
        TaskPlanUpdate(expected_version=0, tasks=self.tasks)
        return self


class MemoryKind(StrEnum):
    """Conservative categories extracted from completed runs."""

    FACT = "fact"
    PREFERENCE = "preference"
    DECISION = "decision"
    CONSTRAINT = "constraint"


class PersistedMemory(DomainModel):
    """Tenant-owned memory with mandatory session/run provenance."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    session_id: uuid.UUID
    source_run_id: uuid.UUID
    kind: MemoryKind
    content: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=1,
            max_length=MAX_PERSISTED_MEMORY_BYTES,
        ),
    ]
    content_hash: Sha256Hex
    metadata: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    extracted_at: AwareTimestamp
    archived_at: AwareTimestamp | None = None

    @model_validator(mode="after")
    def validate_archive(self) -> Self:
        if len(self.content.encode("utf-8")) > MAX_PERSISTED_MEMORY_BYTES:
            raise ValueError("memory content exceeds its UTF-8 byte limit")
        if self.content_hash != memory_content_hash(self.content):
            raise ValueError("memory content hash does not match its content")
        if self.archived_at is not None and self.archived_at < self.extracted_at:
            raise ValueError("memory archive may not precede extraction")
        return self


class MemoryExtractionStatus(StrEnum):
    """Durable asynchronous extraction job state."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class MemoryExtractionJob(DomainModel):
    """Idempotent job created after a completed run when memory is enabled."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    session_id: uuid.UUID
    run_id: uuid.UUID
    status: MemoryExtractionStatus
    source_message_sequence: int = Field(ge=0)
    attempt: int = Field(default=1, ge=1, le=100)
    worker_id: IdentifierString | None = None
    lease_token: uuid.UUID | None = None
    lease_generation: int = Field(default=0, ge=0)
    lease_expires_at: AwareTimestamp | None = None
    error: FrozenJsonObject | None = None
    created_at: AwareTimestamp
    started_at: AwareTimestamp | None = None
    completed_at: AwareTimestamp | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.status is MemoryExtractionStatus.PENDING:
            if (
                self.started_at is not None
                or self.completed_at is not None
                or self.error is not None
                or self.worker_id is not None
                or self.lease_token is not None
                or self.lease_expires_at is not None
            ):
                raise ValueError("pending memory job may not have execution state")
        elif self.status is MemoryExtractionStatus.RUNNING:
            if (
                self.started_at is None
                or self.completed_at is not None
                or self.error is not None
                or self.worker_id is None
                or self.lease_token is None
                or self.lease_expires_at is None
                or self.lease_generation < 1
            ):
                raise ValueError("running memory job requires fenced lease state")
        elif self.status is MemoryExtractionStatus.COMPLETED:
            if (
                self.started_at is None
                or self.completed_at is None
                or self.error is not None
                or self.worker_id is not None
                or self.lease_token is not None
                or self.lease_expires_at is not None
            ):
                raise ValueError("completed memory job requires execution timestamps")
        elif (
            self.started_at is None
            or self.completed_at is None
            or self.error is None
            or self.worker_id is not None
            or self.lease_token is not None
            or self.lease_expires_at is not None
        ):
            raise ValueError("failed memory job requires timestamps and error")
        if self.started_at is not None and self.started_at < self.created_at:
            raise ValueError("memory job start may not precede creation")
        if self.completed_at is not None and (
            self.started_at is None or self.completed_at < self.started_at
        ):
            raise ValueError("memory job completion may not precede start")
        if self.lease_expires_at is not None and (
            self.started_at is None or self.lease_expires_at <= self.started_at
        ):
            raise ValueError("memory job lease must expire after its start")
        return self


def memory_content_hash(content: str) -> str:
    """Return the stable identity for normalized durable memory content."""

    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def run_creation_hash(
    *,
    priority: int,
    priority_class: RunPriorityClass = RunPriorityClass.INTERACTIVE,
) -> str:
    """Return the canonical payload identity for API run idempotency."""

    encoded = json.dumps(
        {"priority": priority, "priority_class": priority_class.value},
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ApprovalDecision",
    "ApprovalStatus",
    "ContextCompactionStatus",
    "IdempotencyKey",
    "MemoryExtractionJob",
    "MemoryExtractionStatus",
    "MemoryKind",
    "PersistedApproval",
    "PersistedContextCompaction",
    "PersistedMemory",
    "PersistedMessage",
    "PersistedTaskPlan",
    "PersistedTaskState",
    "RunCreationResult",
    "TaskPlanUpdate",
    "TaskStatus",
    "TrackedTask",
    "memory_content_hash",
    "run_creation_hash",
]
