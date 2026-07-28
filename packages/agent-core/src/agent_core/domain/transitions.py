"""Central run-state transition policy and state update operation."""

from collections.abc import Mapping
from datetime import datetime

from agent_core.domain.base import normalize_timestamp
from agent_core.domain.errors import DomainOperationError, InvalidRunTransitionError
from agent_core.domain.models import Run
from agent_core.domain.status import RunStatus

RUN_STATUS_TRANSITIONS: Mapping[RunStatus, frozenset[RunStatus]] = {
    RunStatus.QUEUED: frozenset({RunStatus.LEASED, RunStatus.CANCELLED}),
    RunStatus.LEASED: frozenset(
        {RunStatus.RUNNING, RunStatus.QUEUED, RunStatus.CANCELLED, RunStatus.LOST}
    ),
    RunStatus.RUNNING: frozenset(
        {
            RunStatus.WAITING_APPROVAL,
            RunStatus.RETRY_PENDING,
            RunStatus.COMPLETED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.LOST,
        }
    ),
    RunStatus.WAITING_APPROVAL: frozenset(
        {RunStatus.RUNNING, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.LOST}
    ),
    RunStatus.RETRY_PENDING: frozenset(
        {RunStatus.RUNNING, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.LOST}
    ),
    RunStatus.LOST: frozenset({RunStatus.QUEUED, RunStatus.CANCELLED}),
    RunStatus.COMPLETED: frozenset(),
    RunStatus.FAILED: frozenset(),
    RunStatus.CANCELLED: frozenset(),
}


def _operation_timestamp(
    value: datetime,
    *,
    run: Run,
    error_code: str,
) -> datetime:
    try:
        return normalize_timestamp(value)
    except ValueError as error:
        raise DomainOperationError(
            code=error_code,
            message=str(error),
            details={"run_id": str(run.id)},
        ) from error


def allowed_run_transitions(status: RunStatus) -> frozenset[RunStatus]:
    """Return the immutable set of valid successor states."""

    return RUN_STATUS_TRANSITIONS[status]


def transition_run(
    run: Run,
    new_status: RunStatus,
    *,
    occurred_at: datetime,
    worker_id: str | None = None,
    lease_expires_at: datetime | None = None,
) -> Run:
    """Return a new validated run after applying one permitted state transition."""

    allowed = allowed_run_transitions(run.status)
    if new_status not in allowed:
        raise InvalidRunTransitionError(run.status, new_status, allowed)

    timestamp = _operation_timestamp(
        occurred_at,
        run=run,
        error_code="invalid_run_transition_timestamp",
    )
    comparison_start = run.started_at or run.created_at
    if timestamp < comparison_start:
        raise DomainOperationError(
            code="invalid_run_transition_timestamp",
            message="transition timestamp may not precede the current run lifecycle",
            details={"run_id": str(run.id)},
        )
    data = run.model_dump(mode="python")
    data["status"] = new_status

    if new_status is RunStatus.LEASED:
        normalized_worker_id = worker_id.strip() if worker_id is not None else ""
        if not normalized_worker_id or lease_expires_at is None:
            raise DomainOperationError(
                code="invalid_run_lease",
                message="leasing a run requires worker_id and lease_expires_at",
                details={"run_id": str(run.id)},
            )
        normalized_expiry = _operation_timestamp(
            lease_expires_at,
            run=run,
            error_code="invalid_run_lease",
        )
        if normalized_expiry <= timestamp:
            raise DomainOperationError(
                code="invalid_run_lease",
                message="lease_expires_at must follow the lease timestamp",
                details={"run_id": str(run.id)},
            )
        data["assigned_worker_id"] = normalized_worker_id
        data["lease_expires_at"] = normalized_expiry
    elif new_status is RunStatus.RUNNING:
        data["started_at"] = run.started_at or timestamp
    elif new_status is RunStatus.QUEUED:
        data["assigned_worker_id"] = None
        data["lease_expires_at"] = None
        if run.status is RunStatus.LOST:
            data["attempt"] = run.attempt + 1
    elif new_status is RunStatus.LOST:
        data["lease_expires_at"] = None
    elif new_status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}:
        data["completed_at"] = timestamp
        data["lease_expires_at"] = None
        if new_status is RunStatus.CANCELLED:
            data["cancellation_requested"] = True

    return Run.model_validate(data)
