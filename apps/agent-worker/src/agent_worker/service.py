"""Bounded distributed worker lifecycle and lease-heartbeat orchestration."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING, Self, TypeVar

from pydantic import Field, model_validator

from agent_core.distributed import (
    RecoveryStore,
    RunExecutionResult,
    RunExecutor,
    RunLease,
    RunQueue,
    RunRecoveryState,
    WorkerRegistration,
    WorkerStatus,
    WorkspaceLeaseStore,
    WorkspaceRestorer,
    WorkspaceWriterLease,
)
from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError, ErrorDetail
from agent_core.domain.models import (  # noqa: TC001 - Pydantic resolves fields at runtime
    IdentifierString,
)
from agent_core.domain.status import RunStatus

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agent_core.loop import Clock

type Sleep = Callable[[float], Awaitable[None]]
_PhaseResult = TypeVar("_PhaseResult")


class WorkerConfig(DomainModel):
    """Hard bounds for one worker process."""

    worker_id: IdentifierString
    supported_sandbox_types: tuple[IdentifierString, ...] = Field(
        default=("podman",),
        min_length=1,
        max_length=32,
    )
    total_slots: int = Field(default=1, ge=1, le=128)
    sandbox_slots: int = Field(default=1, ge=1, le=128)
    lease_seconds: float = Field(default=30, gt=1, le=3600)
    heartbeat_seconds: float = Field(default=5, gt=0.1, le=300)
    poll_seconds: float = Field(default=0.25, ge=0.01, le=10)

    @model_validator(mode="after")
    def validate_timing(self) -> Self:
        if self.heartbeat_seconds * 2 >= self.lease_seconds:
            raise ValueError("heartbeat_seconds must be less than half lease_seconds")
        if self.sandbox_slots > self.total_slots:
            raise ValueError("sandbox_slots may not exceed total_slots")
        if len(set(self.supported_sandbox_types)) != len(self.supported_sandbox_types):
            raise ValueError("supported_sandbox_types must be unique")
        return self


class WorkerService:
    """Claim, recover, heartbeat, cancel, and drain bounded worker attempts."""

    def __init__(
        self,
        *,
        config: WorkerConfig,
        queue: RunQueue,
        workspace_leases: WorkspaceLeaseStore,
        recovery: RecoveryStore,
        restorer: WorkspaceRestorer,
        executor: RunExecutor,
        clock: Clock,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._config = config
        self._queue = queue
        self._workspace_leases = workspace_leases
        self._recovery = recovery
        self._restorer = restorer
        self._executor = executor
        self._clock = clock
        self._sleep = sleep
        self._active: set[asyncio.Task[None]] = set()
        self._sandbox_slots = asyncio.BoundedSemaphore(config.sandbox_slots)
        self._fatal_error: BaseException | None = None
        self._draining = False
        self._registered = False
        self._lifecycle_lock = asyncio.Lock()

    @property
    def active_count(self) -> int:
        return len(self._active)

    @property
    def draining(self) -> bool:
        return self._draining

    async def register(self) -> WorkerRegistration:
        async with self._lifecycle_lock:
            if self._registered:
                return await self._heartbeat_worker()
            now = self._clock.now()
            registration = await self._queue.register_worker(
                WorkerRegistration(
                    worker_id=self._config.worker_id,
                    supported_sandbox_types=self._config.supported_sandbox_types,
                    total_slots=self._config.total_slots,
                    available_slots=self._config.total_slots,
                    status=WorkerStatus.ACTIVE,
                    registered_at=now,
                    last_heartbeat_at=now,
                )
            )
            self._registered = True
            return registration

    async def run_once(self) -> bool:
        """Claim at most one run and start its processing task."""

        if not self._registered:
            await self.register()
        self._collect_finished()
        self._raise_fatal_error()
        await self._heartbeat_worker()
        if self._draining or self.active_count >= self._config.total_slots:
            return False
        lease = await self._queue.claim(
            self._config.worker_id,
            occurred_at=self._clock.now(),
            lease_duration=self._lease_duration,
        )
        if lease is None:
            return False
        task = asyncio.create_task(
            self._process(lease),
            name=f"agent-run-{lease.run_id}",
        )
        self._active.add(task)
        task.add_done_callback(self._observe_task)
        return True

    async def serve(self, stop: asyncio.Event) -> None:
        """Poll until stopped, then drain already-owned work."""

        await self.register()
        try:
            while not stop.is_set():
                claimed = await self.run_once()
                if not claimed:
                    await self._sleep(self._config.poll_seconds)
        except BaseException:
            with suppress(BaseException):
                await self.drain()
            raise
        else:
            await self.drain()

    async def drain(self) -> None:
        """Stop new claims and wait for all active work to release its lease."""

        if not self._registered:
            self._draining = True
            return
        if not self._draining:
            self._draining = True
            await self._queue.set_worker_draining(
                self._config.worker_id,
                draining=True,
                occurred_at=self._clock.now(),
            )
        if self._active:
            await asyncio.gather(*tuple(self._active), return_exceptions=True)
        await self._heartbeat_worker()
        self._raise_fatal_error()

    async def resume(self) -> None:
        if not self._registered:
            await self.register()
        self._draining = False
        await self._queue.set_worker_draining(
            self._config.worker_id,
            draining=False,
            occurred_at=self._clock.now(),
        )

    async def _process(self, original_lease: RunLease) -> None:
        lease = original_lease
        heartbeat: asyncio.Task[None] | None = None
        workspace_lease: WorkspaceWriterLease | None = None
        sandbox_slot = False
        try:
            lease = await self._queue.start(lease, occurred_at=self._clock.now())
            if lease.cancellation_requested:
                await self._queue.finish(
                    lease,
                    RunExecutionResult(status=RunStatus.CANCELLED),
                    occurred_at=self._clock.now(),
                )
                return
            workspace_lease = self._require_workspace_lease(
                await self._workspace_leases.acquire(
                    lease,
                    occurred_at=self._clock.now(),
                    lease_duration=self._lease_duration,
                ),
                lease,
            )
            heartbeat = asyncio.create_task(
                self._heartbeat_lease(lease, workspace_lease),
                name=f"agent-heartbeat-{lease.run_id}",
            )
            recovery = await self._wait_phase_or_heartbeat(
                asyncio.create_task(
                    self._recovery.load(lease),
                    name=f"agent-recovery-{lease.run_id}",
                ),
                heartbeat,
            )
            await self._wait_phase_or_heartbeat(
                asyncio.create_task(
                    self._sandbox_slots.acquire(),
                    name=f"sandbox-capacity-{lease.run_id}",
                ),
                heartbeat,
            )
            sandbox_slot = True
            if recovery.checkpoint is not None:
                await self._wait_phase_or_heartbeat(
                    asyncio.create_task(
                        self._restorer.restore(
                            lease,
                            recovery.checkpoint,
                            writer_lease=workspace_lease,
                            workspace_revision=self._require_restore_revision(recovery),
                        ),
                        name=f"agent-restore-{lease.run_id}",
                    ),
                    heartbeat,
                )
            execution = asyncio.create_task(
                self._executor.execute(lease, workspace_lease, recovery),
                name=f"agent-execution-{lease.run_id}",
            )
            result = await self._wait_phase_or_heartbeat(execution, heartbeat)
            await self._queue.finish(
                lease,
                result,
                occurred_at=self._clock.now(),
            )
        except asyncio.CancelledError:
            await self._executor.cancel(lease)
            raise
        except DomainOperationError as error:
            if error.code in {"run_lease_lost", "run_lease_expired"}:
                await self._executor.cancel(lease)
                return
            if error.code == "run_cancellation_requested":
                await self._executor.cancel(lease)
                await self._finish_cancelled_if_owned(lease)
                return
            await self._finish_failure_if_owned(lease, error.error)
        except Exception:
            failure = DomainOperationError(
                code="worker_execution_failed",
                message="the worker could not complete the run attempt",
                retryable=True,
            )
            await self._finish_failure_if_owned(
                lease,
                failure.error,
            )
            raise failure from None
        finally:
            if sandbox_slot:
                self._sandbox_slots.release()
            if heartbeat is not None:
                heartbeat.cancel()
                with suppress(asyncio.CancelledError, DomainOperationError):
                    await heartbeat
            if workspace_lease is not None:
                with suppress(DomainOperationError):
                    await self._workspace_leases.release(workspace_lease)

    @staticmethod
    async def _wait_phase_or_heartbeat(
        operation: asyncio.Task[_PhaseResult],
        heartbeat: asyncio.Task[None],
    ) -> _PhaseResult:
        done, _ = await asyncio.wait(
            {operation, heartbeat},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if heartbeat in done:
            error = heartbeat.exception()
            operation.cancel()
            with suppress(asyncio.CancelledError):
                await operation
            if error is not None:
                raise error
            raise DomainOperationError(
                code="run_lease_lost",
                message="the lease heartbeat stopped before the worker phase completed",
                retryable=True,
            )
        return operation.result()

    async def _heartbeat_lease(
        self,
        lease: RunLease,
        workspace_lease: WorkspaceWriterLease,
    ) -> None:
        current_workspace = workspace_lease
        while True:
            await self._sleep(self._config.heartbeat_seconds)
            heartbeat = await self._queue.heartbeat(
                lease,
                occurred_at=self._clock.now(),
                lease_duration=self._lease_duration,
            )
            if heartbeat.cancellation_requested:
                await self._executor.cancel(lease)
                raise DomainOperationError(
                    code="run_cancellation_requested",
                    message="distributed cancellation was requested for the run",
                    retryable=False,
                    details={"run_id": str(lease.run_id)},
                )
            current_workspace = await self._workspace_leases.heartbeat(
                current_workspace,
                occurred_at=self._clock.now(),
                lease_duration=self._lease_duration,
            )

    async def _finish_cancelled_if_owned(self, lease: RunLease) -> None:
        try:
            await self._queue.finish(
                lease,
                RunExecutionResult(status=RunStatus.CANCELLED),
                occurred_at=self._clock.now(),
            )
        except DomainOperationError as finish_error:
            if finish_error.code not in {"run_lease_lost", "run_lease_expired"}:
                raise

    async def _finish_failure_if_owned(
        self,
        lease: RunLease,
        error: ErrorDetail,
    ) -> None:
        try:
            await self._queue.finish(
                lease,
                RunExecutionResult(status=RunStatus.FAILED, error=error),
                occurred_at=self._clock.now(),
            )
        except DomainOperationError as finish_error:
            if finish_error.code not in {"run_lease_lost", "run_lease_expired"}:
                raise

    @staticmethod
    def _require_workspace_lease(
        workspace_lease: WorkspaceWriterLease | None,
        run_lease: RunLease,
    ) -> WorkspaceWriterLease:
        if workspace_lease is None:
            raise DomainOperationError(
                code="workspace_lease_conflict",
                message="the claimed run does not own its workspace writer lease",
                retryable=True,
                details={"workspace_id": str(run_lease.workspace_id)},
            )
        if (
            workspace_lease.tenant_id != run_lease.tenant_id
            or workspace_lease.workspace_id != run_lease.workspace_id
            or workspace_lease.run_id != run_lease.run_id
            or workspace_lease.worker_id != run_lease.worker_id
            or workspace_lease.run_lease_token != run_lease.lease_token
            or workspace_lease.expires_at > run_lease.expires_at
        ):
            raise DomainOperationError(
                code="workspace_lease_invalid",
                message="the workspace writer lease does not match the active run lease",
                retryable=True,
                details={"workspace_id": str(run_lease.workspace_id)},
            )
        return workspace_lease

    @staticmethod
    def _require_restore_revision(recovery: RunRecoveryState) -> str:
        revision = recovery.workspace_restore_revision
        if revision is None:
            raise DomainOperationError(
                code="recovery_state_invalid",
                message="checkpoint recovery is missing its workspace revision",
            )
        return revision

    async def _heartbeat_worker(self) -> WorkerRegistration:
        return await self._queue.heartbeat_worker(
            self._config.worker_id,
            occurred_at=self._clock.now(),
            available_slots=max(0, self._config.total_slots - self.active_count),
        )

    def _collect_finished(self) -> None:
        for task in tuple(self._active):
            if task.done():
                self._observe_task(task)

    def _observe_task(self, task: asyncio.Task[None]) -> None:
        self._active.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and self._fatal_error is None:
            self._fatal_error = error

    def _raise_fatal_error(self) -> None:
        if self._fatal_error is not None:
            raise self._fatal_error

    @property
    def _lease_duration(self) -> timedelta:
        return timedelta(seconds=self._config.lease_seconds)


__all__ = ["WorkerConfig", "WorkerService"]
