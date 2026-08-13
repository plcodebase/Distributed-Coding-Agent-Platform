"""Bounded offline and opt-in live Kubernetes rollout verification harness."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import json
import os
import platform
import re
import tempfile
from collections import deque
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self, cast

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import Any

MAX_REPORT_BYTES = 4 * 1024 * 1024
MAX_TASKS = 10_000
MAX_TICKS = 100_000
_FACTORY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")
type BoundedName = Annotated[
    str,
    StringConstraints(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_-]*$"),
]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        del deep
        values = self.model_dump(mode="python", round_trip=True)
        if update is not None:
            values.update(update)
        return type(self).model_validate(values)


class EvidenceMode(StrEnum):
    SIMULATION = "simulation"
    LIVE = "live"


class RemovalMode(StrEnum):
    GRACEFUL = "graceful"
    ABRUPT = "abrupt"


class DeploymentScenario(_Model):
    name: BoundedName
    accepted_tasks: int = Field(ge=1, le=MAX_TASKS)
    initial_workers: int = Field(ge=1, le=1_000)
    maximum_workers: int = Field(ge=2, le=10_000)
    target_tasks_per_worker: int = Field(ge=1, le=1_000)
    work_ticks: int = Field(ge=1, le=1_000)
    remove_at_tick: int = Field(ge=1, le=MAX_TICKS)
    removal_mode: RemovalMode

    @model_validator(mode="after")
    def validate_worker_range(self) -> Self:
        if self.maximum_workers <= self.initial_workers:
            raise ValueError("maximum_workers must exceed initial_workers")
        return self


class DeploymentSample(_Model):
    tick: int = Field(ge=0, le=MAX_TICKS)
    observed_at: datetime | None = None
    workers: int = Field(ge=0, le=10_000)
    desired_workers: int | None = Field(default=None, ge=0, le=10_000)
    queued: int = Field(ge=0, le=MAX_TASKS)
    queue_metric: int | None = Field(default=None, ge=0, le=MAX_TASKS)
    active: int = Field(ge=0, le=MAX_TASKS)
    completed: int = Field(ge=0, le=MAX_TASKS)
    worker_pods: tuple[BoundedName, ...] = Field(default=(), max_length=1_000)


class DeploymentObservation(_Model):
    accepted_tasks: int = Field(ge=1, le=MAX_TASKS)
    completed_tasks: int = Field(ge=0, le=MAX_TASKS)
    lost_tasks: int = Field(ge=0, le=MAX_TASKS)
    duplicate_completions: int = Field(ge=0, le=MAX_TASKS)
    recovered_tasks: int = Field(ge=0, le=MAX_TASKS)
    initial_workers: int = Field(ge=1, le=1_000)
    peak_workers: int = Field(ge=1, le=10_000)
    removed_worker: bool
    removed_worker_name: BoundedName | None = None
    drained_active_task: bool
    checkpoint_recovery_observed: bool = False
    recovery_seconds: float | None = Field(default=None, ge=0, le=86_400)
    scale_up_observed: bool
    samples: tuple[DeploymentSample, ...] = Field(min_length=1, max_length=MAX_TICKS)

    @model_validator(mode="after")
    def validate_conservation(self) -> Self:
        if self.completed_tasks + self.lost_tasks != self.accepted_tasks:
            raise ValueError("accepted tasks must be completed or explicitly lost")
        if self.peak_workers < self.initial_workers:
            raise ValueError("peak workers may not be below the initial fleet")
        if self.scale_up_observed != (self.peak_workers > self.initial_workers):
            raise ValueError("scale-up flag must match observed worker counts")
        if self.removed_worker != (self.removed_worker_name is not None):
            raise ValueError("removed-worker flag must match its observed pod identity")
        return self


class DeploymentEnvironment(_Model):
    system: str
    release: str
    machine: str
    python: str
    cluster_uid: Annotated[str, StringConstraints(min_length=1, max_length=255)] | None = None
    kubernetes_version: Annotated[str, StringConstraints(min_length=1, max_length=100)] | None = (
        None
    )
    node_summary: Annotated[str, StringConstraints(min_length=1, max_length=2_000)] | None = None

    @model_validator(mode="after")
    def validate_cluster_identity(self) -> Self:
        values = (self.cluster_uid, self.kubernetes_version, self.node_summary)
        if any(value is not None for value in values) and not all(
            value is not None for value in values
        ):
            raise ValueError("live cluster identity fields must be supplied together")
        return self


class DeploymentScenarioResult(_Model):
    scenario: DeploymentScenario
    observation: DeploymentObservation
    passed: bool
    failures: tuple[BoundedName, ...] = Field(max_length=20)

    @model_validator(mode="after")
    def validate_pass_state(self) -> Self:
        if self.observation.accepted_tasks != self.scenario.accepted_tasks:
            raise ValueError("scenario and observation accepted-task counts must match")
        if self.passed == bool(self.failures):
            raise ValueError("a scenario passes exactly when it has no failures")
        return self


class DeploymentReport(_Model):
    report_version: Literal["agent-deployment-v2"] = "agent-deployment-v2"
    mode: EvidenceMode
    result_claim: Literal["simulation_only", "measurement"]
    campaign_id: BoundedName
    source_revision: Annotated[
        str,
        StringConstraints(pattern=r"^(?:[0-9a-f]{7,64}|unavailable)$"),
    ]
    started_at: datetime
    completed_at: datetime
    methodology: tuple[Annotated[str, StringConstraints(min_length=1, max_length=500)], ...] = (
        Field(min_length=1, max_length=20)
    )
    environment: DeploymentEnvironment
    scenarios: tuple[DeploymentScenarioResult, ...] = Field(min_length=2, max_length=20)
    passed: bool

    @model_validator(mode="after")
    def validate_evidence(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("deployment report completion may not precede its start")
        expected_claim = "measurement" if self.mode is EvidenceMode.LIVE else "simulation_only"
        if self.result_claim != expected_claim:
            raise ValueError("result claim does not match deployment evidence mode")
        if self.mode is EvidenceMode.LIVE and self.environment.cluster_uid is None:
            raise ValueError("live deployment evidence requires cluster identity and hardware")
        if self.mode is EvidenceMode.SIMULATION and self.environment.cluster_uid is not None:
            raise ValueError("simulation may not claim a live cluster identity")
        if self.passed != all(item.passed for item in self.scenarios):
            raise ValueError("suite pass state must equal all scenario results")
        modes = {item.scenario.removal_mode for item in self.scenarios}
        if modes != {RemovalMode.GRACEFUL, RemovalMode.ABRUPT}:
            raise ValueError("deployment suite must cover graceful and abrupt removal")
        return self


class DeploymentDriver(Protocol):
    """Trusted adapter for one real cluster or deterministic simulator."""

    mode: EvidenceMode
    environment: DeploymentEnvironment

    async def execute(self, scenario: DeploymentScenario) -> DeploymentObservation:
        """Execute one bounded deployment scenario."""


class SimulationDeploymentDriver:
    """Deterministic state-machine proof; never a performance measurement."""

    mode = EvidenceMode.SIMULATION
    environment = DeploymentEnvironment(
        system=platform.system() or "unknown",
        release=platform.release() or "unknown",
        machine=platform.machine() or "unknown",
        python=platform.python_version(),
    )

    async def execute(self, scenario: DeploymentScenario) -> DeploymentObservation:
        return _simulate(scenario)


def default_scenarios() -> tuple[DeploymentScenario, DeploymentScenario]:
    common = {
        "accepted_tasks": 40,
        "initial_workers": 3,
        "maximum_workers": 12,
        "target_tasks_per_worker": 2,
        "work_ticks": 3,
        "remove_at_tick": 2,
    }
    return (
        DeploymentScenario(name="graceful_worker_drain", removal_mode="graceful", **common),
        DeploymentScenario(name="abrupt_worker_recovery", removal_mode="abrupt", **common),
    )


async def run_deployment_suite(
    driver: DeploymentDriver,
    *,
    campaign_id: str,
    source_revision: str,
    now: Callable[[], datetime] | None = None,
    scenarios: Sequence[DeploymentScenario] | None = None,
) -> DeploymentReport:
    clock = now or (lambda: datetime.now(UTC))
    started_at = clock()
    results: list[DeploymentScenarioResult] = []
    for scenario in scenarios or default_scenarios():
        observation = await driver.execute(scenario)
        failures = _failures(scenario, observation)
        results.append(
            DeploymentScenarioResult(
                scenario=scenario,
                observation=observation,
                passed=not failures,
                failures=failures,
            )
        )
    return DeploymentReport(
        mode=driver.mode,
        result_claim="measurement" if driver.mode is EvidenceMode.LIVE else "simulation_only",
        campaign_id=campaign_id,
        source_revision=source_revision,
        started_at=started_at,
        completed_at=clock(),
        methodology=(
            "Submit a fixed set of durably accepted task identities.",
            "Drive queue-based worker scaling and remove one active worker.",
            "Require every accepted identity to complete once or be explicitly reported lost.",
        ),
        environment=driver.environment,
        scenarios=tuple(results),
        passed=all(item.passed for item in results),
    )


def _simulate(scenario: DeploymentScenario) -> DeploymentObservation:
    queued = deque(range(scenario.accepted_tasks))
    workers: dict[int, tuple[int, int] | None] = dict.fromkeys(range(scenario.initial_workers))
    draining: set[int] = set()
    completed: set[int] = set()
    duplicate_completions = 0
    recovered_tasks = 0
    removed_worker = False
    drained_active_task = False
    next_worker = scenario.initial_workers
    peak_workers = len(workers)
    samples: list[DeploymentSample] = []

    for tick in range(MAX_TICKS):
        duplicates, drained = _advance_workers(workers, draining, completed)
        duplicate_completions += duplicates
        drained_active_task = drained_active_task or drained
        next_worker = _scale_workers(
            workers,
            queued=len(queued),
            scenario=scenario,
            next_worker=next_worker,
        )
        peak_workers = max(peak_workers, len(workers))
        _assign_tasks(
            workers,
            draining,
            queued,
            work_ticks=scenario.work_ticks,
        )

        if tick == scenario.remove_at_tick and workers:
            recovered = _remove_worker(
                workers,
                draining,
                queued,
                mode=scenario.removal_mode,
            )
            removed_worker = True
            recovered_tasks += recovered

        samples.append(
            DeploymentSample(
                tick=tick,
                workers=len(workers),
                desired_workers=len(workers),
                queued=len(queued),
                queue_metric=len(queued),
                active=sum(value is not None for value in workers.values()),
                completed=len(completed),
                worker_pods=tuple(f"worker-{worker_id}" for worker_id in sorted(workers)),
            )
        )
        if len(completed) == scenario.accepted_tasks:
            break

    lost = scenario.accepted_tasks - len(completed)
    return DeploymentObservation(
        accepted_tasks=scenario.accepted_tasks,
        completed_tasks=len(completed),
        lost_tasks=lost,
        duplicate_completions=duplicate_completions,
        recovered_tasks=recovered_tasks,
        initial_workers=scenario.initial_workers,
        peak_workers=peak_workers,
        removed_worker=removed_worker,
        removed_worker_name="worker-0" if removed_worker else None,
        drained_active_task=drained_active_task,
        checkpoint_recovery_observed=(
            scenario.removal_mode is RemovalMode.ABRUPT and recovered_tasks > 0
        ),
        scale_up_observed=peak_workers > scenario.initial_workers,
        samples=tuple(samples),
    )


def _advance_workers(
    workers: dict[int, tuple[int, int] | None],
    draining: set[int],
    completed: set[int],
) -> tuple[int, bool]:
    duplicates = 0
    drained_active = False
    for worker_id, active in tuple(workers.items()):
        if active is None:
            continue
        task_id, remaining = active
        remaining -= 1
        if remaining > 0:
            workers[worker_id] = (task_id, remaining)
            continue
        if task_id in completed:
            duplicates += 1
        completed.add(task_id)
        workers[worker_id] = None
        if worker_id in draining:
            workers.pop(worker_id)
            draining.remove(worker_id)
            drained_active = True
    return duplicates, drained_active


def _scale_workers(
    workers: dict[int, tuple[int, int] | None],
    *,
    queued: int,
    scenario: DeploymentScenario,
    next_worker: int,
) -> int:
    pressure = queued + sum(value is not None for value in workers.values())
    desired = min(
        scenario.maximum_workers,
        max(
            scenario.initial_workers,
            (pressure + scenario.target_tasks_per_worker - 1) // scenario.target_tasks_per_worker,
        ),
    )
    while len(workers) < desired:
        workers[next_worker] = None
        next_worker += 1
    return next_worker


def _assign_tasks(
    workers: dict[int, tuple[int, int] | None],
    draining: set[int],
    queued: deque[int],
    *,
    work_ticks: int,
) -> None:
    for worker_id in sorted(workers):
        if workers[worker_id] is None and worker_id not in draining and queued:
            workers[worker_id] = (queued.popleft(), work_ticks)


def _remove_worker(
    workers: dict[int, tuple[int, int] | None],
    draining: set[int],
    queued: deque[int],
    *,
    mode: RemovalMode,
) -> int:
    candidate = next(
        (worker_id for worker_id, active in workers.items() if active is not None),
        next(iter(workers)),
    )
    if mode is RemovalMode.GRACEFUL and workers[candidate] is not None:
        draining.add(candidate)
        return 0
    active = workers.pop(candidate)
    if active is None:
        return 0
    queued.appendleft(active[0])
    return 1


def _failures(
    scenario: DeploymentScenario,
    observation: DeploymentObservation,
) -> tuple[BoundedName, ...]:
    failures: list[BoundedName] = []
    if observation.accepted_tasks != scenario.accepted_tasks:
        failures.append("accepted_count_mismatch")
    if observation.lost_tasks:
        failures.append("accepted_task_lost")
    if observation.duplicate_completions:
        failures.append("duplicate_completion")
    if not observation.scale_up_observed:
        failures.append("scale_up_not_observed")
    if not observation.removed_worker:
        failures.append("worker_removal_not_observed")
    if scenario.removal_mode is RemovalMode.GRACEFUL and not observation.drained_active_task:
        failures.append("active_task_not_drained")
    if scenario.removal_mode is RemovalMode.ABRUPT and observation.recovered_tasks < 1:
        failures.append("expired_lease_not_recovered")
    return tuple(failures)


def load_driver_factory(specification: str) -> DeploymentDriver:
    """Load an explicitly trusted live-driver factory without provider coupling."""

    if _FACTORY_PATTERN.fullmatch(specification) is None:
        raise ValueError("driver must use a bounded module:attribute reference")
    module_name, _, attribute = specification.partition(":")
    factory = getattr(importlib.import_module(module_name), attribute, None)
    if not callable(factory):
        raise TypeError("deployment driver factory must be callable")
    driver = factory()
    if inspect.isawaitable(driver):
        raise TypeError("deployment driver factories must be synchronous")
    return cast("DeploymentDriver", driver)


def write_report(path: Path, report: DeploymentReport) -> None:
    """Atomically write one bounded report without following a destination symlink."""

    destination = path.absolute()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError("deployment report destination may not be a symlink")
    payload = report.model_dump_json(indent=2).encode("utf-8") + b"\n"
    if len(payload) > MAX_REPORT_BYTES:
        raise ValueError("deployment report exceeds the byte limit")
    descriptor, temporary = tempfile.mkstemp(prefix=".deployment-report-", dir=destination.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        Path(temporary).replace(destination)
    finally:
        Path(temporary).unlink(missing_ok=True)


async def _run(arguments: argparse.Namespace) -> int:
    driver = (
        load_driver_factory(arguments.driver)
        if arguments.driver is not None
        else SimulationDeploymentDriver()
    )
    if arguments.driver is None and arguments.mode != EvidenceMode.SIMULATION.value:
        raise ValueError("live mode requires an explicit trusted driver")
    if driver.mode.value != arguments.mode:
        raise ValueError("driver evidence mode does not match --mode")
    report = await run_deployment_suite(
        driver,
        campaign_id=arguments.campaign_id,
        source_revision=arguments.source_revision,
    )
    write_report(arguments.output, report)
    print(json.dumps({"passed": report.passed, "mode": report.mode.value}))  # noqa: T201
    return 0 if report.passed else 1


def main(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", choices=[item.value for item in EvidenceMode], default="simulation"
    )
    parser.add_argument("--driver")
    parser.add_argument("--campaign-id", default="deployment-simulation")
    parser.add_argument("--source-revision", default="unavailable")
    parser.add_argument("--output", type=Path, required=True)
    raise SystemExit(asyncio.run(_run(parser.parse_args(arguments))))


if __name__ == "__main__":
    main()
