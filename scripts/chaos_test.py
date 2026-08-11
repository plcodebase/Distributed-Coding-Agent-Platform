"""Bounded deterministic and opt-in Podman chaos harness for the agent platform."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import math
import os
import platform
import re
import stat
import sys
import tempfile
import time
from contextlib import suppress
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self, cast

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    model_validator,
)

from sandbox_runtime import BoundedProcessRunner, ProcessResult

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    type ChaosDriverFactory = Callable[[], ChaosDriver | Awaitable[ChaosDriver]]

MAX_SCENARIO_BYTES = 1024 * 1024
MAX_REPORT_BYTES = 16 * 1024 * 1024
MAX_CHAOS_SCENARIOS = 100
MAX_RECOVERY_SECONDS = 3_600.0
MAX_CLEANUP_SECONDS = 120.0
MAX_PODMAN_OUTPUT_BYTES = 1024 * 1024
MAX_PODMAN_TIMEOUT_SECONDS = 300.0
MAX_CONTAINER_NAME_LENGTH = 63
_FACTORY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")
type BoundedName = Annotated[
    str,
    StringConstraints(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_-]*$"),
]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class ChaosScenario(StrEnum):
    WORKER_TERMINATION = "worker_termination"
    REDIS_RESTART = "redis_restart"
    POSTGRES_INTERRUPTION = "postgres_interruption"
    PRIMARY_PROVIDER_DISABLED = "primary_provider_disabled"
    GATEWAY_RATE_LIMIT = "gateway_rate_limit"
    WEBSOCKET_DISCONNECT = "websocket_disconnect"
    SANDBOX_OOM = "sandbox_oom"
    DUPLICATE_TASK_DELIVERY = "duplicate_task_delivery"
    DUPLICATE_TOOL_CALL = "duplicate_tool_call"
    CHECKPOINT_INTERRUPTION = "checkpoint_interruption"


class FaultAction(StrEnum):
    TERMINATE = "terminate"
    RESTART = "restart"
    BLOCK = "block"
    DISABLE = "disable"
    RETURN_429 = "return_429"
    DISCONNECT = "disconnect"
    FORCE_OOM = "force_oom"
    DUPLICATE_TASK = "duplicate_task"
    DUPLICATE_TOOL = "duplicate_tool"
    INTERRUPT_CHECKPOINT = "interrupt_checkpoint"


class FinalRunState(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY_PENDING = "retry_pending"
    RUNNING = "running"


class EvidenceSignal(StrEnum):
    QUEUE_RECOVERY = "queue_recovery"
    WORKER_RECOVERY = "worker_recovery"
    PROVIDER_FALLBACK = "provider_fallback"
    GATEWAY_RETRY = "gateway_retry"
    API_ERROR = "api_error"
    EVENT_REPLAY = "event_replay"
    SANDBOX_FAILURE = "sandbox_failure"
    TOOL_REPLAY = "tool_replay"
    CHECKPOINT_RECOVERY = "checkpoint_recovery"


class EvidenceSource(StrEnum):
    SIMULATION = "simulation"
    PROMETHEUS = "prometheus"


class InvariantFailure(StrEnum):
    FAULT_NOT_OBSERVED = "fault_not_observed"
    RECOVERY_NOT_OBSERVED = "recovery_not_observed"
    RECOVERY_TIMEOUT = "recovery_timeout"
    ACCEPTED_TASK_MISSING = "accepted_task_missing"
    DUPLICATE_COMMITTED_CHANGE = "duplicate_committed_change"
    DURABLE_EVENT_GAP = "durable_event_gap"
    UNEXPECTED_FINAL_STATE = "unexpected_final_state"
    UNEXPECTED_RETRY_CATEGORY = "unexpected_retry_category"
    DASHBOARD_EVIDENCE_MISSING = "dashboard_evidence_missing"
    HARNESS_ERROR = "harness_error"
    CLEANUP_FAILED = "cleanup_failed"
    FAULT_INJECTION_TIMEOUT = "fault_injection_timeout"


_EXPECTED_ACTION = {
    ChaosScenario.WORKER_TERMINATION: FaultAction.TERMINATE,
    ChaosScenario.REDIS_RESTART: FaultAction.RESTART,
    ChaosScenario.POSTGRES_INTERRUPTION: FaultAction.BLOCK,
    ChaosScenario.PRIMARY_PROVIDER_DISABLED: FaultAction.DISABLE,
    ChaosScenario.GATEWAY_RATE_LIMIT: FaultAction.RETURN_429,
    ChaosScenario.WEBSOCKET_DISCONNECT: FaultAction.DISCONNECT,
    ChaosScenario.SANDBOX_OOM: FaultAction.FORCE_OOM,
    ChaosScenario.DUPLICATE_TASK_DELIVERY: FaultAction.DUPLICATE_TASK,
    ChaosScenario.DUPLICATE_TOOL_CALL: FaultAction.DUPLICATE_TOOL,
    ChaosScenario.CHECKPOINT_INTERRUPTION: FaultAction.INTERRUPT_CHECKPOINT,
}

_EXPECTED_TARGET = {
    ChaosScenario.WORKER_TERMINATION: "active_worker",
    ChaosScenario.REDIS_RESTART: "redis",
    ChaosScenario.POSTGRES_INTERRUPTION: "postgres",
    ChaosScenario.PRIMARY_PROVIDER_DISABLED: "primary_provider",
    ChaosScenario.GATEWAY_RATE_LIMIT: "gateway_route",
    ChaosScenario.WEBSOCKET_DISCONNECT: "event_client",
    ChaosScenario.SANDBOX_OOM: "sandbox_process",
    ChaosScenario.DUPLICATE_TASK_DELIVERY: "durable_task",
    ChaosScenario.DUPLICATE_TOOL_CALL: "tool_delivery",
    ChaosScenario.CHECKPOINT_INTERRUPTION: "checkpoint_worker",
}


class FaultDefinition(_Model):
    action: FaultAction
    target: BoundedName


class ExpectedOutcome(_Model):
    allowed_final_states: tuple[FinalRunState, ...] = Field(min_length=1, max_length=4)
    accepted_task_visible: Literal[True] = True
    max_committed_changes: int = Field(default=1, ge=0, le=1)
    durable_event_continuity: Literal[True] = True
    retry_categories: tuple[BoundedName, ...] = Field(default=(), max_length=10)
    required_signals: tuple[EvidenceSignal, ...] = Field(min_length=1, max_length=10)

    @model_validator(mode="after")
    def validate_unique_values(self) -> Self:
        if len(set(self.allowed_final_states)) != len(self.allowed_final_states):
            raise ValueError("allowed final states must be unique")
        return self


class ChaosScenarioSpec(_Model):
    name: BoundedName
    scenario: ChaosScenario
    fault: FaultDefinition
    fault_timeout_seconds: float = Field(default=30, gt=0, le=MAX_PODMAN_TIMEOUT_SECONDS)
    recovery_timeout_seconds: float = Field(gt=0, le=MAX_RECOVERY_SECONDS)
    cleanup_timeout_seconds: float = Field(default=30, gt=0, le=MAX_CLEANUP_SECONDS)
    preconditions: tuple[Annotated[str, StringConstraints(min_length=1, max_length=500)], ...] = (
        Field(min_length=1, max_length=20)
    )
    expected: ExpectedOutcome

    @model_validator(mode="after")
    def validate_fault_action(self) -> Self:
        if self.fault.action is not _EXPECTED_ACTION[self.scenario]:
            raise ValueError("fault action does not match the selected chaos scenario")
        if self.fault.target != _EXPECTED_TARGET[self.scenario]:
            raise ValueError("fault target does not match the selected chaos scenario")
        if len(set(self.expected.required_signals)) != len(self.expected.required_signals):
            raise ValueError("required evidence signals must be unique")
        if len(set(self.expected.retry_categories)) != len(self.expected.retry_categories):
            raise ValueError("expected retry categories must be unique")
        return self


class ChaosObservation(_Model):
    fault_observed: bool
    recovered: bool
    accepted_task_visible: bool
    final_state: FinalRunState
    committed_changes: int = Field(ge=0, le=1_000_000)
    durable_event_count: int = Field(ge=0, le=1_000_000_000)
    durable_event_contiguous: bool
    retry_category: BoundedName | None = None
    recovery_seconds: float = Field(ge=0, le=MAX_RECOVERY_SECONDS)
    evidence: tuple[SignalEvidence, ...] = Field(max_length=20)

    @model_validator(mode="after")
    def validate_signals(self) -> Self:
        signals = tuple(item.signal for item in self.evidence)
        if len(set(signals)) != len(signals):
            raise ValueError("observed evidence signals must be unique")
        return self


class SignalEvidence(_Model):
    signal: EvidenceSignal
    source: EvidenceSource
    observed_value: float = Field(gt=0)


class ChaosScenarioResult(_Model):
    scenario: ChaosScenarioSpec
    started_at: datetime
    completed_at: datetime
    observation: ChaosObservation | None
    failures: tuple[InvariantFailure, ...]
    error_category: BoundedName | None = None
    passed: bool

    @model_validator(mode="after")
    def validate_pass_state(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("chaos scenario completion may not precede its start")
        if len(set(self.failures)) != len(self.failures):
            raise ValueError("chaos invariant failures must be unique")
        if self.passed == bool(self.failures):
            raise ValueError("a chaos result passes exactly when it has no invariant failures")
        if self.observation is None and self.error_category is None:
            raise ValueError("a missing observation requires an opaque error category")
        return self


class ChaosEnvironment(_Model):
    system: str
    release: str
    machine: str
    python: str
    logical_cpus: int | None = Field(default=None, ge=1)
    physical_memory_bytes: int | None = Field(default=None, ge=1)


class ChaosSourceState(_Model):
    revision: Annotated[
        str,
        StringConstraints(pattern=r"^(?:[0-9a-f]{7,64}|unavailable)$"),
    ]
    dirty: bool | None


class ChaosReport(_Model):
    report_version: Literal["agent-chaos-v1"] = "agent-chaos-v1"
    synthetic: bool
    result_claim: Literal["measurement", "simulation_only"]
    started_at: datetime
    completed_at: datetime
    methodology: tuple[str, ...]
    environment: ChaosEnvironment
    source: ChaosSourceState
    results: tuple[ChaosScenarioResult, ...] = Field(min_length=1, max_length=MAX_CHAOS_SCENARIOS)

    @model_validator(mode="after")
    def validate_report(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("chaos report completion may not precede its start")
        expected_claim = "simulation_only" if self.synthetic else "measurement"
        if self.result_claim != expected_claim:
            raise ValueError("chaos report claim must match its synthetic state")
        names = tuple(result.scenario.name for result in self.results)
        if len(set(names)) != len(names):
            raise ValueError("chaos report scenarios must be uniquely named")
        if not self.synthetic and any(
            evidence.source is not EvidenceSource.PROMETHEUS
            for result in self.results
            if result.passed and result.observation is not None
            for evidence in result.observation.evidence
        ):
            raise ValueError("passing live results require Prometheus evidence")
        return self


class ChaosDriver(Protocol):
    @property
    def mode(self) -> Literal["live", "simulation"]: ...

    async def inject(self, scenario: ChaosScenarioSpec) -> None: ...

    async def recover(self, scenario: ChaosScenarioSpec) -> None: ...

    async def observe(self, scenario: ChaosScenarioSpec) -> ChaosObservation: ...

    async def cleanup(self, scenario: ChaosScenarioSpec) -> None: ...


class DeterministicChaosDriver:
    """Stateful CI simulation for harness and invariant validation only."""

    mode: Literal["simulation"] = "simulation"

    def __init__(self) -> None:
        self._injected: set[str] = set()
        self._recovered: set[str] = set()
        self.cleaned: list[str] = []

    async def inject(self, scenario: ChaosScenarioSpec) -> None:
        await asyncio.sleep(0)
        self._injected.add(scenario.name)

    async def recover(self, scenario: ChaosScenarioSpec) -> None:
        await asyncio.sleep(0)
        if scenario.name not in self._injected:
            raise RuntimeError("fault was not injected")
        self._recovered.add(scenario.name)

    async def observe(self, scenario: ChaosScenarioSpec) -> ChaosObservation:
        await asyncio.sleep(0)
        expected = scenario.expected
        return ChaosObservation(
            fault_observed=scenario.name in self._injected,
            recovered=scenario.name in self._recovered,
            accepted_task_visible=True,
            final_state=expected.allowed_final_states[0],
            committed_changes=min(1, expected.max_committed_changes),
            durable_event_count=3,
            durable_event_contiguous=True,
            retry_category=expected.retry_categories[0] if expected.retry_categories else None,
            recovery_seconds=min(0.01, scenario.recovery_timeout_seconds),
            evidence=tuple(
                SignalEvidence(
                    signal=signal,
                    source=EvidenceSource.SIMULATION,
                    observed_value=1,
                )
                for signal in expected.required_signals
            ),
        )

    async def cleanup(self, scenario: ChaosScenarioSpec) -> None:
        await asyncio.sleep(0)
        self.cleaned.append(scenario.name)


class ChaosRunner:
    """Execute chaos scenarios serially and evaluate outcomes, not command success."""

    async def run(
        self,
        scenarios: Sequence[ChaosScenarioSpec],
        driver: ChaosDriver,
    ) -> ChaosReport:
        if not scenarios or len(scenarios) > MAX_CHAOS_SCENARIOS:
            raise ValueError("chaos suite must contain between 1 and 100 scenarios")
        if len({scenario.name for scenario in scenarios}) != len(scenarios):
            raise ValueError("chaos scenarios must be uniquely named")
        started_at = datetime.now(UTC)
        results = [await self._run_one(scenario, driver) for scenario in scenarios]
        return ChaosReport(
            synthetic=driver.mode == "simulation",
            result_claim="simulation_only" if driver.mode == "simulation" else "measurement",
            started_at=started_at,
            completed_at=datetime.now(UTC),
            methodology=(
                "Scenarios execute serially so infrastructure faults cannot overlap.",
                (
                    "A successful fault command is insufficient; every expected "
                    "invariant is evaluated."
                ),
                (
                    "Cleanup has an independent deadline and runs after success, "
                    "failure, or cancellation."
                ),
                (
                    "Synthetic observations validate harness behavior only."
                    if driver.mode == "simulation"
                    else "Live observations were collected from the configured test deployment."
                ),
            ),
            environment=_environment(),
            source=_source_state(),
            results=tuple(results),
        )

    async def _run_one(
        self,
        scenario: ChaosScenarioSpec,
        driver: ChaosDriver,
    ) -> ChaosScenarioResult:
        started_at = datetime.now(UTC)
        observation: ChaosObservation | None = None
        error_category: BoundedName | None = None
        cancelled: asyncio.CancelledError | None = None
        cleanup_failed = False
        phase_failure: InvariantFailure | None = None
        try:
            try:
                observation, error_category, phase_failure = await self._execute_scenario(
                    scenario, driver
                )
            except asyncio.CancelledError as error:
                cancelled = error
        finally:
            cleanup_task = asyncio.create_task(
                self._cleanup(driver, scenario),
                name=f"chaos-cleanup-{scenario.name}",
            )
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as error:
                cancelled = cancelled or error
                try:
                    await cleanup_task
                except Exception:
                    cleanup_failed = True
            except Exception:
                cleanup_failed = True
        if cancelled is not None:
            raise cancelled
        failures = list(_evaluate(scenario, observation))
        if phase_failure is not None:
            failures.append(phase_failure)
        if cleanup_failed:
            failures.append(InvariantFailure.CLEANUP_FAILED)
            error_category = error_category or "cleanup_failed"
        if observation is None:
            failures.append(InvariantFailure.HARNESS_ERROR)
            error_category = error_category or "driver_error"
        unique_failures = tuple(dict.fromkeys(failures))
        return ChaosScenarioResult(
            scenario=scenario,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            observation=observation,
            failures=unique_failures,
            error_category=error_category,
            passed=not unique_failures,
        )

    @staticmethod
    async def _execute_scenario(
        scenario: ChaosScenarioSpec,
        driver: ChaosDriver,
    ) -> tuple[
        ChaosObservation | None,
        BoundedName | None,
        InvariantFailure | None,
    ]:
        try:
            async with asyncio.timeout(scenario.fault_timeout_seconds):
                await driver.inject(scenario)
        except TimeoutError:
            return None, "fault_timeout", InvariantFailure.FAULT_INJECTION_TIMEOUT
        except Exception:
            return None, "driver_error", None

        recovery_started = time.monotonic()
        try:
            async with asyncio.timeout(scenario.recovery_timeout_seconds):
                await driver.recover(scenario)
                observation = await driver.observe(scenario)
            measured = ChaosObservation.model_validate(
                {
                    **observation.model_dump(),
                    "recovery_seconds": time.monotonic() - recovery_started,
                }
            )
        except TimeoutError:
            return None, "recovery_timeout", InvariantFailure.RECOVERY_TIMEOUT
        except Exception:
            return None, "driver_error", None
        return measured, None, None

    @staticmethod
    async def _cleanup(driver: ChaosDriver, scenario: ChaosScenarioSpec) -> None:
        async with asyncio.timeout(scenario.cleanup_timeout_seconds):
            await driver.cleanup(scenario)


def _evaluate(
    scenario: ChaosScenarioSpec,
    observation: ChaosObservation | None,
) -> tuple[InvariantFailure, ...]:
    if observation is None:
        return ()
    expected = scenario.expected
    failures: list[InvariantFailure] = []
    if not observation.fault_observed:
        failures.append(InvariantFailure.FAULT_NOT_OBSERVED)
    if not observation.recovered:
        failures.append(InvariantFailure.RECOVERY_NOT_OBSERVED)
    if observation.recovery_seconds > scenario.recovery_timeout_seconds:
        failures.append(InvariantFailure.RECOVERY_TIMEOUT)
    if expected.accepted_task_visible and not observation.accepted_task_visible:
        failures.append(InvariantFailure.ACCEPTED_TASK_MISSING)
    if observation.committed_changes > expected.max_committed_changes:
        failures.append(InvariantFailure.DUPLICATE_COMMITTED_CHANGE)
    if expected.durable_event_continuity and (
        not observation.durable_event_contiguous or observation.durable_event_count == 0
    ):
        failures.append(InvariantFailure.DURABLE_EVENT_GAP)
    if observation.final_state not in expected.allowed_final_states:
        failures.append(InvariantFailure.UNEXPECTED_FINAL_STATE)
    if expected.retry_categories and observation.retry_category not in expected.retry_categories:
        failures.append(InvariantFailure.UNEXPECTED_RETRY_CATEGORY)
    observed_signals = {item.signal for item in observation.evidence}
    if not set(expected.required_signals).issubset(observed_signals):
        failures.append(InvariantFailure.DASHBOARD_EVIDENCE_MISSING)
    return tuple(failures)


class ChaosObservationProbe(Protocol):
    async def observe(self, scenario: ChaosScenarioSpec) -> ChaosObservation: ...


class ChaosProcessRunner(Protocol):
    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> ProcessResult: ...

    async def close(self) -> None: ...


class PodmanServiceInjector:
    """Exact-target Podman service fault boundary; never discovers broad targets."""

    _SUPPORTED = frozenset(
        {
            ChaosScenario.WORKER_TERMINATION,
            ChaosScenario.REDIS_RESTART,
            ChaosScenario.POSTGRES_INTERRUPTION,
            ChaosScenario.PRIMARY_PROVIDER_DISABLED,
        }
    )

    def __init__(
        self,
        *,
        podman_executable: Path,
        containers: Mapping[ChaosScenario, str],
        cwd: Path,
        timeout_seconds: float = 30,
        output_limit_bytes: int = 64 * 1024,
        process_runner: ChaosProcessRunner | None = None,
    ) -> None:
        if not _is_regular_executable(podman_executable):
            raise ValueError("podman_executable must be an absolute executable file")
        if not cwd.is_absolute() or not cwd.is_dir():
            raise ValueError("cwd must be an existing absolute directory")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= MAX_PODMAN_TIMEOUT_SECONDS
        ):
            raise ValueError("timeout_seconds must be finite, positive, and at most 300")
        if (
            type(output_limit_bytes) is not int
            or not 1 <= output_limit_bytes <= MAX_PODMAN_OUTPUT_BYTES
        ):
            raise ValueError("output_limit_bytes must be between 1 byte and 1 MiB")
        if not containers or not set(containers).issubset(self._SUPPORTED):
            raise ValueError("containers must map only supported service-fault scenarios")
        if any(not _valid_container_name(name) for name in containers.values()):
            raise ValueError("container identifiers must use the bounded allowlist syntax")
        if len(set(containers.values())) != len(containers):
            raise ValueError("each service-fault scenario must target a distinct container")
        self._executable = podman_executable
        self._containers = dict(containers)
        self._cwd = cwd
        self._timeout_seconds = float(timeout_seconds)
        self._output_limit_bytes = output_limit_bytes
        self._runner = process_runner or BoundedProcessRunner()
        self._owns_runner = process_runner is None
        self._active_target: str | None = None

    async def inject(self, scenario: ChaosScenarioSpec) -> None:
        target = self._target(scenario)
        arguments: tuple[str, ...]
        if scenario.scenario is ChaosScenario.WORKER_TERMINATION:
            arguments = ("kill", "--signal", "TERM", target)
        elif scenario.scenario is ChaosScenario.REDIS_RESTART:
            arguments = ("restart", "--time", "1", target)
        else:
            arguments = ("stop", "--time", "1", target)
        await self._run(arguments)
        self._active_target = target

    async def recover(self, scenario: ChaosScenarioSpec) -> None:
        target = self._target(scenario)
        if self._active_target != target:
            raise RuntimeError("Podman fault target is not active")
        if scenario.scenario is ChaosScenario.POSTGRES_INTERRUPTION:
            await self._ensure_running(target)

    async def cleanup(self, scenario: ChaosScenarioSpec) -> None:
        target = self._target(scenario)
        if self._active_target != target:
            return
        await self._ensure_running(target)
        self._active_target = None

    async def aclose(self) -> None:
        if self._owns_runner:
            await self._runner.close()

    def _target(self, scenario: ChaosScenarioSpec) -> str:
        try:
            return self._containers[scenario.scenario]
        except KeyError as error:
            raise ValueError("scenario has no explicitly allowlisted Podman target") from error

    async def _ensure_running(self, target: str) -> None:
        result = await self._run(("inspect", "--format", "{{.State.Running}}", target))
        stdout = "".join(
            chunk.text for chunk in result.chunks if chunk.channel.value == "stdout"
        ).strip()
        if stdout == "true":
            return
        if stdout != "false":
            raise RuntimeError("Podman returned an unsupported inspection state")
        await self._run(("start", target))

    async def _run(self, arguments: tuple[str, ...]) -> ProcessResult:
        result = await self._runner.run(
            (os.fspath(self._executable), *arguments),
            cwd=self._cwd,
            timeout_seconds=self._timeout_seconds,
            max_output_bytes=self._output_limit_bytes,
            environment=_podman_environment(),
        )
        if result.timed_out:
            raise RuntimeError("Podman fault operation timed out")
        if result.output_truncated:
            raise RuntimeError("Podman fault operation exceeded its output limit")
        if result.exit_code != 0:
            raise RuntimeError("Podman fault operation failed")
        return result


class PodmanServiceChaosDriver:
    """Compose an exact Podman service fault with an injected platform probe."""

    mode: Literal["live"] = "live"

    def __init__(self, injector: PodmanServiceInjector, probe: ChaosObservationProbe) -> None:
        self._injector = injector
        self._probe = probe

    async def inject(self, scenario: ChaosScenarioSpec) -> None:
        await self._injector.inject(scenario)

    async def recover(self, scenario: ChaosScenarioSpec) -> None:
        await self._injector.recover(scenario)

    async def observe(self, scenario: ChaosScenarioSpec) -> ChaosObservation:
        return await self._probe.observe(scenario)

    async def cleanup(self, scenario: ChaosScenarioSpec) -> None:
        await self._injector.cleanup(scenario)

    async def aclose(self) -> None:
        await self._injector.aclose()


_SCENARIO_ADAPTER = TypeAdapter(list[ChaosScenarioSpec])


def load_scenarios(path: Path) -> tuple[ChaosScenarioSpec, ...]:
    payload = path.read_bytes()
    if not payload or len(payload) > MAX_SCENARIO_BYTES:
        raise ValueError("chaos scenario file must be between 1 byte and 1 MiB")
    scenarios = tuple(_SCENARIO_ADAPTER.validate_python(yaml.safe_load(payload)))
    if not scenarios or len(scenarios) > MAX_CHAOS_SCENARIOS:
        raise ValueError("chaos suite must contain between 1 and 100 scenarios")
    if len({scenario.name for scenario in scenarios}) != len(scenarios):
        raise ValueError("chaos scenarios must be uniquely named")
    return scenarios


def write_report(report: ChaosReport, path: Path) -> None:
    if path.suffix != ".json" or not path.parent.is_dir():
        raise ValueError("report path must be a JSON file in an existing directory")
    payload = (report.model_dump_json(indent=2) + "\n").encode("utf-8")
    if len(payload) > MAX_REPORT_BYTES:
        raise ValueError("chaos report exceeded 16 MiB")
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
        Path(temporary_name).replace(path)
    finally:
        if temporary_name is not None:
            with suppress(FileNotFoundError):
                Path(temporary_name).unlink()


def _valid_container_name(value: object) -> bool:
    if (
        not isinstance(value, str)
        or not value.isascii()
        or not 1 <= len(value) <= MAX_CONTAINER_NAME_LENGTH
    ):
        return False
    return value[0].isalnum() and all(
        character.isalnum() or character in "_.-" for character in value
    )


def _is_regular_executable(path: Path) -> bool:
    try:
        return (
            path.is_absolute()
            and not path.is_symlink()
            and stat.S_ISREG(path.lstat().st_mode)
            and os.access(path, os.X_OK)
        )
    except OSError:
        return False


def _podman_environment() -> dict[str, str]:
    environment = {"LANG": "C", "LC_ALL": "C"}
    for name in ("HOME", "PATH", "TMPDIR", "XDG_RUNTIME_DIR"):
        value = os.environ.get(name)
        if value:
            environment[name] = value
    return environment


def _environment() -> ChaosEnvironment:
    memory: int | None = None
    with suppress(OSError, TypeError, ValueError):
        memory = int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    return ChaosEnvironment(
        system=platform.system() or "unknown",
        release=platform.release() or "unknown",
        machine=platform.machine() or "unknown",
        python=platform.python_version(),
        logical_cpus=os.cpu_count(),
        physical_memory_bytes=memory if memory is not None and memory > 0 else None,
    )


def _source_state() -> ChaosSourceState:
    revision = os.getenv("AGENT_PLATFORM_BENCHMARK_GIT_REVISION", "unavailable").lower()
    dirty_value = os.getenv("AGENT_PLATFORM_BENCHMARK_GIT_DIRTY")
    if dirty_value is None or dirty_value == "unknown":
        dirty = None
    elif dirty_value in {"1", "true", "dirty"}:
        dirty = True
    elif dirty_value in {"0", "false", "clean"}:
        dirty = False
    else:
        raise ValueError("AGENT_PLATFORM_BENCHMARK_GIT_DIRTY must be clean, dirty, or unknown")
    return ChaosSourceState(revision=revision, dirty=dirty)


def load_chaos_driver_factory(specification: str) -> ChaosDriverFactory:
    """Load a trusted live-driver composition root by ``module:attribute``."""

    if not _FACTORY_PATTERN.fullmatch(specification):
        raise ValueError("chaos driver factory must use a bounded module:attribute reference")
    module_name, _, attribute = specification.partition(":")
    factory = getattr(importlib.import_module(module_name), attribute, None)
    if not callable(factory):
        raise TypeError("chaos driver factory reference must be callable")
    return cast("ChaosDriverFactory", factory)


async def _create_live_driver(specification: str) -> ChaosDriver:
    candidate = load_chaos_driver_factory(specification)()
    if inspect.isawaitable(candidate):
        candidate = await candidate
    if getattr(candidate, "mode", None) != "live" or any(
        not callable(getattr(candidate, name, None))
        for name in ("inject", "recover", "observe", "cleanup")
    ):
        raise TypeError("chaos driver factory must return a live ChaosDriver")
    return candidate


async def _close_driver(driver: ChaosDriver) -> None:
    close = getattr(driver, "aclose", None)
    if close is None:
        return
    result = close()
    if inspect.isawaitable(result):
        async with asyncio.timeout(MAX_CLEANUP_SECONDS):
            await result


async def _run_cli(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenarios", type=Path, required=True)
    parser.add_argument("--scenario")
    parser.add_argument("--mode", choices=("simulation", "live"), required=True)
    parser.add_argument("--driver-factory")
    parser.add_argument("--report", type=Path, required=True)
    options = parser.parse_args(arguments)
    scenarios = load_scenarios(options.scenarios)
    if options.scenario is not None:
        scenarios = tuple(item for item in scenarios if item.name == options.scenario)
        if not scenarios:
            raise ValueError("requested chaos scenario was not found")
    if options.mode == "simulation":
        if options.driver_factory is not None:
            raise ValueError("simulation mode does not accept a live driver factory")
        report = await ChaosRunner().run(scenarios, DeterministicChaosDriver())
    else:
        if options.driver_factory is None:
            raise ValueError("live mode requires --driver-factory")
        driver = await _create_live_driver(options.driver_factory)
        try:
            report = await ChaosRunner().run(scenarios, driver)
        finally:
            await _close_driver(driver)
    write_report(report, options.report)


def main(arguments: Sequence[str] | None = None) -> None:
    asyncio.run(_run_cli(arguments))


if __name__ == "__main__":
    main(sys.argv[1:])


__all__ = [
    "ChaosDriver",
    "ChaosEnvironment",
    "ChaosObservation",
    "ChaosObservationProbe",
    "ChaosProcessRunner",
    "ChaosReport",
    "ChaosRunner",
    "ChaosScenario",
    "ChaosScenarioResult",
    "ChaosScenarioSpec",
    "ChaosSourceState",
    "DeterministicChaosDriver",
    "EvidenceSignal",
    "EvidenceSource",
    "ExpectedOutcome",
    "FaultAction",
    "FaultDefinition",
    "FinalRunState",
    "InvariantFailure",
    "PodmanServiceChaosDriver",
    "PodmanServiceInjector",
    "SignalEvidence",
    "load_chaos_driver_factory",
    "load_scenarios",
    "main",
    "write_report",
]
