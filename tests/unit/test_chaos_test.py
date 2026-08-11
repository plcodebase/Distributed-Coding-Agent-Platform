from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pytest
from pydantic import ValidationError
from scripts.chaos_test import (
    ChaosObservation,
    ChaosReport,
    ChaosRunner,
    ChaosScenario,
    ChaosScenarioSpec,
    DeterministicChaosDriver,
    FinalRunState,
    InvariantFailure,
    PodmanServiceInjector,
    SignalEvidence,
    load_scenarios,
    write_report,
)

from agent_core.tools import ToolOutputChannel
from sandbox_runtime import ProcessChunk, ProcessResult

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

ROOT = Path(__file__).parents[2]


def scenario(**updates: object) -> ChaosScenarioSpec:
    values: dict[str, object] = {
        "name": "unit-chaos",
        "scenario": "gateway_rate_limit",
        "fault": {"action": "return_429", "target": "gateway_route"},
        "recovery_timeout_seconds": 1,
        "cleanup_timeout_seconds": 1,
        "preconditions": ["A deterministic unit fixture is active."],
        "expected": {
            "allowed_final_states": ["completed"],
            "max_committed_changes": 1,
            "retry_categories": ["rate_limit"],
            "required_signals": ["gateway_retry"],
        },
    }
    values.update(updates)
    return ChaosScenarioSpec.model_validate(values)


def passing_observation(spec: ChaosScenarioSpec) -> ChaosObservation:
    return ChaosObservation(
        fault_observed=True,
        recovered=True,
        accepted_task_visible=True,
        final_state=spec.expected.allowed_final_states[0],
        committed_changes=min(1, spec.expected.max_committed_changes),
        durable_event_count=3,
        durable_event_contiguous=True,
        retry_category=(
            spec.expected.retry_categories[0] if spec.expected.retry_categories else None
        ),
        recovery_seconds=0.01,
        evidence=tuple(
            SignalEvidence(signal=signal, observed_value=1)
            for signal in spec.expected.required_signals
        ),
    )


def test_versioned_manifest_covers_every_required_failure_scenario() -> None:
    scenarios = load_scenarios(ROOT / "benchmarks" / "chaos" / "scenarios.yaml")

    assert len(scenarios) == 10
    assert {item.scenario for item in scenarios} == set(ChaosScenario)
    assert all(item.preconditions for item in scenarios)
    assert all(item.expected.accepted_task_visible for item in scenarios)
    assert all(item.expected.durable_event_continuity for item in scenarios)
    assert all(item.expected.required_signals for item in scenarios)


@pytest.mark.asyncio
async def test_deterministic_suite_passes_but_is_never_measurement_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenarios = load_scenarios(ROOT / "benchmarks" / "chaos" / "scenarios.yaml")
    driver = DeterministicChaosDriver()
    monkeypatch.setenv("AGENT_PLATFORM_BENCHMARK_GIT_REVISION", "b" * 40)
    monkeypatch.setenv("AGENT_PLATFORM_BENCHMARK_GIT_DIRTY", "clean")

    report = await ChaosRunner().run(scenarios, driver)

    assert report.synthetic is True
    assert report.result_claim == "simulation_only"
    assert all(result.passed for result in report.results)
    assert driver.cleaned == [item.name for item in scenarios]
    assert report.source.revision == "b" * 40
    assert report.source.dirty is False
    assert report.environment.machine
    assert any("Synthetic" in line for line in report.methodology)


class ObservationDriver:
    mode: Literal["live"] = "live"

    def __init__(self, observation: ChaosObservation) -> None:
        self.observation = observation
        self.cleaned = False

    async def inject(self, spec: ChaosScenarioSpec) -> None:
        del spec

    async def recover(self, spec: ChaosScenarioSpec) -> None:
        del spec

    async def observe(self, spec: ChaosScenarioSpec) -> ChaosObservation:
        del spec
        return self.observation

    async def cleanup(self, spec: ChaosScenarioSpec) -> None:
        del spec
        self.cleaned = True


@pytest.mark.asyncio
async def test_runner_evaluates_every_invariant_instead_of_fault_command_success() -> None:
    spec = scenario(recovery_timeout_seconds=0.05)
    observation = ChaosObservation(
        fault_observed=False,
        recovered=False,
        accepted_task_visible=False,
        final_state=FinalRunState.FAILED,
        committed_changes=2,
        durable_event_count=0,
        durable_event_contiguous=False,
        retry_category="wrong_retry",
        recovery_seconds=0.1,
        evidence=(),
    )
    driver = ObservationDriver(observation)

    report = await ChaosRunner().run((spec,), driver)

    result = report.results[0]
    assert result.passed is False
    assert set(result.failures) == {
        InvariantFailure.FAULT_NOT_OBSERVED,
        InvariantFailure.RECOVERY_NOT_OBSERVED,
        InvariantFailure.RECOVERY_TIMEOUT,
        InvariantFailure.ACCEPTED_TASK_MISSING,
        InvariantFailure.DUPLICATE_COMMITTED_CHANGE,
        InvariantFailure.DURABLE_EVENT_GAP,
        InvariantFailure.UNEXPECTED_FINAL_STATE,
        InvariantFailure.UNEXPECTED_RETRY_CATEGORY,
        InvariantFailure.DASHBOARD_EVIDENCE_MISSING,
    }
    assert driver.cleaned is True


class TimeoutDriver:
    mode: Literal["live"] = "live"

    def __init__(self) -> None:
        self.injected = asyncio.Event()
        self.cleaned = False

    async def inject(self, spec: ChaosScenarioSpec) -> None:
        del spec
        self.injected.set()
        await asyncio.Event().wait()

    async def recover(self, spec: ChaosScenarioSpec) -> None:
        del spec

    async def observe(self, spec: ChaosScenarioSpec) -> ChaosObservation:
        return passing_observation(spec)

    async def cleanup(self, spec: ChaosScenarioSpec) -> None:
        del spec
        self.cleaned = True


@pytest.mark.asyncio
async def test_timeout_is_opaque_and_cleanup_still_runs() -> None:
    driver = TimeoutDriver()
    report = await ChaosRunner().run(
        (scenario(recovery_timeout_seconds=0.01),),
        driver,
    )

    result = report.results[0]
    assert result.observation is None
    assert result.error_category == "recovery_timeout"
    assert result.failures == (InvariantFailure.HARNESS_ERROR,)
    assert driver.cleaned is True


@pytest.mark.asyncio
async def test_external_cancellation_waits_for_cleanup_and_propagates() -> None:
    driver = TimeoutDriver()
    task = asyncio.create_task(ChaosRunner().run((scenario(),), driver))
    await driver.injected.wait()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert driver.cleaned is True


@pytest.mark.asyncio
async def test_driver_and_cleanup_failures_are_secret_safe() -> None:
    class FailedDriver:
        mode: Literal["live"] = "live"

        async def inject(self, spec: ChaosScenarioSpec) -> None:
            del spec
            raise RuntimeError("driver-secret-must-not-escape")

        async def recover(self, spec: ChaosScenarioSpec) -> None:
            del spec

        async def observe(self, spec: ChaosScenarioSpec) -> ChaosObservation:
            return passing_observation(spec)

        async def cleanup(self, spec: ChaosScenarioSpec) -> None:
            del spec
            raise RuntimeError("cleanup-secret-must-not-escape")

    report = await ChaosRunner().run((scenario(),), FailedDriver())
    serialized = report.model_dump_json()

    assert report.results[0].failures == (
        InvariantFailure.CLEANUP_FAILED,
        InvariantFailure.HARNESS_ERROR,
    )
    assert "secret-must-not-escape" not in serialized


def test_scenario_and_report_contracts_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="fault action"):
        scenario(fault={"action": "terminate", "target": "gateway_route"})
    with pytest.raises(ValidationError):
        scenario(unknown=True)

    duplicate = tmp_path / "duplicate.yaml"
    source = (ROOT / "benchmarks" / "chaos" / "scenarios.yaml").read_text(encoding="utf-8")
    first = source.split("\n- name: redis-restart", maxsplit=1)[0]
    duplicate.write_text(f"{first}\n{first}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="uniquely named"):
        load_scenarios(duplicate)

    oversized = tmp_path / "oversized.yaml"
    oversized.write_bytes(b"x" * (1024 * 1024 + 1))
    with pytest.raises(ValueError, match="1 MiB"):
        load_scenarios(oversized)

    with pytest.raises(ValidationError):
        ChaosReport.model_validate(
            {
                "synthetic": True,
                "result_claim": "simulation_only",
                "started_at": "2026-01-01T00:00:00Z",
                "completed_at": "2026-01-01T00:00:01Z",
                "methodology": [],
                "environment": {},
                "source": {},
                "results": [],
            }
        )


@pytest.mark.asyncio
async def test_report_write_is_atomic_and_json(tmp_path: Path) -> None:
    report = await ChaosRunner().run((scenario(),), DeterministicChaosDriver())
    output = tmp_path / "chaos.json"

    write_report(report, output)

    value = json.loads(output.read_text(encoding="utf-8"))
    assert value["report_version"] == "agent-chaos-v1"
    assert value["results"][0]["passed"] is True
    assert await asyncio.to_thread(lambda: list(tmp_path.glob(".chaos.json.*"))) == []
    with pytest.raises(ValueError, match="JSON file"):
        write_report(report, tmp_path / "chaos.txt")


class FakeProcessRunner:
    def __init__(self, results: list[ProcessResult]) -> None:
        self.results = results
        self.calls: list[tuple[tuple[str, ...], dict[str, str]]] = []
        self.closed = False

    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        del cwd, timeout_seconds, max_output_bytes
        self.calls.append((tuple(argv), dict(environment or {})))
        return self.results.pop(0)

    async def close(self) -> None:
        self.closed = True


def process_result(stdout: str = "", *, exit_code: int = 0) -> ProcessResult:
    chunks = (ProcessChunk(channel=ToolOutputChannel.STDOUT, text=stdout),) if stdout else ()
    return ProcessResult(chunks=chunks, exit_code=exit_code)


def executable(tmp_path: Path) -> Path:
    path = tmp_path / "podman-test-double"
    path.write_text("test double; never executed\n", encoding="utf-8")
    path.chmod(0o700)
    return path


@pytest.mark.asyncio
async def test_podman_injector_uses_only_exact_allowlisted_bounded_argv(tmp_path: Path) -> None:
    fake = FakeProcessRunner(
        [
            process_result(),
            process_result("false\n"),
            process_result(),
            process_result("true\n"),
        ]
    )
    podman = executable(tmp_path)
    injector = PodmanServiceInjector(
        podman_executable=podman,
        containers={ChaosScenario.WORKER_TERMINATION: "agent-worker-1"},
        cwd=tmp_path,
        process_runner=fake,
    )
    spec = scenario(
        scenario="worker_termination",
        fault={"action": "terminate", "target": "active_worker"},
        expected={
            "allowed_final_states": ["completed"],
            "required_signals": ["worker_recovery"],
        },
    )

    await injector.inject(spec)
    await injector.recover(spec)
    await injector.cleanup(spec)
    await injector.aclose()

    assert [call[0][1:] for call in fake.calls] == [
        ("kill", "--signal", "TERM", "agent-worker-1"),
        ("inspect", "--format", "{{.State.Running}}", "agent-worker-1"),
        ("start", "agent-worker-1"),
        ("inspect", "--format", "{{.State.Running}}", "agent-worker-1"),
    ]
    assert all(call[0][0] == str(podman) for call in fake.calls)
    assert all("CONTAINER_HOST" not in call[1] for call in fake.calls)
    assert fake.closed is False


@pytest.mark.asyncio
async def test_podman_injector_rejects_broad_targets_and_opaque_failures(tmp_path: Path) -> None:
    podman = executable(tmp_path)
    with pytest.raises(ValueError, match="allowlist syntax"):
        PodmanServiceInjector(
            podman_executable=podman,
            containers={ChaosScenario.WORKER_TERMINATION: "../all-workers"},
            cwd=tmp_path,
        )
    with pytest.raises(ValueError, match="allowlist syntax"):
        PodmanServiceInjector(
            podman_executable=podman,
            containers={ChaosScenario.WORKER_TERMINATION: "wörker"},
            cwd=tmp_path,
        )
    with pytest.raises(ValueError, match="absolute executable"):
        PodmanServiceInjector(
            podman_executable=Path("podman"),
            containers={ChaosScenario.WORKER_TERMINATION: "worker"},
            cwd=tmp_path,
        )
    symlink = tmp_path / "podman-link"
    symlink.symlink_to(podman)
    with pytest.raises(ValueError, match="absolute executable"):
        PodmanServiceInjector(
            podman_executable=symlink,
            containers={ChaosScenario.WORKER_TERMINATION: "worker"},
            cwd=tmp_path,
        )
    with pytest.raises(ValueError, match="at most 300"):
        PodmanServiceInjector(
            podman_executable=podman,
            containers={ChaosScenario.WORKER_TERMINATION: "worker"},
            cwd=tmp_path,
            timeout_seconds=float("inf"),
        )

    fake = FakeProcessRunner([process_result(exit_code=9)])
    injector = PodmanServiceInjector(
        podman_executable=podman,
        containers={ChaosScenario.WORKER_TERMINATION: "worker"},
        cwd=tmp_path,
        process_runner=fake,
    )
    spec = scenario(
        scenario="worker_termination",
        fault={"action": "terminate", "target": "active_worker"},
        expected={
            "allowed_final_states": ["completed"],
            "required_signals": ["worker_recovery"],
        },
    )
    with pytest.raises(RuntimeError, match="operation failed"):
        await injector.inject(spec)
    await injector.cleanup(spec)
    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_podman_injector_refuses_non_service_faults_without_execution(
    tmp_path: Path,
) -> None:
    fake = FakeProcessRunner([])
    injector = PodmanServiceInjector(
        podman_executable=executable(tmp_path),
        containers={ChaosScenario.WORKER_TERMINATION: "worker"},
        cwd=tmp_path,
        process_runner=fake,
    )

    with pytest.raises(ValueError, match="no explicitly allowlisted"):
        await injector.inject(scenario())
    assert fake.calls == []
