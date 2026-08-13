from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from scripts.deployment_test import (
    DeploymentEnvironment,
    DeploymentObservation,
    DeploymentReport,
    DeploymentSample,
    DeploymentScenario,
    DeploymentScenarioResult,
    EvidenceMode,
    SimulationDeploymentDriver,
    default_scenarios,
    load_driver_factory,
    main,
    run_deployment_suite,
    write_report,
)

if TYPE_CHECKING:
    from pathlib import Path

NOW = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_simulation_proves_scale_drain_recovery_and_task_conservation(tmp_path: Path) -> None:
    report = await run_deployment_suite(
        SimulationDeploymentDriver(),
        campaign_id="ci-deployment",
        source_revision="abcdef1",
        now=lambda: NOW,
    )

    assert report.passed
    assert report.result_claim == "simulation_only"
    assert all(result.observation.scale_up_observed for result in report.scenarios)
    graceful, abrupt = report.scenarios
    assert graceful.observation.drained_active_task
    assert abrupt.observation.recovered_tasks == 1
    assert sum(result.observation.lost_tasks for result in report.scenarios) == 0
    assert sum(result.observation.duplicate_completions for result in report.scenarios) == 0

    destination = tmp_path / "deployment.json"
    write_report(destination, report)
    assert DeploymentReport.model_validate_json(destination.read_bytes()) == report


def test_evidence_contract_rejects_simulation_as_measurement() -> None:
    report = _report()
    with pytest.raises(ValidationError, match="result claim"):
        report.model_copy(update={"result_claim": "measurement"})


def test_live_evidence_requires_complete_cluster_identity() -> None:
    with pytest.raises(ValidationError, match="supplied together"):
        DeploymentEnvironment(
            system="linux",
            release="1",
            machine="amd64",
            python="3.12",
            cluster_uid="cluster-1",
        )


def test_observation_rejects_lost_unaccounted_tasks_and_false_scale_flag() -> None:
    sample = DeploymentSample(tick=0, workers=3, queued=0, active=0, completed=1)
    with pytest.raises(ValidationError, match="completed or explicitly lost"):
        DeploymentObservation(
            accepted_tasks=2,
            completed_tasks=1,
            lost_tasks=0,
            duplicate_completions=0,
            recovered_tasks=0,
            initial_workers=3,
            peak_workers=3,
            removed_worker=True,
            removed_worker_name="agent-worker-a",
            drained_active_task=True,
            scale_up_observed=False,
            samples=(sample,),
        )
    with pytest.raises(ValidationError, match="scale-up flag"):
        DeploymentObservation(
            accepted_tasks=1,
            completed_tasks=1,
            lost_tasks=0,
            duplicate_completions=0,
            recovered_tasks=0,
            initial_workers=3,
            peak_workers=4,
            removed_worker=True,
            removed_worker_name="agent-worker-a",
            drained_active_task=True,
            scale_up_observed=False,
            samples=(sample,),
        )


def test_default_scenarios_are_closed_and_cover_both_removal_paths() -> None:
    scenarios = default_scenarios()
    assert {item.removal_mode.value for item in scenarios} == {"graceful", "abrupt"}
    with pytest.raises(ValidationError, match="maximum_workers"):
        DeploymentScenario(
            name="invalid",
            accepted_tasks=1,
            initial_workers=3,
            maximum_workers=3,
            target_tasks_per_worker=1,
            work_ticks=1,
            remove_at_tick=1,
            removal_mode="abrupt",
        )


def test_driver_loader_and_report_destination_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="module:attribute"):
        load_driver_factory("not valid")
    with pytest.raises(TypeError, match="callable"):
        load_driver_factory("scripts.deployment_test:MAX_TASKS")

    destination = tmp_path / "report.json"
    destination.symlink_to(tmp_path / "elsewhere")
    report = _report()
    with pytest.raises(ValueError, match="symlink"):
        write_report(destination, report)


def test_deployment_cli_writes_simulation_report(tmp_path: Path) -> None:
    output = tmp_path / "deployment-cli.json"

    with pytest.raises(SystemExit) as stopped:
        main(
            (
                "--mode",
                "simulation",
                "--campaign-id",
                "cli-deployment",
                "--source-revision",
                "abcdef1",
                "--output",
                str(output),
            )
        )

    assert stopped.value.code == 0
    assert DeploymentReport.model_validate_json(output.read_bytes()).passed


def _report() -> DeploymentReport:
    scenario = default_scenarios()[0]
    sample = DeploymentSample(tick=0, workers=4, queued=0, active=0, completed=40)
    result_observation = DeploymentObservation(
        accepted_tasks=40,
        completed_tasks=40,
        lost_tasks=0,
        duplicate_completions=0,
        recovered_tasks=0,
        initial_workers=3,
        peak_workers=4,
        removed_worker=True,
        removed_worker_name="agent-worker-a",
        drained_active_task=True,
        scale_up_observed=True,
        samples=(sample,),
    )
    result = DeploymentScenarioResult(
        scenario=scenario,
        observation=result_observation,
        passed=True,
        failures=(),
    )
    second = result.model_copy(
        update={"scenario": default_scenarios()[1]},
    )
    return DeploymentReport(
        mode=EvidenceMode.SIMULATION,
        result_claim="simulation_only",
        campaign_id="report-test",
        source_revision="unavailable",
        started_at=NOW,
        completed_at=NOW,
        methodology=("test",),
        environment=SimulationDeploymentDriver.environment,
        scenarios=(result, second),
        passed=True,
    )
