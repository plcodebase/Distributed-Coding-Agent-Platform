from __future__ import annotations

import asyncio
import stat
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError
from scripts import coding_evaluation
from scripts.coding_evaluation import (
    CodingEvaluationReport,
    CodingTask,
    EvaluationEnvironment,
    EvaluationMode,
    MetricSummary,
    SimulationCodingDriver,
    TaskCategory,
    TaskMetrics,
    TaskObservation,
    VerificationResult,
    load_driver_factory,
    load_tasks,
    main,
    materialize_task,
    run_coding_evaluation,
    write_report,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _tasks_path() -> Path:
    return Path("benchmarks/coding_tasks/tasks.yaml")


def test_corpus_has_thirty_unique_tasks_and_three_of_each_category() -> None:
    tasks = load_tasks(_tasks_path())

    assert len(tasks) == 30
    assert len({task.id for task in tasks}) == 30
    assert Counter(task.category for task in tasks) == dict.fromkeys(TaskCategory, 3)
    assert all(task.fixture_template is task.category for task in tasks)


def test_fixture_materialization_is_private_contained_and_bounded(tmp_path: Path) -> None:
    task = load_tasks(_tasks_path())[0]
    workspace = materialize_task(task, tmp_path)

    assert stat.S_IMODE(workspace.stat().st_mode) == 0o700
    files = tuple(path for path in workspace.rglob("*") if path.is_file())
    assert files
    assert all(path.is_relative_to(workspace) for path in files)
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in files)
    with pytest.raises(ValueError, match="already exists"):
        materialize_task(task, tmp_path)


def test_every_category_has_a_meaningful_deficient_fixture_without_placeholder_tests() -> None:
    for task in load_tasks(_tasks_path()):
        files = coding_evaluation._fixture_files(task.category, task.variant)
        assert coding_evaluation._fixture_is_deficient(task, files)
        assert all("assert True" not in content for content in files.values())
        if task.category is TaskCategory.MISSING_TESTS:
            assert not any(path.startswith("test_") for path in files)
        else:
            assert any(path.startswith("test_") for path in files)


@pytest.mark.asyncio
async def test_simulation_exercises_all_tasks_and_records_every_metric(tmp_path: Path) -> None:
    tasks = load_tasks(_tasks_path())
    report = await run_coding_evaluation(
        tasks,
        SimulationCodingDriver(),
        campaign_id="ci-coding-eval",
        source_revision="abcdef1",
        now=lambda: NOW,
    )

    assert report.mode is EvaluationMode.SIMULATION
    assert report.result_claim == "simulation_only"
    assert report.aggregate.attempted == 30
    assert report.aggregate.passed == 30
    assert report.aggregate.tests_passed == 30
    assert report.aggregate.unexpected_failures == 0
    assert report.aggregate.total_latency_seconds.count == 30
    assert report.aggregate.queue_wait_seconds.count == 30
    assert report.aggregate.first_token_seconds.count == 30
    assert report.aggregate.input_tokens.count == 30
    assert report.aggregate.cost_usd.count == 30

    output = tmp_path / "report.json"
    write_report(output, report)
    assert CodingEvaluationReport.model_validate_json(output.read_bytes()) == report


@pytest.mark.asyncio
async def test_driver_failure_is_opaque_and_recorded_for_every_task() -> None:
    class BrokenDriver:
        mode = EvaluationMode.SIMULATION
        environment = SimulationCodingDriver.environment

        async def evaluate(self, task: CodingTask, workspace: Path) -> TaskObservation:
            del task, workspace
            raise RuntimeError("do not expose this provider secret")

    report = await run_coding_evaluation(
        load_tasks(_tasks_path()),
        BrokenDriver(),
        campaign_id="broken-driver",
        source_revision="unavailable",
        now=lambda: NOW,
    )

    assert report.aggregate.unexpected_failures == 30
    assert {result.observation.error_category for result in report.results} == {
        "evaluation_driver_error"
    }
    assert "do not expose" not in report.model_dump_json()


@pytest.mark.asyncio
async def test_harness_rejects_noop_driver_even_when_it_fabricates_success() -> None:
    class NoopDriver:
        mode = EvaluationMode.SIMULATION
        environment = SimulationCodingDriver.environment

        async def evaluate(self, task: CodingTask, workspace: Path) -> TaskObservation:
            del workspace
            return TaskObservation(
                outcome="completed",
                tests_passed=True,
                committed_changes=1,
                changed_paths=task.expected_changed_paths[:1],
                verifications=tuple(
                    VerificationResult(
                        argv=command.argv,
                        backend="simulation",
                        exit_code=0,
                        duration_seconds=0,
                        output_sha256="0" * 64,
                    )
                    for command in task.verification
                ),
                metrics=TaskMetrics(
                    iterations=1,
                    tool_calls=1,
                    total_latency_seconds=0,
                    retries=0,
                    fallbacks=0,
                    permission_denials=0,
                ),
            )

    report = await run_coding_evaluation(
        load_tasks(_tasks_path()),
        NoopDriver(),
        campaign_id="fabricated-noop",
        source_revision="unavailable",
        now=lambda: NOW,
    )

    assert report.aggregate.passed == 0
    assert {item.observation.error_category for item in report.results} == {
        "evaluation_driver_error"
    }


@pytest.mark.asyncio
async def test_timeout_is_structured_and_does_not_stop_later_tasks() -> None:
    class SlowDriver:
        mode = EvaluationMode.SIMULATION
        environment = SimulationCodingDriver.environment

        async def evaluate(self, task: CodingTask, workspace: Path) -> TaskObservation:
            del task, workspace
            await asyncio.sleep(1)
            raise AssertionError

    tasks = tuple(
        task.model_copy(update={"timeout_seconds": 0.001}) for task in load_tasks(_tasks_path())
    )
    report = await run_coding_evaluation(
        tasks,
        SlowDriver(),
        campaign_id="timeout-driver",
        source_revision="unavailable",
        now=lambda: NOW,
    )

    assert len(report.results) == 30
    assert {result.observation.outcome.value for result in report.results} == {"timeout"}
    assert {result.observation.error_category for result in report.results} == {
        "evaluation_timeout"
    }


def test_manifest_rejects_duplicates_missing_categories_and_traversal(tmp_path: Path) -> None:
    raw = yaml.safe_load(_tasks_path().read_text())
    raw[1]["id"] = raw[0]["id"]
    duplicate = tmp_path / "duplicate.yaml"
    duplicate.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="identities must be unique"):
        load_tasks(duplicate)

    too_short = tmp_path / "short.yaml"
    too_short.write_text(yaml.safe_dump(raw[:29]))
    with pytest.raises(ValueError, match="30-100"):
        load_tasks(too_short)

    raw = yaml.safe_load(_tasks_path().read_text())
    raw[0]["expected_changed_paths"] = ["../escape.py"]
    traversal = tmp_path / "traversal.yaml"
    traversal.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValidationError, match="contained relative"):
        load_tasks(traversal)


def test_metrics_and_claims_reject_inconsistent_values() -> None:
    with pytest.raises(ValidationError, match="queue wait"):
        TaskMetrics(
            iterations=1,
            tool_calls=1,
            queue_wait_seconds=2,
            total_latency_seconds=1,
            retries=0,
            fallbacks=0,
            permission_denials=0,
        )
    with pytest.raises(ValidationError, match="one commit"):
        TaskObservation(
            outcome="completed",
            tests_passed=True,
            committed_changes=2,
            changed_paths=("file.py",),
            verifications=(
                VerificationResult(
                    argv=("python", "-m", "pytest"),
                    backend="simulation",
                    exit_code=0,
                    duration_seconds=0,
                    output_sha256="0" * 64,
                ),
            ),
            metrics=TaskMetrics(
                iterations=1,
                tool_calls=1,
                total_latency_seconds=1,
                retries=0,
                fallbacks=0,
                permission_denials=0,
            ),
        )


def test_live_mode_requires_deployment_identity_and_model_copy_revalidates() -> None:
    report = _simulation_report()
    with pytest.raises(ValidationError, match="result claim"):
        report.model_copy(update={"result_claim": "measurement"})
    with pytest.raises(ValidationError, match="deployment identity"):
        report.model_copy(update={"mode": "live", "result_claim": "measurement"})

    environment = EvaluationEnvironment(
        system="linux",
        release="1",
        machine="amd64",
        python="3.12",
        deployment_id="cluster/deployment",
    )
    live_results = tuple(
        result.model_copy(
            update={
                "observation": result.observation.model_copy(
                    update={
                        "verifications": tuple(
                            verification.model_copy(update={"backend": "podman"})
                            for verification in result.observation.verifications
                        )
                    }
                )
            }
        )
        for result in report.results
    )
    live = report.model_copy(
        update={
            "mode": "live",
            "result_claim": "measurement",
            "environment": environment,
            "results": live_results,
        }
    )
    assert live.mode is EvaluationMode.LIVE


def test_report_recomputes_aggregate_and_binds_corpus_digest() -> None:
    report = _simulation_report()
    forged_summary = MetricSummary(
        count=30,
        minimum=1,
        p50=1,
        p95=1,
        maximum=1,
        mean=1,
    )
    with pytest.raises(ValidationError, match="aggregate metrics"):
        report.model_copy(
            update={
                "aggregate": report.aggregate.model_copy(
                    update={"permission_denials": forged_summary}
                )
            }
        )
    with pytest.raises(ValidationError, match="corpus digest"):
        report.model_copy(update={"corpus_sha256": "0" * 64})


def test_driver_loader_and_report_destination_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="module:attribute"):
        load_driver_factory("invalid factory")
    with pytest.raises(TypeError, match="callable"):
        load_driver_factory("scripts.coding_evaluation:MAX_TASKS")

    destination = tmp_path / "report.json"
    destination.symlink_to(tmp_path / "missing")
    with pytest.raises(ValueError, match="symlink"):
        write_report(destination, _simulation_report())


def test_coding_evaluation_cli_runs_all_thirty_tasks(tmp_path: Path) -> None:
    output = tmp_path / "coding-cli.json"

    with pytest.raises(SystemExit) as stopped:
        main(
            (
                "--mode",
                "simulation",
                "--campaign-id",
                "cli-coding",
                "--source-revision",
                "abcdef1",
                "--output",
                str(output),
            )
        )

    assert stopped.value.code == 0
    report = CodingEvaluationReport.model_validate_json(output.read_bytes())
    assert report.aggregate.attempted == 30
    assert report.aggregate.unexpected_failures == 0


def _simulation_report() -> CodingEvaluationReport:
    return asyncio.run(
        run_coding_evaluation(
            load_tasks(_tasks_path()),
            SimulationCodingDriver(),
            campaign_id="copy-test",
            source_revision="unavailable",
            now=lambda: NOW,
        )
    )
