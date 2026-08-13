from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError
from scripts.coding_evaluation import (
    CodingEvaluationReport,
    EvaluationEnvironment,
    SimulationCodingDriver,
    load_tasks,
    run_coding_evaluation,
)
from scripts.coding_evaluation import (
    write_report as write_coding_report,
)
from scripts.deployment_test import (
    DeploymentReport,
    SimulationDeploymentDriver,
    run_deployment_suite,
)
from scripts.deployment_test import (
    write_report as write_deployment_report,
)
from scripts.final_benchmark_report import (
    FinalBenchmarkReport,
    MeasuredMetric,
    compile_final_report,
    main,
    render_markdown,
    write_final_report,
)
from scripts.load_test import (
    DeterministicLoadDriver,
    LoadProfile,
    LoadRunner,
    LoadScenario,
)
from scripts.load_test import (
    write_report as write_load_report,
)
from scripts.quality_gate_report import (
    GateCommandResult,
    QualityEnvironment,
    QualityGateName,
    QualityGateReport,
    QualityGateResult,
    write_quality_report,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_compiler_keeps_simulation_evidence_honestly_incomplete(tmp_path: Path) -> None:
    deployment, coding = _simulation_artifacts(tmp_path)

    report = compile_final_report(
        (deployment, coding),
        generated_at=NOW,
    )

    assert report.status.value == "incomplete"
    assert report.measured_metrics == ()
    assert {item.status.value for item in report.acceptance} == {
        "verified_simulation",
        "unverified",
    }
    assert any("Simulation-only" in item for item in report.limitations)
    simulation_limitation = next(
        item for item in report.limitations if item.startswith("Simulation-only")
    )
    assert simulation_limitation.count("coding") == 1
    assert simulation_limitation.count("deployment") == 1
    assert "No production measurements were admitted" in render_markdown(report)

    json_path = tmp_path / "final.json"
    markdown_path = tmp_path / "final.md"
    write_final_report(json_path, markdown_path, report)
    assert FinalBenchmarkReport.model_validate_json(json_path.read_bytes()) == report
    assert markdown_path.read_text().startswith("# Final benchmark")


def test_compiler_admits_metrics_only_after_live_identity(tmp_path: Path) -> None:
    _, coding_path = _simulation_artifacts(tmp_path)
    coding = _coding_report()
    environment = EvaluationEnvironment(
        system="linux",
        release="6.1",
        machine="amd64",
        python="3.12",
        logical_cpus=8,
        physical_memory_bytes=16 * 1024**3,
        deployment_id="cluster-1/agent-platform",
    )
    live = _live_coding_report(coding, environment)
    write_coding_report(coding_path, live)

    report = compile_final_report((coding_path,), generated_at=NOW)

    assert {
        "coding_success_rate",
        "coding_total_latency_p95",
        "coding_queue_wait_p95",
        "coding_first_token_p95",
        "coding_cost_p95",
    }.issubset({item.name for item in report.measured_metrics})
    assert all(item.evidence.startswith("coding-") for item in report.measured_metrics)
    assert report.status.value == "incomplete"


def test_final_schema_rejects_simulation_backed_performance_claim(tmp_path: Path) -> None:
    deployment, coding = _simulation_artifacts(tmp_path)
    report = compile_final_report((deployment, coding), generated_at=NOW)
    metric = MeasuredMetric(
        name="invented_latency",
        value=1,
        unit="seconds",
        statistic="p95",
        evidence=report.artifacts[0].name,
    )

    with pytest.raises(ValidationError, match="live evidence"):
        report.model_copy(update={"measured_metrics": (metric,)})


def test_compiler_rejects_mixed_revisions_and_digest_mismatch(tmp_path: Path) -> None:
    deployment, coding = _simulation_artifacts(tmp_path)
    deployment_report = _deployment_report().model_copy(update={"source_revision": "abcdef1"})
    coding_report = _coding_report().model_copy(update={"source_revision": "1234567"})
    write_deployment_report(deployment, deployment_report)
    write_coding_report(coding, coding_report)

    with pytest.raises(ValueError, match="different source revisions"):
        compile_final_report((deployment, coding), generated_at=NOW)

    digest = hashlib.sha256(deployment.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="trusted index"):
        compile_final_report(
            (deployment,),
            expected_digests={str(deployment): "0" * 64},
            generated_at=NOW,
        )
    report = compile_final_report(
        (deployment,),
        expected_digests={str(deployment): digest},
        generated_at=NOW,
    )
    assert report.artifacts[0].sha256 == digest


def test_compiler_rejects_duplicate_json_keys_unknown_versions_and_symlink_output(
    tmp_path: Path,
) -> None:
    malformed = tmp_path / "duplicate.json"
    malformed.write_text('{"report_version":"agent-load-v1","report_version":"unknown"}')
    with pytest.raises(ValueError, match="duplicate JSON keys"):
        compile_final_report((malformed,), generated_at=NOW)

    unknown = tmp_path / "unknown.json"
    unknown.write_text('{"report_version":"unknown"}')
    with pytest.raises(ValueError, match="unsupported report version"):
        compile_final_report((unknown,), generated_at=NOW)

    deployment, _ = _simulation_artifacts(tmp_path)
    report = compile_final_report((deployment,), generated_at=NOW)
    output = tmp_path / "linked.json"
    output.symlink_to(tmp_path / "missing")
    with pytest.raises(ValueError, match="symlink"):
        write_final_report(output, tmp_path / "final.md", report)


def test_final_report_cli_compiles_artifacts(tmp_path: Path) -> None:
    deployment, coding = _simulation_artifacts(tmp_path)
    json_output = tmp_path / "compiled.json"
    markdown_output = tmp_path / "compiled.md"

    main(
        (
            str(deployment),
            str(coding),
            "--json-output",
            str(json_output),
            "--markdown-output",
            str(markdown_output),
        )
    )

    report = FinalBenchmarkReport.model_validate_json(json_output.read_bytes())
    assert report.status.value == "incomplete"
    assert "simulation-only" in markdown_output.read_text()


def test_compilation_is_deterministic_for_unchanged_artifacts(tmp_path: Path) -> None:
    deployment, coding = _simulation_artifacts(tmp_path)

    first = compile_final_report((deployment, coding))
    second = compile_final_report((deployment, coding))

    assert first == second
    assert first.generated_at == max(item.completed_at for item in first.artifacts)
    first_json = tmp_path / "first.json"
    first_markdown = tmp_path / "first.md"
    second_json = tmp_path / "second.json"
    second_markdown = tmp_path / "second.md"
    write_final_report(first_json, first_markdown, first)
    write_final_report(second_json, second_markdown, second)
    assert first_json.read_bytes() == second_json.read_bytes()
    assert first_markdown.read_bytes() == second_markdown.read_bytes()


def test_quality_evidence_is_required_but_cannot_replace_live_campaigns(tmp_path: Path) -> None:
    quality_path = tmp_path / "quality.json"
    command = GateCommandResult(
        argv=("fixed-command",),
        exit_code=0,
        timed_out=False,
        output_truncated=False,
        output_bytes=0,
        output_sha256=hashlib.sha256(b"").hexdigest(),
        duration_seconds=0,
    )
    quality = QualityGateReport(
        source_revision="unavailable",
        source_dirty=True,
        started_at=NOW,
        completed_at=NOW,
        environment=QualityEnvironment(
            system="test",
            release="test",
            machine="test",
            python="3.12",
        ),
        gates=tuple(
            QualityGateResult(name=name, commands=(command,), passed=True)
            for name in QualityGateName
        ),
        passed=True,
    )
    write_quality_report(quality_path, quality)

    report = compile_final_report((quality_path,))

    quality_acceptance = next(
        item for item in report.acceptance if item.requirement == "repository_quality_gates"
    )
    production = next(
        item for item in report.acceptance if item.requirement == "production_measurement_coverage"
    )
    assert quality_acceptance.status.value == "verified"
    assert production.status.value == "unverified"
    assert report.status.value == "incomplete"


@pytest.mark.asyncio
async def test_load_acceptance_uses_scenario_specific_expected_outcomes(tmp_path: Path) -> None:
    paths: list[Path] = []
    for scenario in (
        LoadScenario.API_SUBMISSION,
        LoadScenario.GATEWAY_RATE_LIMIT,
        LoadScenario.PROVIDER_FALLBACK,
    ):
        report = await LoadRunner().run(
            LoadProfile(
                name=f"unit-{scenario.value.replace('_', '-')}",
                scenario=scenario,
                concurrency=4,
                operations=20,
                operation_timeout_seconds=1,
            ),
            DeterministicLoadDriver(),
        )
        path = tmp_path / f"{scenario.value}.json"
        write_load_report(report, path)
        paths.append(path)

    final = compile_final_report(tuple(paths), generated_at=NOW)

    assert all(artifact.passed for artifact in final.artifacts)
    assert all(artifact.kind.value == "load" for artifact in final.artifacts)
    load_acceptance = next(
        item for item in final.acceptance if item.requirement == "bounded_load_campaign"
    )
    assert load_acceptance.status.value == "unverified"
    assert "complete required campaign" in load_acceptance.explanation


def _simulation_artifacts(tmp_path: Path) -> tuple[Path, Path]:
    deployment = tmp_path / "deployment.json"
    coding = tmp_path / "coding.json"
    write_deployment_report(deployment, _deployment_report())
    write_coding_report(coding, _coding_report())
    return deployment, coding


def _deployment_report() -> DeploymentReport:
    return asyncio.run(
        run_deployment_suite(
            SimulationDeploymentDriver(),
            campaign_id="baseline-deployment",
            source_revision="unavailable",
            now=lambda: NOW,
        )
    )


def _coding_report() -> CodingEvaluationReport:
    return asyncio.run(
        run_coding_evaluation(
            load_tasks(Path("benchmarks/coding_tasks/tasks.yaml")),
            SimulationCodingDriver(),
            campaign_id="baseline-coding",
            source_revision="unavailable",
            now=lambda: NOW,
        )
    )


def _live_coding_report(
    report: CodingEvaluationReport,
    environment: EvaluationEnvironment,
) -> CodingEvaluationReport:
    results = tuple(
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
    return report.model_copy(
        update={
            "mode": "live",
            "result_claim": "measurement",
            "environment": environment,
            "results": results,
        }
    )
