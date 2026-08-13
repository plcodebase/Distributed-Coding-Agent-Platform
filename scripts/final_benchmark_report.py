"""Compile versioned benchmark artifacts into an evidence-gated final report."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
import tempfile
from datetime import datetime  # noqa: TC003 - Pydantic resolves fields at runtime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from scripts.chaos_test import ChaosReport, ChaosScenario
from scripts.coding_evaluation import CodingEvaluationReport, TaskCategory
from scripts.deployment_test import DeploymentReport
from scripts.load_test import LoadReport, LoadScenario
from scripts.quality_gate_report import QualityGateReport

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from typing import Any

MAX_ARTIFACTS = 100
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_TOTAL_EVIDENCE_BYTES = 64 * 1024 * 1024
MAX_FINAL_REPORT_BYTES = 16 * 1024 * 1024
SHA256_HEX_LENGTH = 64
MIN_COMPLETE_CODING_TASKS = 30
type BoundedName = Annotated[
    str,
    StringConstraints(min_length=1, max_length=120, pattern=r"^[a-z][a-z0-9_-]*$"),
]
type BoundedText = Annotated[str, StringConstraints(min_length=1, max_length=4_000)]


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


class EvidenceKind(StrEnum):
    DEPLOYMENT = "deployment"
    CODING = "coding"
    LOAD = "load"
    CHAOS = "chaos"
    QUALITY = "quality"


class EvidenceMode(StrEnum):
    SIMULATION = "simulation"
    LIVE = "live"


class AcceptanceStatus(StrEnum):
    VERIFIED = "verified"
    VERIFIED_SIMULATION = "verified_simulation"
    FAILED = "failed"
    UNVERIFIED = "unverified"


class FinalStatus(StrEnum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    FAILED = "failed"


class EvidenceArtifact(_Model):
    name: BoundedName
    kind: EvidenceKind
    report_version: BoundedName
    path: Annotated[str, StringConstraints(min_length=1, max_length=1_000)]
    sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    mode: EvidenceMode
    source_revision: Annotated[
        str,
        StringConstraints(pattern=r"^(?:[0-9a-f]{7,64}|unavailable)$"),
    ]
    source_dirty: bool | None = None
    started_at: datetime
    completed_at: datetime
    passed: bool

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            path.is_absolute()
            or not path.parts
            or any(part in {"", ".", ".."} for part in path.parts)
        ):
            raise ValueError("artifact path must be canonical relative text")
        return path.as_posix()

    @model_validator(mode="after")
    def validate_time(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("artifact completion may not precede its start")
        return self


class AcceptanceResult(_Model):
    requirement: BoundedName
    status: AcceptanceStatus
    evidence: tuple[BoundedName, ...] = Field(max_length=20)
    explanation: BoundedText


class MeasuredMetric(_Model):
    name: BoundedName
    value: float = Field(ge=0)
    unit: BoundedName
    statistic: BoundedName
    evidence: BoundedName


class FinalBenchmarkReport(_Model):
    report_version: Literal["agent-final-benchmark-v2"] = "agent-final-benchmark-v2"
    generated_at: datetime
    source_revision: Annotated[
        str,
        StringConstraints(pattern=r"^(?:[0-9a-f]{7,64}|unavailable)$"),
    ]
    status: FinalStatus
    methodology: tuple[BoundedText, ...] = Field(min_length=1, max_length=30)
    environment_and_hardware: tuple[BoundedText, ...] = Field(min_length=1, max_length=100)
    artifacts: tuple[EvidenceArtifact, ...] = Field(min_length=1, max_length=MAX_ARTIFACTS)
    acceptance: tuple[AcceptanceResult, ...] = Field(min_length=1, max_length=100)
    measured_metrics: tuple[MeasuredMetric, ...] = Field(max_length=500)
    limitations: tuple[BoundedText, ...] = Field(max_length=100)

    @model_validator(mode="after")
    def validate_final_status(self) -> Self:
        names = tuple(item.name for item in self.artifacts)
        if len(set(names)) != len(names):
            raise ValueError("evidence artifact names must be unique")
        requirements = tuple(item.requirement for item in self.acceptance)
        if len(set(requirements)) != len(requirements):
            raise ValueError("acceptance requirement names must be unique")
        evidence_names = set(names)
        if any(not set(item.evidence).issubset(evidence_names) for item in self.acceptance):
            raise ValueError("acceptance results reference unknown evidence")
        if any(item.evidence not in evidence_names for item in self.measured_metrics):
            raise ValueError("measured metrics reference unknown evidence")
        live_names = {item.name for item in self.artifacts if item.mode is EvidenceMode.LIVE}
        if any(item.evidence not in live_names for item in self.measured_metrics):
            raise ValueError("performance metrics require live evidence")
        metric_keys = tuple((item.name, item.evidence) for item in self.measured_metrics)
        if len(set(metric_keys)) != len(metric_keys):
            raise ValueError("measured metric identities must be unique per artifact")
        expected = _final_status(self.acceptance)
        if self.status is not expected:
            raise ValueError("final status does not match acceptance results")
        if self.status is FinalStatus.COMPLETE and self.limitations:
            raise ValueError("complete reports may not retain limitations")
        return self


class _LoadedArtifact:
    def __init__(self, metadata: EvidenceArtifact, report: object) -> None:
        self.metadata = metadata
        self.report = report


def compile_final_report(
    paths: Sequence[Path],
    *,
    expected_revision: str = "unavailable",
    expected_digests: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> FinalBenchmarkReport:
    if not 1 <= len(paths) <= MAX_ARTIFACTS:
        raise ValueError(f"final reports require 1-{MAX_ARTIFACTS} evidence artifacts")
    loaded: list[_LoadedArtifact] = []
    total_bytes = 0
    names: set[str] = set()
    for path in paths:
        payload = path.read_bytes()
        total_bytes += len(payload)
        if not payload or len(payload) > MAX_ARTIFACT_BYTES:
            raise ValueError("evidence artifact is empty or exceeds its byte limit")
        if total_bytes > MAX_TOTAL_EVIDENCE_BYTES:
            raise ValueError("evidence set exceeds its aggregate byte limit")
        digest = hashlib.sha256(payload).hexdigest()
        if expected_digests is not None:
            expected = expected_digests.get(str(path))
            if expected is None or not _constant_time_equal(digest, expected):
                raise ValueError("evidence artifact digest does not match its trusted index")
        raw = _json_object(payload)
        report_version = raw.get("report_version")
        report, metadata = _parse_artifact(path, payload, report_version, digest)
        if metadata.name in names:
            raise ValueError("evidence artifacts have duplicate stable names")
        names.add(metadata.name)
        loaded.append(_LoadedArtifact(metadata, report))

    revisions = {
        item.metadata.source_revision
        for item in loaded
        if item.metadata.source_revision != "unavailable"
    }
    if len(revisions) > 1:
        raise ValueError("evidence artifacts describe different source revisions")
    if expected_revision != "unavailable" and revisions != {expected_revision}:
        raise ValueError("evidence source revision does not match the requested revision")
    source_revision = next(iter(revisions), expected_revision)
    acceptance = _acceptance(loaded)
    metrics = _live_metrics(loaded)
    status = _final_status(acceptance)
    limitations = (
        _limitations(loaded, source_revision) if status is not FinalStatus.COMPLETE else ()
    )
    return FinalBenchmarkReport(
        generated_at=generated_at or max(item.metadata.completed_at for item in loaded),
        source_revision=source_revision,
        status=status,
        methodology=(
            "Validate each input with its versioned closed report schema.",
            "Require one source revision and retain the SHA-256 digest of every artifact.",
            "Use simulation only for deterministic contract evidence and correctness checks.",
            (
                "Admit performance, capacity, latency, cost, and success-rate metrics "
                "only from live evidence."
            ),
        ),
        environment_and_hardware=_environments(loaded),
        artifacts=tuple(item.metadata for item in loaded),
        acceptance=acceptance,
        measured_metrics=metrics,
        limitations=limitations,
    )


def _parse_artifact(
    path: Path,
    payload: bytes,
    report_version: object,
    digest: str,
) -> tuple[object, EvidenceArtifact]:
    if report_version == "agent-deployment-v2":
        deployment_report = DeploymentReport.model_validate_json(payload)
        return deployment_report, _deployment_metadata(path, deployment_report, digest)
    if report_version == "agent-coding-eval-v2":
        coding_report = CodingEvaluationReport.model_validate_json(payload)
        return coding_report, _coding_metadata(path, coding_report, digest)
    if report_version == "agent-load-v1":
        load_report = LoadReport.model_validate_json(payload)
        return load_report, _load_metadata(path, load_report, digest)
    if report_version == "agent-chaos-v1":
        chaos_report = ChaosReport.model_validate_json(payload)
        return chaos_report, _chaos_metadata(path, chaos_report, digest)
    if report_version == "agent-quality-gates-v1":
        quality_report = QualityGateReport.model_validate_json(payload)
        return quality_report, _quality_metadata(path, quality_report, digest)
    raise ValueError("evidence artifact uses an unsupported report version")


def _deployment_metadata(path: Path, report: DeploymentReport, digest: str) -> EvidenceArtifact:
    return EvidenceArtifact(
        name=f"deployment-{report.campaign_id}",
        kind="deployment",
        report_version=report.report_version,
        path=_evidence_path(path),
        sha256=digest,
        mode=report.mode.value,
        source_revision=report.source_revision,
        source_dirty=None,
        started_at=report.started_at,
        completed_at=report.completed_at,
        passed=report.passed,
    )


def _coding_metadata(path: Path, report: CodingEvaluationReport, digest: str) -> EvidenceArtifact:
    return EvidenceArtifact(
        name=f"coding-{report.campaign_id}",
        kind="coding",
        report_version=report.report_version,
        path=_evidence_path(path),
        sha256=digest,
        mode=report.mode.value,
        source_revision=report.source_revision,
        source_dirty=None,
        started_at=report.started_at,
        completed_at=report.completed_at,
        passed=report.aggregate.unexpected_failures == 0,
    )


def _load_metadata(path: Path, report: LoadReport, digest: str) -> EvidenceArtifact:
    return EvidenceArtifact(
        name=f"load-{report.campaign_id}-{report.profile.name}",
        kind="load",
        report_version=report.report_version,
        path=_evidence_path(path),
        sha256=digest,
        mode="simulation" if report.synthetic else "live",
        source_revision=report.source.revision,
        started_at=report.started_at,
        completed_at=report.completed_at,
        source_dirty=report.source.dirty,
        passed=_load_passed(report),
    )


def _chaos_metadata(path: Path, report: ChaosReport, digest: str) -> EvidenceArtifact:
    return EvidenceArtifact(
        name=f"chaos-{digest[:12]}",
        kind="chaos",
        report_version=report.report_version,
        path=_evidence_path(path),
        sha256=digest,
        mode="simulation" if report.synthetic else "live",
        source_revision=report.source.revision,
        source_dirty=report.source.dirty,
        started_at=report.started_at,
        completed_at=report.completed_at,
        passed=all(item.passed for item in report.results),
    )


def _quality_metadata(
    path: Path,
    report: QualityGateReport,
    digest: str,
) -> EvidenceArtifact:
    return EvidenceArtifact(
        name=f"quality-{digest[:12]}",
        kind="quality",
        report_version=report.report_version,
        path=_evidence_path(path),
        sha256=digest,
        mode="live",
        source_revision=report.source_revision,
        source_dirty=report.source_dirty,
        started_at=report.started_at,
        completed_at=report.completed_at,
        passed=report.passed,
    )


def _load_passed(report: LoadReport) -> bool:
    aggregate = report.aggregate
    if report.profile.scenario is LoadScenario.GATEWAY_RATE_LIMIT:
        return (
            aggregate.succeeded > 0
            and aggregate.errors.get("rate_limit", 0) > 0
            and aggregate.succeeded + aggregate.errors.get("rate_limit", 0) == aggregate.attempted
        )
    if report.profile.scenario is LoadScenario.PROVIDER_FALLBACK:
        return aggregate.succeeded == aggregate.attempted and aggregate.fallbacks.maximum > 0
    return aggregate.succeeded == aggregate.attempted


def _acceptance(loaded: Sequence[_LoadedArtifact]) -> tuple[AcceptanceResult, ...]:
    deployments = [item for item in loaded if item.metadata.kind is EvidenceKind.DEPLOYMENT]
    coding = [item for item in loaded if item.metadata.kind is EvidenceKind.CODING]
    load = [item for item in loaded if item.metadata.kind is EvidenceKind.LOAD]
    chaos = [item for item in loaded if item.metadata.kind is EvidenceKind.CHAOS]
    quality = [item for item in loaded if item.metadata.kind is EvidenceKind.QUALITY]
    load_scenarios = {
        item.report.profile.scenario for item in load if isinstance(item.report, LoadReport)
    }
    chaos_scenarios = {
        result.scenario.scenario
        for item in chaos
        if isinstance(item.report, ChaosReport)
        for result in item.report.results
    }
    results = [
        _campaign_acceptance(
            "queue_scale_and_worker_recovery",
            deployments,
            complete=_deployment_complete(deployments),
            explanation=(
                "Deployment scenarios conserve accepted tasks while exercising scale and recovery."
            ),
        ),
        _campaign_acceptance(
            "coding_task_campaign",
            coding,
            complete=_coding_complete(coding),
            explanation="The versioned 30-task campaign records all required coding metrics.",
        ),
        _campaign_acceptance(
            "bounded_load_campaign",
            load,
            complete=load_scenarios == set(LoadScenario),
            explanation=(
                "Bounded load profiles record throughput, latency, tokens, cost, and failures."
            ),
        ),
        _campaign_acceptance(
            "failure_recovery_campaign",
            chaos,
            complete=chaos_scenarios == set(ChaosScenario),
            explanation=(
                "Failure scenarios require task visibility, event continuity, and "
                "idempotent recovery."
            ),
        ),
        _campaign_acceptance(
            "repository_quality_gates",
            quality,
            complete=bool(quality),
            explanation=(
                "All fixed lint, typing, test, audit, lock, build, and diff gates passed."
            ),
        ),
    ]
    campaign_results = tuple(results)
    live_verified = all(result.status is AcceptanceStatus.VERIFIED for result in campaign_results)
    clean_quality = any(
        item.metadata.passed and item.metadata.source_dirty is False for item in quality
    )
    production_verified = live_verified and clean_quality
    results.append(
        AcceptanceResult(
            requirement="production_measurement_coverage",
            status="verified" if production_verified else "unverified",
            evidence=tuple(
                item.metadata.name for item in loaded if item.metadata.mode is EvidenceMode.LIVE
            ),
            explanation=(
                "Every campaign is complete, live, revision-bound, and quality-gated."
                if production_verified
                else (
                    "Production verification requires complete live campaigns and a clean "
                    "passing quality report."
                )
            ),
        )
    )
    return tuple(results)


def _campaign_acceptance(
    requirement: str,
    artifacts: Sequence[_LoadedArtifact],
    *,
    complete: bool,
    explanation: str,
) -> AcceptanceResult:
    if not artifacts:
        return AcceptanceResult(
            requirement=requirement,
            status="unverified",
            evidence=(),
            explanation="No compatible evidence artifact was supplied.",
        )
    if any(not item.metadata.passed for item in artifacts):
        return AcceptanceResult(
            requirement=requirement,
            status="failed",
            evidence=tuple(item.metadata.name for item in artifacts),
            explanation="At least one evidence artifact failed its closed acceptance contract.",
        )
    if not complete:
        return AcceptanceResult(
            requirement=requirement,
            status="unverified",
            evidence=tuple(item.metadata.name for item in artifacts),
            explanation="The supplied artifacts do not cover the complete required campaign.",
        )
    live = all(item.metadata.mode is EvidenceMode.LIVE for item in artifacts)
    return AcceptanceResult(
        requirement=requirement,
        status="verified" if live else "verified_simulation",
        evidence=tuple(item.metadata.name for item in artifacts),
        explanation=explanation,
    )


def _deployment_complete(artifacts: Sequence[_LoadedArtifact]) -> bool:
    return any(
        isinstance(item.report, DeploymentReport)
        and {scenario.scenario.removal_mode.value for scenario in item.report.scenarios}
        == {"graceful", "abrupt"}
        for item in artifacts
    )


def _coding_complete(artifacts: Sequence[_LoadedArtifact]) -> bool:
    return any(
        isinstance(item.report, CodingEvaluationReport)
        and len(item.report.results) >= MIN_COMPLETE_CODING_TASKS
        and {result.category for result in item.report.results} == set(TaskCategory)
        for item in artifacts
    )


def _live_metrics(loaded: Sequence[_LoadedArtifact]) -> tuple[MeasuredMetric, ...]:
    metrics: list[MeasuredMetric] = []
    for item in loaded:
        if item.metadata.mode is not EvidenceMode.LIVE:
            continue
        report = item.report
        if isinstance(report, CodingEvaluationReport):
            metrics.append(
                MeasuredMetric(
                    name="coding_success_rate",
                    value=report.aggregate.success_rate,
                    unit="ratio",
                    statistic="campaign",
                    evidence=item.metadata.name,
                )
            )
            for name, summary, unit in (
                ("coding_total_latency", report.aggregate.total_latency_seconds, "seconds"),
                ("coding_queue_wait", report.aggregate.queue_wait_seconds, "seconds"),
                ("coding_first_token", report.aggregate.first_token_seconds, "seconds"),
                ("coding_input_tokens", report.aggregate.input_tokens, "tokens"),
                ("coding_output_tokens", report.aggregate.output_tokens, "tokens"),
                ("coding_cost", report.aggregate.cost_usd, "usd"),
                ("coding_retries", report.aggregate.retries, "count"),
                ("coding_fallbacks", report.aggregate.fallbacks, "count"),
                (
                    "coding_permission_denials",
                    report.aggregate.permission_denials,
                    "count",
                ),
            ):
                if summary.p95 is not None:
                    metrics.append(
                        MeasuredMetric(
                            name=f"{name}_p95",
                            value=summary.p95,
                            unit=unit,
                            statistic="p95",
                            evidence=item.metadata.name,
                        )
                    )
        elif isinstance(report, LoadReport):
            metrics.extend(
                (
                    MeasuredMetric(
                        name=f"{report.profile.name}_throughput",
                        value=report.aggregate.throughput_per_second,
                        unit="requests_per_second",
                        statistic="mean",
                        evidence=item.metadata.name,
                    ),
                    MeasuredMetric(
                        name=f"{report.profile.name}_latency",
                        value=report.aggregate.latency_seconds.p95,
                        unit="seconds",
                        statistic="p95",
                        evidence=item.metadata.name,
                    ),
                    MeasuredMetric(
                        name=f"{report.profile.name}_queue_wait",
                        value=report.aggregate.queue_wait_seconds.p95,
                        unit="seconds",
                        statistic="p95",
                        evidence=item.metadata.name,
                    ),
                    MeasuredMetric(
                        name=f"{report.profile.name}_first_token",
                        value=report.aggregate.first_token_seconds.p95,
                        unit="seconds",
                        statistic="p95",
                        evidence=item.metadata.name,
                    ),
                    MeasuredMetric(
                        name=f"{report.profile.name}_cost",
                        value=(report.aggregate.cost_usd.mean * report.aggregate.cost_usd.count),
                        unit="usd",
                        statistic="total",
                        evidence=item.metadata.name,
                    ),
                )
            )
        elif isinstance(report, DeploymentReport):
            peak = max(result.observation.peak_workers for result in report.scenarios)
            recovery = sorted(
                result.observation.recovery_seconds
                for result in report.scenarios
                if result.observation.recovery_seconds is not None
            )
            metrics.extend(
                (
                    MeasuredMetric(
                        name="peak_worker_replicas",
                        value=float(peak),
                        unit="replicas",
                        statistic="maximum",
                        evidence=item.metadata.name,
                    ),
                    *(
                        (
                            MeasuredMetric(
                                name="deployment_recovery_time",
                                value=_percentile(recovery, 0.95),
                                unit="seconds",
                                statistic="p95",
                                evidence=item.metadata.name,
                            ),
                        )
                        if recovery
                        else ()
                    ),
                )
            )
        elif isinstance(report, ChaosReport):
            recovery = sorted(
                result.observation.recovery_seconds
                for result in report.results
                if result.observation is not None
            )
            if recovery:
                metrics.append(
                    MeasuredMetric(
                        name="failure_recovery_time",
                        value=_percentile(recovery, 0.95),
                        unit="seconds",
                        statistic="p95",
                        evidence=item.metadata.name,
                    )
                )
    return tuple(metrics)


def _environments(loaded: Sequence[_LoadedArtifact]) -> tuple[BoundedText, ...]:
    values: list[BoundedText] = []
    for item in loaded:
        report = item.report
        environment = getattr(report, "environment", None)
        if environment is None:
            continue
        parts = [
            f"{item.metadata.name}: system={environment.system}",
            f"release={environment.release}",
            f"machine={environment.machine}",
            f"python={environment.python}",
        ]
        logical_cpus = getattr(environment, "logical_cpus", None)
        memory = getattr(environment, "physical_memory_bytes", None)
        if logical_cpus is not None:
            parts.append(f"logical_cpus={logical_cpus}")
        if memory is not None:
            parts.append(f"physical_memory_bytes={memory}")
        values.append(", ".join(parts))
    return tuple(values) or ("Environment metadata was unavailable in the supplied artifact.",)


def _limitations(
    loaded: Sequence[_LoadedArtifact], source_revision: str
) -> tuple[BoundedText, ...]:
    limitations: list[BoundedText] = []
    simulated = sorted(
        {
            item.metadata.kind.value
            for item in loaded
            if item.metadata.mode is EvidenceMode.SIMULATION
        }
    )
    if simulated:
        limitations.append(
            "Simulation-only evidence cannot support production performance claims: "
            + ", ".join(simulated)
            + "."
        )
    missing = sorted(
        item.value for item in set(EvidenceKind) - {item.metadata.kind for item in loaded}
    )
    if missing:
        limitations.append("No evidence artifacts were supplied for: " + ", ".join(missing) + ".")
    if source_revision == "unavailable":
        limitations.append("The evidence set is not bound to a committed source revision.")
    limitations.append(
        "A live Kubernetes cluster, external metrics adapter, managed data services, "
        "and real provider traffic are required to complete production acceptance."
    )
    return tuple(dict.fromkeys(limitations))


def _final_status(acceptance: Sequence[AcceptanceResult]) -> FinalStatus:
    statuses = {item.status for item in acceptance}
    if AcceptanceStatus.FAILED in statuses:
        return FinalStatus.FAILED
    if statuses == {AcceptanceStatus.VERIFIED}:
        return FinalStatus.COMPLETE
    return FinalStatus.INCOMPLETE


def render_markdown(report: FinalBenchmarkReport) -> str:
    lines = [
        "# Final benchmark and acceptance report",
        "",
        f"Status: **{report.status.value}**",
        f"Source revision: `{report.source_revision}`",
        f"Generated: `{report.generated_at.isoformat()}`",
        "",
        "## Methodology",
        "",
        *(f"- {item}" for item in report.methodology),
        "",
        "## Acceptance",
        "",
        "| Requirement | Status | Evidence |",
        "|---|---|---|",
        *(
            f"| {item.requirement} | {item.status.value} | {', '.join(item.evidence) or 'none'} |"
            for item in report.acceptance
        ),
        "",
        "## Measured metrics",
        "",
    ]
    if report.measured_metrics:
        lines.extend(
            [
                "| Metric | Value | Unit | Statistic | Evidence |",
                "|---|---:|---|---|---|",
                *(
                    (
                        f"| {item.name} | {item.value:g} | {item.unit} | "
                        f"{item.statistic} | {item.evidence} |"
                    )
                    for item in report.measured_metrics
                ),
            ]
        )
    else:
        lines.append(
            "No production measurements were admitted; supplied evidence is simulation-only."
        )
    lines.extend(["", "## Environment and hardware", ""])
    lines.extend(f"- {item}" for item in report.environment_and_hardware)
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in report.limitations)
    lines.extend(["", "## Artifact integrity", ""])
    lines.extend(
        f"- `{item.path}` — `{item.sha256}` ({item.mode.value})" for item in report.artifacts
    )
    return "\n".join(lines) + "\n"


def write_final_report(
    json_path: Path,
    markdown_path: Path,
    report: FinalBenchmarkReport,
) -> None:
    json_payload = report.model_dump_json(indent=2).encode("utf-8") + b"\n"
    markdown_payload = render_markdown(report).encode("utf-8")
    for path, payload in ((json_path, json_payload), (markdown_path, markdown_payload)):
        if len(payload) > MAX_FINAL_REPORT_BYTES:
            raise ValueError("final report output exceeds its byte limit")
        _write_atomic(path, payload)


def _write_atomic(path: Path, payload: bytes) -> None:
    destination = path.absolute()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError("final report destination may not be a symlink")
    descriptor, temporary = tempfile.mkstemp(prefix=".final-report-", dir=destination.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        Path(temporary).replace(destination)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _json_object(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(payload, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("evidence artifact is not valid JSON") from error
    if not isinstance(value, dict):
        raise TypeError("evidence artifact must be a JSON object")
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("evidence artifact contains duplicate JSON keys")
        value[key] = item
    return value


def _constant_time_equal(left: str, right: str) -> bool:
    if len(right) != SHA256_HEX_LENGTH or any(
        character not in "0123456789abcdef" for character in right
    ):
        return False
    return hmac.compare_digest(left, right)


def _evidence_path(path: Path) -> str:
    absolute = path.absolute()
    try:
        return absolute.relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return f"external/{path.name}"


def _percentile(values: Sequence[float], percentile: float) -> float:
    return values[max(0, min(math.ceil(percentile * len(values)) - 1, len(values) - 1))]


def main(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    parser.add_argument("--expected-revision", default="unavailable")
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    parsed = parser.parse_args(arguments)
    report = compile_final_report(parsed.artifacts, expected_revision=parsed.expected_revision)
    write_final_report(parsed.json_output, parsed.markdown_output, report)


if __name__ == "__main__":
    main()
