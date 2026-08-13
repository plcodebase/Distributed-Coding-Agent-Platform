"""Deterministic 30-100 task coding-agent evaluation harness."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import inspect
import json
import math
import os
import platform
import re
import stat
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self, cast

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    field_validator,
    model_validator,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import Any

MIN_TASKS = 30
MAX_TASKS = 100
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_REPORT_BYTES = 16 * 1024 * 1024
MAX_FIXTURE_FILES = 32
MAX_FIXTURE_BYTES = 1024 * 1024
MAX_WORKSPACE_FILES = 64
MAX_WORKSPACE_BYTES = 2 * 1024 * 1024
MIN_TASKS_PER_CATEGORY = 3
MIN_VERIFICATION_ARGV = 3
_FACTORY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")
type BoundedName = Annotated[
    str,
    StringConstraints(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_-]*$"),
]
type BoundedText = Annotated[str, StringConstraints(min_length=1, max_length=4_000)]
type RelativePath = Annotated[
    str,
    StringConstraints(min_length=1, max_length=500, pattern=r"^[^\x00]+$"),
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


class EvaluationMode(StrEnum):
    SIMULATION = "simulation"
    LIVE = "live"


class TaskCategory(StrEnum):
    FIX_FAILING_TEST = "fix_failing_test"
    API_ENDPOINT = "api_endpoint"
    TYPE_ERROR = "type_error"
    MULTI_FILE_RENAME = "multi_file_rename"
    INPUT_VALIDATION = "input_validation"
    DEDUP_REFACTOR = "dedup_refactor"
    DEPENDENCY_USAGE = "dependency_usage"
    CONCURRENCY_BUG = "concurrency_bug"
    MISSING_TESTS = "missing_tests"
    DIAGNOSE_EXCEPTION = "diagnose_exception"


class TaskOutcome(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    REFUSED = "refused"
    TIMEOUT = "timeout"


class VerificationCommand(_Model):
    argv: tuple[Annotated[str, StringConstraints(min_length=1, max_length=500)], ...] = Field(
        min_length=1,
        max_length=20,
    )

    @model_validator(mode="after")
    def validate_fixed_verifier(self) -> Self:
        if len(self.argv) < MIN_VERIFICATION_ARGV or self.argv[:2] != ("python", "-m"):
            raise ValueError("verification commands must use python -m")
        if self.argv[2] not in {"pytest", "mypy"}:
            raise ValueError("verification module is not allowlisted")
        if any(value.startswith("@") or "\x00" in value for value in self.argv):
            raise ValueError("verification arguments contain a forbidden value")
        return self


class CodingTask(_Model):
    id: BoundedName
    category: TaskCategory
    fixture_template: TaskCategory
    variant: int = Field(ge=1, le=100)
    instruction: BoundedText
    verification: tuple[VerificationCommand, ...] = Field(min_length=1, max_length=10)
    expected_outcomes: tuple[TaskOutcome, ...] = (TaskOutcome.COMPLETED,)
    expected_changed_paths: tuple[RelativePath, ...] = Field(min_length=1, max_length=32)
    timeout_seconds: float = Field(default=900, gt=0, le=3_600)

    @field_validator("expected_changed_paths")
    @classmethod
    def validate_paths(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(_relative_path(value) for value in values)
        if len(set(normalized)) != len(normalized):
            raise ValueError("expected changed paths must be unique")
        return normalized

    @model_validator(mode="after")
    def validate_task_contract(self) -> Self:
        if self.fixture_template is not self.category:
            raise ValueError("fixture_template must match the task category")
        if len(set(self.expected_outcomes)) != len(self.expected_outcomes):
            raise ValueError("expected outcomes must be unique")
        return self


_TASK_ADAPTER = TypeAdapter(list[CodingTask])


class TaskMetrics(_Model):
    iterations: int = Field(ge=0, le=1_000_000)
    tool_calls: int = Field(ge=0, le=1_000_000)
    input_tokens: int | None = Field(default=None, ge=0, le=1_000_000_000)
    output_tokens: int | None = Field(default=None, ge=0, le=1_000_000_000)
    cost_usd: float | None = Field(default=None, ge=0, le=1_000_000)
    queue_wait_seconds: float | None = Field(default=None, ge=0, le=86_400)
    first_token_seconds: float | None = Field(default=None, ge=0, le=86_400)
    total_latency_seconds: float = Field(ge=0, le=86_400)
    retries: int = Field(ge=0, le=1_000_000)
    fallbacks: int = Field(ge=0, le=1_000_000)
    permission_denials: int = Field(ge=0, le=1_000_000)

    @model_validator(mode="after")
    def validate_latency_order(self) -> Self:
        if (
            self.queue_wait_seconds is not None
            and self.queue_wait_seconds > self.total_latency_seconds
        ):
            raise ValueError("queue wait may not exceed total latency")
        if (
            self.first_token_seconds is not None
            and self.first_token_seconds > self.total_latency_seconds
        ):
            raise ValueError("first-token latency may not exceed total latency")
        return self


class VerificationResult(_Model):
    """Measured execution evidence for one configured verification command."""

    argv: tuple[Annotated[str, StringConstraints(min_length=1, max_length=500)], ...] = Field(
        min_length=1,
        max_length=20,
    )
    backend: Literal["simulation", "podman"]
    exit_code: int = Field(ge=-255, le=255)
    timed_out: bool = False
    duration_seconds: float = Field(ge=0, le=3_600)
    output_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    output_truncated: bool = False

    @property
    def passed(self) -> bool:
        return self.exit_code == 0 and not self.timed_out and not self.output_truncated


class TaskObservation(_Model):
    outcome: TaskOutcome
    tests_passed: bool
    committed_changes: int = Field(ge=0, le=1_000)
    changed_paths: tuple[RelativePath, ...] = Field(max_length=32)
    verifications: tuple[VerificationResult, ...] = Field(default=(), max_length=10)
    metrics: TaskMetrics
    error_category: BoundedName | None = None

    @field_validator("changed_paths")
    @classmethod
    def validate_changed_paths(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(_relative_path(value) for value in values)
        if len(set(normalized)) != len(normalized):
            raise ValueError("changed paths must be unique")
        return normalized

    @model_validator(mode="after")
    def validate_outcome(self) -> Self:
        measured_tests_passed = bool(self.verifications) and all(
            result.passed for result in self.verifications
        )
        if self.tests_passed != measured_tests_passed:
            raise ValueError("tests_passed must be derived from verification evidence")
        if self.outcome is TaskOutcome.COMPLETED:
            if (
                not self.tests_passed
                or self.committed_changes != 1
                or self.error_category is not None
                or not self.changed_paths
            ):
                raise ValueError(
                    "completed tasks require passing tests, one commit, and actual changes"
                )
        elif self.error_category is None:
            raise ValueError("non-completed tasks require an opaque error category")
        if self.tests_passed and self.outcome is not TaskOutcome.COMPLETED:
            raise ValueError("passing tests require a completed outcome")
        return self


class CodingTaskResult(_Model):
    task_id: BoundedName
    category: TaskCategory
    task_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    fixture_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    verifier_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    expected_outcomes: tuple[TaskOutcome, ...] = Field(min_length=1, max_length=4)
    observation: TaskObservation
    passed: bool
    failures: tuple[BoundedName, ...] = Field(max_length=20)

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if self.passed == bool(self.failures):
            raise ValueError("a task passes exactly when it has no invariant failures")
        if self.passed and self.observation.outcome not in self.expected_outcomes:
            raise ValueError("passing tasks require an expected outcome")
        return self


class MetricSummary(_Model):
    count: int = Field(ge=0, le=MAX_TASKS)
    minimum: float | None = Field(default=None, ge=0)
    p50: float | None = Field(default=None, ge=0)
    p95: float | None = Field(default=None, ge=0)
    maximum: float | None = Field(default=None, ge=0)
    mean: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_distribution(self) -> Self:
        values = (self.minimum, self.p50, self.p95, self.maximum, self.mean)
        if self.count == 0 and any(value is not None for value in values):
            raise ValueError("unobserved metric summaries must contain null values")
        if self.count > 0:
            if any(value is None for value in values):
                raise ValueError("observed metric summaries require every statistic")
            minimum, p50, p95, maximum, mean = cast("tuple[float, ...]", values)
            if not minimum <= p50 <= p95 <= maximum or not minimum <= mean <= maximum:
                raise ValueError("metric summary values must be ordered")
        return self


class CodingAggregate(_Model):
    attempted: int = Field(ge=MIN_TASKS, le=MAX_TASKS)
    passed: int = Field(ge=0, le=MAX_TASKS)
    tests_passed: int = Field(ge=0, le=MAX_TASKS)
    unexpected_failures: int = Field(ge=0, le=MAX_TASKS)
    duplicate_commits: int = Field(ge=0, le=MAX_TASKS)
    success_rate: float = Field(ge=0, le=1)
    outcomes: dict[TaskOutcome, int]
    total_latency_seconds: MetricSummary
    queue_wait_seconds: MetricSummary
    first_token_seconds: MetricSummary
    iterations: MetricSummary
    tool_calls: MetricSummary
    input_tokens: MetricSummary
    output_tokens: MetricSummary
    cost_usd: MetricSummary
    retries: MetricSummary
    fallbacks: MetricSummary
    permission_denials: MetricSummary

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if sum(self.outcomes.values()) != self.attempted:
            raise ValueError("outcome counts must equal attempted tasks")
        if not 0 <= self.tests_passed <= self.passed <= self.attempted:
            raise ValueError("success counts exceed attempted tasks")
        if self.unexpected_failures != self.attempted - self.passed:
            raise ValueError("unexpected failure count must equal non-passing tasks")
        if not math.isclose(self.success_rate, self.passed / self.attempted, abs_tol=1e-12):
            raise ValueError("success rate does not match result counts")
        return self


class EvaluationEnvironment(_Model):
    system: str
    release: str
    machine: str
    python: str
    logical_cpus: int | None = Field(default=None, ge=1)
    physical_memory_bytes: int | None = Field(default=None, ge=1)
    deployment_id: Annotated[str, StringConstraints(min_length=1, max_length=255)] | None = None


class CodingEvaluationReport(_Model):
    report_version: Literal["agent-coding-eval-v2"] = "agent-coding-eval-v2"
    mode: EvaluationMode
    result_claim: Literal["simulation_only", "measurement"]
    campaign_id: BoundedName
    source_revision: Annotated[
        str,
        StringConstraints(pattern=r"^(?:[0-9a-f]{7,64}|unavailable)$"),
    ]
    corpus_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    started_at: datetime
    completed_at: datetime
    methodology: tuple[BoundedText, ...] = Field(min_length=1, max_length=20)
    environment: EvaluationEnvironment
    results: tuple[CodingTaskResult, ...] = Field(min_length=MIN_TASKS, max_length=MAX_TASKS)
    aggregate: CodingAggregate

    @model_validator(mode="after")
    def validate_report(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("evaluation completion may not precede its start")
        expected_claim = "measurement" if self.mode is EvaluationMode.LIVE else "simulation_only"
        if self.result_claim != expected_claim:
            raise ValueError("result claim does not match evaluation mode")
        if self.mode is EvaluationMode.LIVE and self.environment.deployment_id is None:
            raise ValueError("live evaluation requires a deployment identity")
        if self.mode is EvaluationMode.SIMULATION and self.environment.deployment_id is not None:
            raise ValueError("simulation may not identify a live deployment")
        identifiers = tuple(item.task_id for item in self.results)
        if len(set(identifiers)) != len(identifiers):
            raise ValueError("evaluation results must have unique task identities")
        if self.aggregate.attempted != len(self.results):
            raise ValueError("aggregate attempted count must equal results")
        counts = Counter(item.category for item in self.results)
        if set(counts) != set(TaskCategory) or any(
            count < MIN_TASKS_PER_CATEGORY for count in counts.values()
        ):
            raise ValueError("evaluation results do not cover the complete corpus")
        corpus_entries = tuple(
            f"{item.task_sha256}:{item.fixture_sha256}:{item.verifier_sha256}"
            for item in self.results
        )
        if self.corpus_sha256 != _digest_lines(corpus_entries):
            raise ValueError("corpus digest does not match task results")
        expected_backend = "podman" if self.mode is EvaluationMode.LIVE else "simulation"
        if any(
            verification.backend != expected_backend
            for result in self.results
            for verification in result.observation.verifications
        ):
            raise ValueError("verification backend does not match evaluation mode")
        if self.aggregate != _aggregate(self.results):
            raise ValueError("aggregate metrics do not match task results")
        return self


class CodingEvaluationDriver(Protocol):
    """Trusted deployed-agent adapter; model-authored commands never run in this harness."""

    mode: EvaluationMode
    environment: EvaluationEnvironment

    async def evaluate(self, task: CodingTask, workspace: Path) -> TaskObservation:
        """Run one task through the target agent and return bounded observations."""


class SimulationCodingDriver:
    """Deterministic harness proof, explicitly excluded from coding-success claims."""

    mode = EvaluationMode.SIMULATION
    environment = EvaluationEnvironment(
        system=platform.system() or "unknown",
        release=platform.release() or "unknown",
        machine=platform.machine() or "unknown",
        python=platform.python_version(),
        logical_cpus=os.cpu_count(),
    )

    async def evaluate(self, task: CodingTask, workspace: Path) -> TaskObservation:
        if not await asyncio.to_thread(_has_python_files, workspace):
            raise RuntimeError("fixture was not materialized")
        changed_paths = await asyncio.to_thread(_apply_reference_solution, task, workspace)
        passed = await asyncio.to_thread(_verify_reference_solution, task, workspace)
        latency = 0.05 + task.variant / 100
        output = f"reference-verifier:{task.id}:{'passed' if passed else 'failed'}"
        verifications = tuple(
            VerificationResult(
                argv=command.argv,
                backend="simulation",
                exit_code=0 if passed else 1,
                duration_seconds=0,
                output_sha256=hashlib.sha256(output.encode("utf-8")).hexdigest(),
            )
            for command in task.verification
        )
        return TaskObservation(
            outcome="completed" if passed else "failed",
            tests_passed=passed,
            committed_changes=1 if passed else 0,
            changed_paths=changed_paths,
            verifications=verifications,
            metrics=TaskMetrics(
                iterations=task.variant + 1,
                tool_calls=task.variant + 2,
                input_tokens=1_000 + task.variant,
                output_tokens=200 + task.variant,
                cost_usd=0.001 * task.variant,
                queue_wait_seconds=0.01,
                first_token_seconds=0.02,
                total_latency_seconds=latency,
                retries=0,
                fallbacks=0,
                permission_denials=0,
            ),
            error_category=None if passed else "reference_verification_failed",
        )


def load_tasks(path: Path) -> tuple[CodingTask, ...]:
    payload = path.read_bytes()
    if not payload or len(payload) > MAX_MANIFEST_BYTES:
        raise ValueError("coding-task manifest is empty or exceeds the byte limit")
    try:
        raw = yaml.safe_load(payload)
    except yaml.YAMLError as error:
        raise ValueError("coding-task manifest contains invalid YAML") from error
    tasks = tuple(_TASK_ADAPTER.validate_python(raw))
    if not MIN_TASKS <= len(tasks) <= MAX_TASKS:
        raise ValueError(f"coding-task campaigns require {MIN_TASKS}-{MAX_TASKS} tasks")
    identifiers = tuple(item.id for item in tasks)
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("coding-task identities must be unique")
    counts = Counter(task.category for task in tasks)
    if set(counts) != set(TaskCategory) or any(
        count < MIN_TASKS_PER_CATEGORY for count in counts.values()
    ):
        raise ValueError("every required task category must have at least three tasks")
    return tasks


def materialize_task(task: CodingTask, parent: Path) -> Path:
    root = parent / task.id
    if root.exists() or root.is_symlink():
        raise ValueError("task workspace already exists")
    root.mkdir(mode=0o700, parents=False)
    files = _fixture_files(task.fixture_template, task.variant)
    if not _fixture_is_deficient(task, files):
        raise ValueError("task fixture does not exhibit its declared deficiency")
    if not files or len(files) > MAX_FIXTURE_FILES:
        raise ValueError("task fixture file count is invalid")
    total = sum(len(content.encode("utf-8")) for content in files.values())
    if total > MAX_FIXTURE_BYTES:
        raise ValueError("task fixture exceeds its byte limit")
    for relative, content in sorted(files.items()):
        normalized = _relative_path(relative)
        destination = root.joinpath(*PurePosixPath(normalized).parts)
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        destination.write_text(content, encoding="utf-8")
        destination.chmod(0o600)
    return root


async def run_coding_evaluation(
    tasks: Sequence[CodingTask],
    driver: CodingEvaluationDriver,
    *,
    campaign_id: str,
    source_revision: str,
    now: Callable[[], datetime] | None = None,
) -> CodingEvaluationReport:
    if not MIN_TASKS <= len(tasks) <= MAX_TASKS:
        raise ValueError(f"coding-task campaigns require {MIN_TASKS}-{MAX_TASKS} tasks")
    clock = now or (lambda: datetime.now(UTC))
    started_at = clock()
    results: list[CodingTaskResult] = []
    with tempfile.TemporaryDirectory(prefix="agent-coding-eval-") as temporary:
        root = Path(temporary)
        for task in tasks:
            workspace = materialize_task(task, root)
            fixture_sha256 = await asyncio.to_thread(_workspace_digest, workspace)
            observation = await _evaluate_one(driver, task, workspace)
            failures = _task_failures(task, observation)
            results.append(
                CodingTaskResult(
                    task_id=task.id,
                    category=task.category,
                    task_sha256=_task_digest(task),
                    fixture_sha256=fixture_sha256,
                    verifier_sha256=_verifier_digest(task),
                    expected_outcomes=task.expected_outcomes,
                    observation=observation,
                    passed=not failures,
                    failures=failures,
                )
            )
    aggregate = _aggregate(tuple(results))
    return CodingEvaluationReport(
        mode=driver.mode,
        result_claim="measurement" if driver.mode is EvaluationMode.LIVE else "simulation_only",
        campaign_id=campaign_id,
        source_revision=source_revision,
        corpus_sha256=_digest_lines(
            tuple(
                f"{item.task_sha256}:{item.fixture_sha256}:{item.verifier_sha256}"
                for item in results
            )
        ),
        started_at=started_at,
        completed_at=clock(),
        methodology=(
            "Materialize each versioned fixture into a private temporary workspace.",
            "Delegate agent and sandbox execution to an injected trusted driver.",
            "Record every expected and unexpected outcome with bounded per-task metrics.",
        ),
        environment=driver.environment,
        results=tuple(results),
        aggregate=aggregate,
    )


async def _evaluate_one(
    driver: CodingEvaluationDriver,
    task: CodingTask,
    workspace: Path,
) -> TaskObservation:
    started = time.monotonic()
    try:
        before = await asyncio.to_thread(_workspace_manifest, workspace)
        async with asyncio.timeout(task.timeout_seconds):
            observation = await driver.evaluate(task, workspace)
        after = await asyncio.to_thread(_workspace_manifest, workspace)
        changed_paths = tuple(
            sorted(
                path for path in before.keys() | after.keys() if before.get(path) != after.get(path)
            )
        )
        if observation.outcome is TaskOutcome.COMPLETED:
            return observation.model_copy(update={"changed_paths": changed_paths})
        else:
            return observation
    except TimeoutError:
        category = "evaluation_timeout"
        outcome = TaskOutcome.TIMEOUT
    except Exception:
        category = "evaluation_driver_error"
        outcome = TaskOutcome.FAILED
    return TaskObservation(
        outcome=outcome,
        tests_passed=False,
        committed_changes=0,
        changed_paths=(),
        metrics=TaskMetrics(
            iterations=0,
            tool_calls=0,
            total_latency_seconds=min(time.monotonic() - started, 86_400),
            retries=0,
            fallbacks=0,
            permission_denials=0,
        ),
        error_category=category,
    )


def _task_failures(task: CodingTask, observation: TaskObservation) -> tuple[BoundedName, ...]:
    failures: list[BoundedName] = []
    if observation.outcome not in task.expected_outcomes:
        failures.append("unexpected_outcome")
    if observation.outcome is TaskOutcome.COMPLETED and not observation.tests_passed:
        failures.append("tests_not_passed")
    if observation.committed_changes > 1:
        failures.append("duplicate_commit")
    if observation.outcome is TaskOutcome.COMPLETED:
        observed = set(observation.changed_paths)
        expected = set(task.expected_changed_paths)
        if not observed:
            failures.append("workspace_not_changed")
        elif not observed.issubset(expected):
            failures.append("unexpected_changed_path")
        configured = tuple(command.argv for command in task.verification)
        measured = tuple(result.argv for result in observation.verifications)
        if measured != configured:
            failures.append("verification_evidence_mismatch")
    return tuple(failures)


def _aggregate(results: tuple[CodingTaskResult, ...]) -> CodingAggregate:
    observations = tuple(item.observation for item in results)
    passed = sum(item.passed for item in results)
    return CodingAggregate(
        attempted=len(results),
        passed=passed,
        tests_passed=sum(item.observation.tests_passed for item in results),
        unexpected_failures=len(results) - passed,
        duplicate_commits=sum(item.observation.committed_changes > 1 for item in results),
        success_rate=passed / len(results),
        outcomes=dict(Counter(item.outcome for item in observations)),
        total_latency_seconds=_summary(
            [item.metrics.total_latency_seconds for item in observations]
        ),
        queue_wait_seconds=_summary([item.metrics.queue_wait_seconds for item in observations]),
        first_token_seconds=_summary([item.metrics.first_token_seconds for item in observations]),
        iterations=_summary([float(item.metrics.iterations) for item in observations]),
        tool_calls=_summary([float(item.metrics.tool_calls) for item in observations]),
        input_tokens=_summary([_float(item.metrics.input_tokens) for item in observations]),
        output_tokens=_summary([_float(item.metrics.output_tokens) for item in observations]),
        cost_usd=_summary([item.metrics.cost_usd for item in observations]),
        retries=_summary([float(item.metrics.retries) for item in observations]),
        fallbacks=_summary([float(item.metrics.fallbacks) for item in observations]),
        permission_denials=_summary(
            [float(item.metrics.permission_denials) for item in observations]
        ),
    )


def _summary(values: Sequence[float | None]) -> MetricSummary:
    observed = sorted(item for item in values if item is not None)
    if not observed:
        return MetricSummary(count=0)
    return MetricSummary(
        count=len(observed),
        minimum=observed[0],
        p50=_percentile(observed, 0.50),
        p95=_percentile(observed, 0.95),
        maximum=observed[-1],
        mean=sum(observed) / len(observed),
    )


def _percentile(values: Sequence[float], percentile: float) -> float:
    index = math.ceil(percentile * len(values)) - 1
    return values[max(0, min(index, len(values) - 1))]


def _float(value: int | None) -> float | None:
    return float(value) if value is not None else None


def _relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("workspace paths must be canonical contained relative paths")
    return path.as_posix()


def _fixture_files(category: TaskCategory, variant: int) -> dict[str, str]:
    suffix = str(variant)
    fixtures: dict[TaskCategory, dict[str, str]] = {
        TaskCategory.FIX_FAILING_TEST: {
            f"calculator_{suffix}.py": (
                "def add(left: int, right: int) -> int:\n    return left - right\n"
            ),
            f"test_calculator_{suffix}.py": (
                f"from calculator_{suffix} import add\n\n"
                "def test_add():\n    assert add(2, 3) == 5\n"
            ),
        },
        TaskCategory.API_ENDPOINT: {
            f"api_{suffix}.py": "ROUTES: dict[str, object] = {}\n",
            f"test_api_{suffix}.py": (
                f"from api_{suffix} import ROUTES\n\n"
                "def test_health_route_is_registered():\n"
                "    assert ROUTES['/health']() == {'status': 'ok'}\n"
            ),
        },
        TaskCategory.TYPE_ERROR: {
            f"typed_{suffix}.py": "def length(value: str) -> int:\n    return value\n",
            f"test_typed_{suffix}.py": (
                f"from typed_{suffix} import length\n\n"
                "def test_length():\n    assert length('agent') == 5\n"
            ),
        },
        TaskCategory.MULTI_FILE_RENAME: {
            f"service_{suffix}.py": "def old_name() -> str:\n    return 'ok'\n",
            f"consumer_{suffix}.py": f"from service_{suffix} import old_name\n",
            f"test_rename_{suffix}.py": (
                f"from service_{suffix} import current_name\n"
                f"from consumer_{suffix} import call_service\n\n"
                "def test_current_name():\n"
                "    assert current_name() == 'ok'\n"
                "    assert call_service() == 'ok'\n"
            ),
        },
        TaskCategory.INPUT_VALIDATION: {
            f"validation_{suffix}.py": "def port(value: int) -> int:\n    return value\n",
            f"test_validation_{suffix}.py": (
                "import pytest\n"
                f"from validation_{suffix} import port\n\n"
                "def test_valid_port():\n    assert port(443) == 443\n\n"
                "@pytest.mark.parametrize('value', [True, 0, 65536])\n"
                "def test_invalid_port(value):\n"
                "    with pytest.raises((TypeError, ValueError)):\n"
                "        port(value)\n"
            ),
        },
        TaskCategory.DEDUP_REFACTOR: {
            f"dedup_{suffix}.py": (
                "def one(x: int) -> int:\n    return x + 1\n\n"
                "def two(x: int) -> int:\n    return x + 1\n"
            ),
            f"test_dedup_{suffix}.py": (
                f"from dedup_{suffix} import one, two\n\n"
                "def test_public_behavior():\n"
                "    assert one(4) == 5\n"
                "    assert two(4) == 5\n"
            ),
        },
        TaskCategory.DEPENDENCY_USAGE: {
            f"dependency_{suffix}.py": (
                "import json\n\ndef encode(value: object) -> str:\n    return str(value)\n"
            ),
            f"test_dependency_{suffix}.py": (
                f"from dependency_{suffix} import encode\n\n"
                "def test_canonical_json():\n"
                "    assert encode({'b': 2, 'a': 1}) == '{\"a\":1,\"b\":2}'\n"
            ),
        },
        TaskCategory.CONCURRENCY_BUG: {
            f"counter_{suffix}.py": (
                "import asyncio\n\nvalue = 0\n\n"
                "async def increment() -> None:\n"
                "    global value\n"
                "    current = value\n"
                "    await asyncio.sleep(0)\n"
                "    value = current + 1\n"
            ),
            f"test_counter_{suffix}.py": (
                "import asyncio\n"
                f"import counter_{suffix} as counter\n\n"
                "def test_concurrent_increments():\n"
                "    counter.value = 0\n"
                "    async def exercise():\n"
                "        await asyncio.gather(*(counter.increment() for _ in range(20)))\n"
                "    asyncio.run(exercise())\n"
                "    assert counter.value == 20\n"
            ),
        },
        TaskCategory.MISSING_TESTS: {
            f"normalize_{suffix}.py": (
                "def normalize(value: str) -> str:\n    return value.strip().lower()\n"
            ),
        },
        TaskCategory.DIAGNOSE_EXCEPTION: {
            f"parser_{suffix}.py": (
                "def parse(value: str) -> int:\n    return int(value.split(':')[1])\n"
            ),
            f"test_parser_{suffix}.py": (
                "import pytest\n"
                f"from parser_{suffix} import parse\n\n"
                "def test_valid_value():\n    assert parse('port:42') == 42\n\n"
                "def test_malformed_value_is_structured():\n"
                "    with pytest.raises(ValueError, match='key:value'):\n"
                "        parse('malformed')\n"
            ),
        },
    }
    return fixtures[category]


def _reference_solution(task: CodingTask) -> dict[str, str]:  # noqa: PLR0911
    suffix = str(task.variant)
    if task.category is TaskCategory.FIX_FAILING_TEST:
        return {
            f"calculator_{suffix}.py": (
                "def add(left: int, right: int) -> int:\n    return left + right\n"
            )
        }
    if task.category is TaskCategory.API_ENDPOINT:
        return {
            f"api_{suffix}.py": (
                "def health() -> dict[str, str]:\n    return {'status': 'ok'}\n\n"
                "ROUTES: dict[str, object] = {'/health': health}\n"
            )
        }
    if task.category is TaskCategory.TYPE_ERROR:
        return {f"typed_{suffix}.py": "def length(value: str) -> int:\n    return len(value)\n"}
    if task.category is TaskCategory.MULTI_FILE_RENAME:
        return {
            f"service_{suffix}.py": "def current_name() -> str:\n    return 'ok'\n",
            f"consumer_{suffix}.py": (
                f"from service_{suffix} import current_name\n\n"
                "def call_service() -> str:\n    return current_name()\n"
            ),
        }
    if task.category is TaskCategory.INPUT_VALIDATION:
        return {
            f"validation_{suffix}.py": (
                "def port(value: int) -> int:\n"
                "    if isinstance(value, bool) or not isinstance(value, int):\n"
                "        raise TypeError('port must be an integer')\n"
                "    if not 1 <= value <= 65535:\n"
                "        raise ValueError('port is outside the TCP range')\n"
                "    return value\n"
            )
        }
    if task.category is TaskCategory.DEDUP_REFACTOR:
        return {
            f"dedup_{suffix}.py": (
                "def _increment(value: int) -> int:\n    return value + 1\n\n"
                "def one(x: int) -> int:\n    return _increment(x)\n\n"
                "def two(x: int) -> int:\n    return _increment(x)\n"
            )
        }
    if task.category is TaskCategory.DEPENDENCY_USAGE:
        return {
            f"dependency_{suffix}.py": (
                "import json\n\n"
                "def encode(value: object) -> str:\n"
                "    return json.dumps(value, sort_keys=True, separators=(',', ':'))\n"
            )
        }
    if task.category is TaskCategory.CONCURRENCY_BUG:
        return {
            f"counter_{suffix}.py": (
                "import asyncio\n\nvalue = 0\n_lock = asyncio.Lock()\n\n"
                "async def increment() -> None:\n"
                "    global value\n"
                "    async with _lock:\n"
                "        value += 1\n"
            )
        }
    if task.category is TaskCategory.MISSING_TESTS:
        return {
            f"test_normalize_{suffix}.py": (
                f"from normalize_{suffix} import normalize\n\n"
                "def test_normalize_boundaries():\n"
                "    assert normalize('  Agent  ') == 'agent'\n"
                "    assert normalize('') == ''\n"
                "    assert normalize('STRASSE') == 'strasse'\n"
            )
        }
    return {
        f"parser_{suffix}.py": (
            "def parse(value: str) -> int:\n"
            "    key, separator, raw = value.partition(':')\n"
            "    if not separator or not key or not raw:\n"
            "        raise ValueError('expected key:value')\n"
            "    return int(raw)\n"
        )
    }


def _fixture_is_deficient(task: CodingTask, files: Mapping[str, str]) -> bool:
    if any("assert True" in content for content in files.values()):
        return False
    solution = _reference_solution(task)
    return bool(solution) and any(files.get(path) != content for path, content in solution.items())


def _apply_reference_solution(task: CodingTask, workspace: Path) -> tuple[str, ...]:
    before = _workspace_manifest(workspace)
    for relative, content in _reference_solution(task).items():
        destination = workspace.joinpath(*PurePosixPath(relative).parts)
        destination.write_text(content, encoding="utf-8")
        destination.chmod(0o600)
    after = _workspace_manifest(workspace)
    return tuple(
        sorted(path for path in before.keys() | after.keys() if before.get(path) != after.get(path))
    )


def _verify_reference_solution(task: CodingTask, workspace: Path) -> bool:
    return all(
        (workspace / relative).is_file()
        and (workspace / relative).read_text(encoding="utf-8") == expected
        for relative, expected in _reference_solution(task).items()
    )


def _workspace_manifest(workspace: Path) -> dict[str, tuple[int, int, str]]:
    root = workspace.resolve(strict=True)
    manifest: dict[str, tuple[int, int, str]] = {}
    total_bytes = 0
    for path in sorted(root.rglob("*")):
        metadata = path.lstat()
        if stat.S_ISDIR(metadata.st_mode):
            continue
        if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
            raise ValueError("evaluation workspace contains a symlink or special file")
        if len(manifest) >= MAX_WORKSPACE_FILES:
            raise ValueError("evaluation workspace exceeds its file-count limit")
        relative = path.relative_to(root).as_posix()
        size = metadata.st_size
        total_bytes += size
        if size > MAX_WORKSPACE_BYTES or total_bytes > MAX_WORKSPACE_BYTES:
            raise ValueError("evaluation workspace exceeds its byte limit")
        digest = hashlib.sha256()
        with path.open("rb") as source:
            while chunk := source.read(64 * 1024):
                digest.update(chunk)
        manifest[relative] = (stat.S_IMODE(metadata.st_mode), size, digest.hexdigest())
    return manifest


def _workspace_digest(workspace: Path) -> str:
    payload = json.dumps(
        _workspace_manifest(workspace),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _task_digest(task: CodingTask) -> str:
    payload = json.dumps(
        task.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _verifier_digest(task: CodingTask) -> str:
    payload = json.dumps(
        _reference_solution(task),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(b"agent-coding-hidden-verifier-v1\x00" + payload).hexdigest()


def _digest_lines(values: Sequence[str]) -> str:
    digest = hashlib.sha256()
    for value in values:
        digest.update(value.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def _has_python_files(workspace: Path) -> bool:
    return workspace.is_dir() and next(workspace.rglob("*.py"), None) is not None


def load_driver_factory(specification: str) -> CodingEvaluationDriver:
    if _FACTORY_PATTERN.fullmatch(specification) is None:
        raise ValueError("driver must use a bounded module:attribute reference")
    module_name, _, attribute = specification.partition(":")
    factory = getattr(importlib.import_module(module_name), attribute, None)
    if not callable(factory):
        raise TypeError("coding evaluation driver factory must be callable")
    driver = factory()
    if inspect.isawaitable(driver):
        raise TypeError("coding evaluation driver factories must be synchronous")
    return cast("CodingEvaluationDriver", driver)


def write_report(path: Path, report: CodingEvaluationReport) -> None:
    destination = path.absolute()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError("evaluation report destination may not be a symlink")
    payload = report.model_dump_json(indent=2).encode("utf-8") + b"\n"
    if len(payload) > MAX_REPORT_BYTES:
        raise ValueError("evaluation report exceeds the byte limit")
    descriptor, temporary = tempfile.mkstemp(prefix=".coding-eval-", dir=destination.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        Path(temporary).replace(destination)
    finally:
        Path(temporary).unlink(missing_ok=True)


async def _run(arguments: argparse.Namespace) -> int:
    tasks = load_tasks(arguments.tasks)
    driver = load_driver_factory(arguments.driver) if arguments.driver else SimulationCodingDriver()
    if driver.mode.value != arguments.mode:
        raise ValueError("driver evidence mode does not match --mode")
    if arguments.mode == EvaluationMode.LIVE.value and arguments.driver is None:
        raise ValueError("live mode requires an explicit trusted driver")
    report = await run_coding_evaluation(
        tasks,
        driver,
        campaign_id=arguments.campaign_id,
        source_revision=arguments.source_revision,
    )
    write_report(arguments.output, report)
    return 0 if report.aggregate.unexpected_failures == 0 else 1


def main(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=Path, default=Path("benchmarks/coding_tasks/tasks.yaml"))
    parser.add_argument(
        "--mode", choices=[item.value for item in EvaluationMode], default="simulation"
    )
    parser.add_argument("--driver")
    parser.add_argument("--campaign-id", default="coding-evaluation")
    parser.add_argument("--source-revision", default="unavailable")
    parser.add_argument("--output", type=Path, required=True)
    raise SystemExit(asyncio.run(_run(parser.parse_args(arguments))))


if __name__ == "__main__":
    main()
