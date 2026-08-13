"""Run fixed repository quality gates and emit bounded machine-readable evidence."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import platform
import shutil
import tempfile
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from sandbox_runtime import BoundedProcessRunner, ProcessResult

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from typing import Any

MAX_GATE_OUTPUT_BYTES = 8 * 1024 * 1024
MAX_REPORT_BYTES = 4 * 1024 * 1024
MAX_GATE_SECONDS = 1_800
type Clock = Callable[[], datetime]


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


class QualityGateName(StrEnum):
    RUFF_FORMAT = "ruff_format"
    RUFF_LINT = "ruff_lint"
    STRICT_MYPY = "strict_mypy"
    UNIT_COVERAGE = "unit_coverage"
    INTEGRATION = "integration"
    PRE_COMMIT = "pre_commit"
    DEPENDENCY_AUDIT = "dependency_audit"
    FROZEN_LOCK = "frozen_lock"
    PACKAGE_BUILDS = "package_builds"
    DIFF_CHECK = "diff_check"


class GateCommandResult(_Model):
    argv: tuple[Annotated[str, StringConstraints(min_length=1, max_length=1_000)], ...] = Field(
        min_length=1,
        max_length=50,
    )
    exit_code: int = Field(ge=-255, le=255)
    timed_out: bool
    output_truncated: bool
    output_bytes: int = Field(ge=0, le=MAX_GATE_OUTPUT_BYTES)
    output_sha256: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
    duration_seconds: float = Field(ge=0, le=MAX_GATE_SECONDS)

    @property
    def passed(self) -> bool:
        return self.exit_code == 0 and not self.timed_out and not self.output_truncated


class QualityGateResult(_Model):
    name: QualityGateName
    commands: tuple[GateCommandResult, ...] = Field(min_length=1, max_length=5)
    passed: bool

    @model_validator(mode="after")
    def validate_passed(self) -> Self:
        if self.passed != all(command.passed for command in self.commands):
            raise ValueError("quality gate status must match its command evidence")
        return self


class QualityEnvironment(_Model):
    system: str
    release: str
    machine: str
    python: str


class QualityGateReport(_Model):
    report_version: Literal["agent-quality-gates-v1"] = "agent-quality-gates-v1"
    result_claim: Literal["measurement"] = "measurement"
    source_revision: Annotated[
        str,
        StringConstraints(pattern=r"^(?:[0-9a-f]{7,64}|unavailable)$"),
    ]
    source_dirty: bool
    started_at: datetime
    completed_at: datetime
    environment: QualityEnvironment
    gates: tuple[QualityGateResult, ...] = Field(min_length=10, max_length=10)
    passed: bool

    @model_validator(mode="after")
    def validate_report(self) -> Self:
        if self.completed_at < self.started_at:
            raise ValueError("quality report completion may not precede its start")
        names = tuple(result.name for result in self.gates)
        if len(set(names)) != len(names) or set(names) != set(QualityGateName):
            raise ValueError("quality report must contain every gate exactly once")
        if self.passed != all(result.passed for result in self.gates):
            raise ValueError("quality report status must match all gate results")
        return self


class QualityCommandRunner(Protocol):
    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> ProcessResult: ...


async def run_quality_gates(
    repository: Path,
    *,
    source_revision: str,
    source_dirty: bool,
    runner: QualityCommandRunner | None = None,
    now: Clock | None = None,
) -> QualityGateReport:
    """Execute the fixed gate matrix without a shell or model-provided argv."""

    root = await asyncio.to_thread(_resolve_repository, repository)
    executable = root / ".venv" / "bin"
    required = ("ruff", "mypy", "pytest", "pre-commit", "pip-audit", "uv")
    binaries = {name: _executable(executable / name) for name in required}
    git = _resolve_git()
    audit_requirements = root / ".cache" / "audit-requirements.txt"
    command_sets: tuple[tuple[QualityGateName, tuple[tuple[str, ...], ...]], ...] = (
        (
            QualityGateName.RUFF_FORMAT,
            (
                (
                    binaries["ruff"],
                    "format",
                    "--check",
                    "apps",
                    "packages",
                    "scripts",
                    "tests",
                    "services/fake-llm/app.py",
                ),
            ),
        ),
        (
            QualityGateName.RUFF_LINT,
            (
                (
                    binaries["ruff"],
                    "check",
                    "apps",
                    "packages",
                    "scripts",
                    "tests",
                    "services/fake-llm/app.py",
                ),
            ),
        ),
        (
            QualityGateName.STRICT_MYPY,
            ((binaries["mypy"], "apps", "packages", "scripts", "tests"),),
        ),
        (
            QualityGateName.UNIT_COVERAGE,
            ((binaries["pytest"], "tests/unit", "--cov", "--cov-report=term-missing"),),
        ),
        (QualityGateName.INTEGRATION, ((binaries["pytest"], "tests/integration"),)),
        (QualityGateName.PRE_COMMIT, ((binaries["pre-commit"], "run", "--all-files"),)),
        (
            QualityGateName.DEPENDENCY_AUDIT,
            (
                (
                    binaries["uv"],
                    "export",
                    "--all-packages",
                    "--frozen",
                    "--no-emit-workspace",
                    "--no-hashes",
                    "--output-file",
                    str(audit_requirements),
                ),
                (
                    binaries["pip-audit"],
                    "--requirement",
                    str(audit_requirements),
                    "--no-deps",
                    "--disable-pip",
                    "--cache-dir",
                    str(root / ".cache" / "pip-audit"),
                ),
            ),
        ),
        (QualityGateName.FROZEN_LOCK, ((binaries["uv"], "lock", "--check"),)),
        (QualityGateName.PACKAGE_BUILDS, ((binaries["uv"], "build", "--all-packages"),)),
        (QualityGateName.DIFF_CHECK, ((git, "diff", "--check"),)),
    )
    clock = now or (lambda: datetime.now(UTC))
    started_at = clock()
    actual_runner = runner or BoundedProcessRunner()
    environment = _environment(root)
    results: list[QualityGateResult] = []
    for name, commands in command_sets:
        evidence: list[GateCommandResult] = []
        for command in commands:
            began = asyncio.get_running_loop().time()
            result = await actual_runner.run(
                command,
                cwd=root,
                timeout_seconds=MAX_GATE_SECONDS,
                max_output_bytes=MAX_GATE_OUTPUT_BYTES,
                environment=environment,
            )
            evidence.append(
                _command_evidence(
                    _portable_argv(root, command),
                    result,
                    asyncio.get_running_loop().time() - began,
                )
            )
            if not evidence[-1].passed:
                break
        results.append(
            QualityGateResult(
                name=name,
                commands=tuple(evidence),
                passed=(
                    all(command.passed for command in evidence) and len(evidence) == len(commands)
                ),
            )
        )
    return QualityGateReport(
        source_revision=source_revision,
        source_dirty=source_dirty,
        started_at=started_at,
        completed_at=clock(),
        environment=QualityEnvironment(
            system=platform.system() or "unknown",
            release=platform.release() or "unknown",
            machine=platform.machine() or "unknown",
            python=platform.python_version(),
        ),
        gates=tuple(results),
        passed=all(result.passed for result in results),
    )


def _command_evidence(
    argv: tuple[str, ...],
    result: ProcessResult,
    duration_seconds: float,
) -> GateCommandResult:
    digest = hashlib.sha256()
    output_bytes = 0
    for chunk in result.chunks:
        encoded = chunk.text.encode("utf-8")
        digest.update(chunk.channel.value.encode("ascii"))
        digest.update(b"\x00")
        digest.update(encoded)
        output_bytes += len(encoded)
    return GateCommandResult(
        argv=argv,
        exit_code=result.exit_code,
        timed_out=result.timed_out,
        output_truncated=result.output_truncated,
        output_bytes=output_bytes,
        output_sha256=digest.hexdigest(),
        duration_seconds=min(max(0.0, duration_seconds), MAX_GATE_SECONDS),
    )


def _portable_argv(root: Path, argv: tuple[str, ...]) -> tuple[str, ...]:
    """Remove checkout-specific prefixes from persisted command evidence."""

    portable: list[str] = []
    for argument in argv:
        candidate = Path(argument)
        normalized = argument
        if candidate.is_absolute() and candidate.is_relative_to(root):
            normalized = candidate.relative_to(root).as_posix()
        portable.append(normalized)
    return tuple(portable)


def write_quality_report(path: Path, report: QualityGateReport) -> None:
    destination = path.absolute()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError("quality report destination may not be a symlink")
    payload = report.model_dump_json(indent=2).encode("utf-8") + b"\n"
    if len(payload) > MAX_REPORT_BYTES:
        raise ValueError("quality report exceeds its byte limit")
    descriptor, temporary = tempfile.mkstemp(prefix=".quality-gates-", dir=destination.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        Path(temporary).replace(destination)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _executable(path: Path) -> str:
    resolved = path.resolve(strict=True)
    if not resolved.is_file() or not resolved.stat().st_mode & 0o111:
        raise ValueError(f"required quality executable is unavailable: {path.name}")
    return str(resolved)


def _resolve_repository(repository: Path) -> Path:
    root = repository.resolve(strict=True)
    if not root.is_dir() or not (root / "pyproject.toml").is_file():
        raise ValueError("repository must contain the platform pyproject.toml")
    return root


def _resolve_git() -> str:
    for candidate in (Path("/usr/bin/git"), Path("/opt/homebrew/bin/git")):
        if candidate.is_file() and candidate.stat().st_mode & 0o111:
            return str(candidate)
    raise ValueError("an absolute Git executable is required")


def _environment(root: Path) -> dict[str, str]:
    command_paths = [root / ".venv" / "bin", Path("/usr/bin"), Path("/bin")]
    command_paths.extend(
        candidate
        for candidate in (Path("/opt/homebrew/bin"), Path("/usr/local/bin"))
        if candidate.is_dir()
    )
    ripgrep = shutil.which("rg")
    if ripgrep is not None:
        command_paths.append(Path(ripgrep).resolve().parent)
    values = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": ":".join(str(path) for path in command_paths),
        "UV_CACHE_DIR": str(root / ".cache" / "uv"),
        "PRE_COMMIT_HOME": str(root / ".cache" / "pre-commit"),
        "PIP_CACHE_DIR": str(root / ".cache" / "pip"),
        "RUFF_CACHE_DIR": str(root / ".cache" / "ruff"),
    }
    for name in ("HOME", "TMPDIR", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE"):
        value = os.environ.get(name)
        if value:
            values[name] = value
    return values


async def _run(arguments: argparse.Namespace) -> int:
    report = await run_quality_gates(
        arguments.repository,
        source_revision=arguments.source_revision,
        source_dirty=arguments.source_dirty,
    )
    write_quality_report(arguments.output, report)
    return 0 if report.passed else 1


def main(arguments: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path())
    parser.add_argument("--source-revision", default="unavailable")
    parser.add_argument("--source-dirty", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    raise SystemExit(asyncio.run(_run(parser.parse_args(arguments))))


if __name__ == "__main__":
    main()


__all__ = [
    "GateCommandResult",
    "QualityGateName",
    "QualityGateReport",
    "QualityGateResult",
    "run_quality_gates",
    "write_quality_report",
]
