from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from scripts.quality_gate_report import (
    QualityGateName,
    QualityGateReport,
    run_quality_gates,
    write_quality_report,
)

from agent_core.tools import ToolOutputChannel
from sandbox_runtime import ProcessChunk, ProcessResult

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

NOW = datetime(2026, 1, 1, tzinfo=UTC)


class _Runner:
    def __init__(self, *, fail_at: int | None = None) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._fail_at = fail_at

    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> ProcessResult:
        del cwd, timeout_seconds, max_output_bytes, environment
        call = tuple(argv)
        self.calls.append(call)
        failed = self._fail_at == len(self.calls)
        return ProcessResult(
            chunks=(
                ProcessChunk(
                    channel=ToolOutputChannel.STDOUT,
                    text="failed\n" if failed else "passed\n",
                ),
            ),
            exit_code=1 if failed else 0,
        )


@pytest.mark.asyncio
async def test_quality_runner_executes_fixed_complete_gate_matrix(tmp_path: Path) -> None:
    _repository(tmp_path)
    runner = _Runner()

    report = await run_quality_gates(
        tmp_path,
        source_revision="abcdef1",
        source_dirty=True,
        runner=runner,
        now=lambda: NOW,
    )

    assert report.passed
    assert {result.name for result in report.gates} == set(QualityGateName)
    assert len(runner.calls) == 11
    assert all("-c" not in call for call in runner.calls)
    assert all(result.commands[0].output_sha256 != "0" * 64 for result in report.gates)
    assert all(
        not argument.startswith(str(tmp_path))
        for gate in report.gates
        for command in gate.commands
        for argument in command.argv
    )
    assert report.gates[0].commands[0].argv[0] == ".venv/bin/ruff"
    output = tmp_path / "quality.json"
    write_quality_report(output, report)
    assert QualityGateReport.model_validate_json(output.read_bytes()) == report


@pytest.mark.asyncio
async def test_quality_runner_records_failure_without_claiming_success(tmp_path: Path) -> None:
    _repository(tmp_path)
    report = await run_quality_gates(
        tmp_path,
        source_revision="unavailable",
        source_dirty=True,
        runner=_Runner(fail_at=3),
        now=lambda: NOW,
    )

    assert not report.passed
    mypy = next(result for result in report.gates if result.name is QualityGateName.STRICT_MYPY)
    assert not mypy.passed
    with pytest.raises(ValidationError, match="quality report status"):
        report.model_copy(update={"passed": True})


def _repository(root: Path) -> None:
    root.joinpath("pyproject.toml").write_text("[project]\nname='test'\nversion='0'\n")
    binary = root / ".venv" / "bin"
    binary.mkdir(parents=True)
    for name in ("ruff", "mypy", "pytest", "pre-commit", "pip-audit", "uv"):
        executable = binary / name
        executable.write_text("placeholder")
        executable.chmod(0o700)
