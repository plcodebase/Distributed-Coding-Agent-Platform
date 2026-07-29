import asyncio
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from agent_core.domain import DomainOperationError, FrozenJsonObject
from agent_core.sandbox import CommandCompleted, CommandOutput, CommandSpec
from agent_core.tools import (
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolOutputChunk,
)
from sandbox_runtime import GitWorktreeManager, LocalSandbox, WorkspaceToolset

GIT = shutil.which("git") or "/usr/bin/git"


def git(repository: Path, *arguments: str) -> None:
    subprocess.run(  # noqa: S603 - tests invoke a resolved Git executable
        (GIT, "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
    )


def create_repository(path: Path) -> None:
    path.mkdir()
    git(path, "init", "-q")
    git(path, "config", "user.name", "Test")
    git(path, "config", "user.email", "test@example.invalid")
    (path / "tracked.txt").write_text("base\n", encoding="utf-8")
    git(path, "add", ".")
    git(path, "commit", "-q", "-m", "initial")


async def collect_command(
    sandbox: LocalSandbox,
    command: CommandSpec,
) -> list[CommandOutput | CommandCompleted]:
    return [event async for event in sandbox.execute(command)]


async def test_local_sandbox_is_disabled_by_default(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-disabled",
    )
    sandbox = LocalSandbox(workspace)
    try:
        with pytest.raises(DomainOperationError) as disabled:
            await collect_command(
                sandbox,
                CommandSpec(
                    argv=(sys.executable, "-c", "print('never')"),
                    timeout_seconds=1,
                    max_output_bytes=100,
                ),
            )
        assert disabled.value.code == "local_execution_disabled"
    finally:
        await sandbox.destroy()


async def test_local_sandbox_streams_bounded_output_and_minimal_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AGENT_TEST_SECRET", "must-not-pass-through")
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-enabled",
    )
    sandbox = LocalSandbox(
        workspace,
        unsafe_allow_host_execution=True,
        runtime_environment="test",
    )
    try:
        events = await collect_command(
            sandbox,
            CommandSpec(
                argv=(
                    sys.executable,
                    "-c",
                    (
                        "import os,sys;"
                        "sys.stdout.write(os.environ.get('AGENT_TEST_SECRET','absent'));"
                        "sys.stderr.write('error')"
                    ),
                ),
                timeout_seconds=2,
                max_output_bytes=100,
            ),
        )
        output = [event for event in events if isinstance(event, CommandOutput)]
        terminal = events[-1]
        assert {event.channel.value for event in output} == {"stdout", "stderr"}
        assert "must-not-pass-through" not in "".join(event.chunk for event in output)
        assert "absent" in "".join(event.chunk for event in output)
        assert isinstance(terminal, CommandCompleted)
        assert terminal.exit_code == 0
        assert terminal.output_truncated is False

        bounded = await collect_command(
            sandbox,
            CommandSpec(
                argv=(sys.executable, "-c", "print('🙂' * 1000)"),
                timeout_seconds=2,
                max_output_bytes=9,
            ),
        )
        bounded_output = b"".join(
            event.chunk.encode("utf-8") for event in bounded if isinstance(event, CommandOutput)
        )
        assert len(bounded_output) <= 9
        assert isinstance(bounded[-1], CommandCompleted)
        assert bounded[-1].output_truncated is True
    finally:
        await sandbox.destroy()


async def test_local_sandbox_enforces_timeout_and_command_tool_outcomes(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-timeout",
    )
    sandbox = LocalSandbox(
        workspace,
        unsafe_allow_host_execution=True,
        runtime_environment="test",
    )
    try:
        timed = await collect_command(
            sandbox,
            CommandSpec(
                argv=(sys.executable, "-c", "import time; time.sleep(10)"),
                timeout_seconds=0.05,
                max_output_bytes=100,
            ),
        )
        assert isinstance(timed[-1], CommandCompleted)
        assert timed[-1].timed_out is True

        toolset = WorkspaceToolset(sandbox.workspace, sandbox=sandbox)
        registry = toolset.registry(include_command=True)
        context = ToolExecutionContext(
            run_id="10000000-0000-0000-0000-000000000001",
            tool_call_id="command-1",
            max_output_bytes=100,
            max_result_bytes=100,
        )
        prepared = registry.prepare(
            "run_command",
            FrozenJsonObject(
                {
                    "argv": [sys.executable, "-c", "print('ok')"],
                    "timeout_seconds": 1,
                }
            ),
        )
        events = [event async for event in prepared.stream(context)]
        assert any(isinstance(event, ToolOutputChunk) and "ok" in event.chunk for event in events)
        assert isinstance(events[-1], ToolExecutionCompleted)
        assert events[-1].result.to_json_object() == {"exit_code": 0}

        failed = registry.prepare(
            "run_command",
            FrozenJsonObject({"argv": [sys.executable, "-c", "raise SystemExit(7)"]}),
        )
        with pytest.raises(DomainOperationError) as nonzero:
            _ = [event async for event in failed.stream(context)]
        assert nonzero.value.code == "command_failed"
        assert nonzero.value.details["exit_code"] == 7
    finally:
        await sandbox.destroy()


def test_local_sandbox_rejects_reserved_environment_overrides(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-environment",
    )
    with pytest.raises(ValueError, match="may not override"):
        LocalSandbox(
            workspace,
            unsafe_allow_host_execution=True,
            runtime_environment="test",
            trusted_environment={"HOME": str(tmp_path / "unsafe")},
        )

    asyncio.run(workspace.destroy())


def test_local_sandbox_is_prohibited_in_production(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-production",
    )
    with pytest.raises(ValueError, match="prohibited in production"):
        LocalSandbox(
            workspace,
            unsafe_allow_host_execution=True,
            runtime_environment="production",
        )
    asyncio.run(workspace.destroy())
