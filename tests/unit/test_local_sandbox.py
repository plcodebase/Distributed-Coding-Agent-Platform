import asyncio
import shutil
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

from agent_core.domain import DomainOperationError, FrozenJsonObject
from agent_core.sandbox import (
    CommandCompleted,
    CommandOutput,
    CommandSpec,
    WorkspaceSnapshot,
)
from agent_core.tools import (
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolOutputChannel,
    ToolOutputChunk,
)
from sandbox_runtime import GitWorktreeManager, LocalSandbox, WorkspaceToolset
from sandbox_runtime._process import BoundedProcessRunner, ProcessChunk

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


def test_local_sandbox_rejects_unknown_runtime_labels(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-runtime-label",
    )
    with pytest.raises(ValueError, match="not recognized"):
        LocalSandbox(
            workspace,
            unsafe_allow_host_execution=True,
            runtime_environment=cast("object", "staging"),  # type: ignore[arg-type]
        )
    asyncio.run(workspace.destroy())


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"max_concurrent_commands": 0}, "max_concurrent_commands"),
        ({"max_concurrent_commands": 65}, "max_concurrent_commands"),
        ({"max_write_bytes": -1}, "max_write_bytes"),
        ({"max_snapshots": 0}, "max_snapshots"),
        ({"executable_path": "relative"}, "executable_path"),
        ({"trusted_environment": {"LD_PRELOAD": "unsafe"}}, "may not override"),
        ({"trusted_environment": {"BAD-NAME": "value"}}, "invalid variable name"),
        ({"trusted_environment": {"OK": "x" * (32 * 1024 + 1)}}, "invalid variable value"),
    ],
)
def test_local_sandbox_validates_constructor_limits(
    tmp_path: Path,
    options: dict[str, object],
    message: str,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-invalid-constructor",
    )
    try:
        with pytest.raises(ValueError, match=message):
            LocalSandbox(workspace, **options)  # type: ignore[arg-type]
    finally:
        asyncio.run(workspace.destroy())


async def test_local_sandbox_bounds_writes_and_owns_snapshots(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-snapshots",
    )
    sandbox = LocalSandbox(
        workspace,
        max_write_bytes=4,
        max_snapshots=1,
    )
    try:
        with pytest.raises(DomainOperationError) as write_limit:
            await sandbox.write_file("tracked.txt", b"large")
        assert write_limit.value.code == "sandbox_write_limit"
        assert await sandbox.read_file("tracked.txt") == b"base\n"

        snapshot = await sandbox.create_snapshot()
        forged = snapshot.model_copy(update={"revision": "f" * 40})
        with pytest.raises(DomainOperationError) as identity:
            await sandbox.restore_snapshot(forged)
        assert identity.value.code == "snapshot_identity_mismatch"

        foreign = WorkspaceSnapshot(
            id="snapshot-foreign",
            uri="git-worktree://local/foreign",
            revision="f" * 40,
        )
        with pytest.raises(DomainOperationError) as missing:
            await sandbox.restore_snapshot(foreign)
        assert missing.value.code == "snapshot_not_found"

        await sandbox.write_file("tracked.txt", b"new\n")
        with pytest.raises(DomainOperationError) as snapshot_limit:
            await sandbox.create_snapshot()
        assert snapshot_limit.value.code == "snapshot_limit"
    finally:
        await sandbox.destroy()


async def test_local_sandbox_cancel_invalidates_queued_commands(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-cancel-queue",
    )
    sandbox = LocalSandbox(
        workspace,
        unsafe_allow_host_execution=True,
        runtime_environment="test",
        max_concurrent_commands=1,
    )
    command = CommandSpec(
        argv=(sys.executable, "-c", "import time; time.sleep(10)"),
        timeout_seconds=20,
        max_output_bytes=100,
    )
    first = asyncio.create_task(collect_command(sandbox, command))
    try:
        for _ in range(100):
            if sandbox._active_tasks:
                break
            await asyncio.sleep(0.01)
        assert sandbox._active_tasks
        second = asyncio.create_task(collect_command(sandbox, command))
        await asyncio.sleep(0)

        await sandbox.cancel_active()
        first_result, second_result = await asyncio.gather(
            first,
            second,
            return_exceptions=True,
        )

        assert isinstance(first_result, asyncio.CancelledError)
        assert isinstance(second_result, DomainOperationError)
        assert second_result.code == "command_cancelled"
    finally:
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        await sandbox.destroy()


async def test_local_sandbox_destroy_is_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="local-destroy-retry",
    )
    sandbox = LocalSandbox(workspace)
    original_destroy = workspace.destroy
    failures = 1

    async def fail_once() -> None:
        nonlocal failures
        if failures:
            failures -= 1
            raise DomainOperationError(
                code="workspace_cleanup_failed",
                message="simulated",
            )
        await original_destroy()

    monkeypatch.setattr(workspace, "destroy", fail_once)
    with pytest.raises(DomainOperationError) as cleanup:
        await sandbox.destroy()
    assert cleanup.value.code == "sandbox_cleanup_failed"
    assert cleanup.value.details["retryable"] is True
    with pytest.raises(DomainOperationError) as unavailable:
        await sandbox.read_file("tracked.txt")
    assert unavailable.value.code == "sandbox_cleanup_required"

    await sandbox.destroy()
    await sandbox.destroy()
    assert not sandbox._home.exists()


@pytest.mark.parametrize(
    ("grace",),
    [(0,), (-1,), (float("nan"),), (61,)],
)
def test_process_runner_rejects_invalid_grace_period(grace: float) -> None:
    with pytest.raises(ValueError):
        BoundedProcessRunner(terminate_grace_seconds=grace)


async def test_process_runner_terminates_when_output_callback_fails(
    tmp_path: Path,
) -> None:
    runner = BoundedProcessRunner()
    marker = tmp_path / "callback-leak"

    async def fail_callback(chunk: ProcessChunk) -> None:
        assert chunk.channel is ToolOutputChannel.STDOUT
        raise RuntimeError("consumer failed")

    with pytest.raises(RuntimeError, match="consumer failed"):
        await runner.run(
            (
                sys.executable,
                "-c",
                (
                    "import pathlib,sys,time;"
                    "sys.stdout.write('started');sys.stdout.flush();"
                    "time.sleep(0.2);"
                    f"pathlib.Path({str(marker)!r}).write_text('leaked')"
                ),
            ),
            cwd=tmp_path,
            timeout_seconds=2,
            max_output_bytes=100,
            on_chunk=fail_callback,
        )
    await asyncio.sleep(0.3)
    assert not marker.exists()
    await runner.close()


async def test_process_runner_timeout_terminates_the_entire_process_group(
    tmp_path: Path,
) -> None:
    runner = BoundedProcessRunner()
    child_marker = tmp_path / "child-survived-timeout"
    child_source = (
        "import pathlib,time;"
        "time.sleep(0.7);"
        f"pathlib.Path({str(child_marker)!r}).write_text('leaked')"
    )
    parent_source = (
        "import subprocess,sys,time;"
        f"subprocess.Popen([sys.executable, '-c', {child_source!r}]);"
        "print('child-started', flush=True);"
        "time.sleep(10)"
    )

    result = await runner.run(
        (sys.executable, "-c", parent_source),
        cwd=tmp_path,
        timeout_seconds=0.2,
        max_output_bytes=1024,
    )

    assert result.timed_out is True
    await asyncio.sleep(0.8)
    assert not child_marker.exists()
    await runner.close()


async def test_process_runner_rejects_unbounded_requests_before_start(
    tmp_path: Path,
) -> None:
    runner = BoundedProcessRunner()
    with pytest.raises(DomainOperationError) as timeout:
        await runner.run(
            (sys.executable, "-c", "print('never')"),
            cwd=tmp_path,
            timeout_seconds=3601,
            max_output_bytes=100,
        )
    assert timeout.value.code == "command_invalid"

    with pytest.raises(DomainOperationError) as output:
        await runner.run(
            (sys.executable, "-c", "print('never')"),
            cwd=tmp_path,
            timeout_seconds=1,
            max_output_bytes=100 * 1024 * 1024 + 1,
        )
    assert output.value.code == "command_invalid"
    await runner.close()


async def test_process_runner_close_covers_process_start_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_start = asyncio.create_subprocess_exec
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed_start(*args: object, **kwargs: object) -> asyncio.subprocess.Process:
        entered.set()
        await release.wait()
        return await real_start(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed_start)
    runner = BoundedProcessRunner()
    running = asyncio.create_task(
        runner.run(
            (sys.executable, "-c", "import time; time.sleep(10)"),
            cwd=tmp_path,
            timeout_seconds=20,
            max_output_bytes=100,
        )
    )
    await entered.wait()
    closing = asyncio.create_task(runner.close())
    await asyncio.sleep(0)
    release.set()

    await closing
    result = await running
    assert result.exit_code != 0
    with pytest.raises(DomainOperationError) as closed:
        await runner.run(
            (sys.executable, "-c", "print('never')"),
            cwd=tmp_path,
            timeout_seconds=1,
            max_output_bytes=100,
        )
    assert closed.value.code == "command_runner_closed"
