import asyncio
import os
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from agent_core.domain import DomainOperationError, FrozenJsonObject
from agent_core.sandbox import CommandCompleted, CommandOutput, CommandSpec, WorkspaceSnapshot
from agent_core.tools import ToolExecutionContext, ToolOutputChannel
from sandbox_runtime import PodmanSandbox, PodmanSandboxConfig, WorkspaceToolset
from sandbox_runtime._process import BoundedProcessRunner, ProcessChunk, ProcessResult

if TYPE_CHECKING:
    from sandbox_runtime.git_workspace import GitWorktreeWorkspace
    from sandbox_runtime.workspace import RootedWorkspace

SANDBOX_ID = UUID("10000000-0000-0000-0000-000000000001")


class _FakeWorkspace:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.destroyed = False
        self.revision = "a" * 40

    def resolve_path(self, path: str) -> Path:
        candidate = (self.root / path).resolve()
        if not candidate.is_relative_to(self.root):
            raise DomainOperationError(
                code="workspace_path_escape",
                message="path escaped",
            )
        return candidate

    def file_bytes(self, path: str) -> bytes:
        return self.resolve_path(path).read_bytes()

    def write_file_atomic(self, path: str, content: bytes) -> None:
        self.resolve_path(path).write_bytes(content)

    def create_snapshot(self, *, label: str) -> WorkspaceSnapshot:
        assert label
        return WorkspaceSnapshot(
            id=f"snapshot-{self.revision[:16]}",
            uri=f"git-worktree://local/{self.revision}",
            revision=self.revision,
        )

    def restore_revision(self, revision: str) -> None:
        self.revision = revision

    async def destroy(self) -> None:
        self.destroyed = True


class _FakePodmanRunner:
    def __init__(
        self,
        *,
        rootless: bool = True,
        run_exit_code: int = 0,
        remove_failures: int = 0,
    ) -> None:
        self.rootless = rootless
        self.run_exit_code = run_exit_code
        self.remove_failures = remove_failures
        self.calls: list[tuple[str, ...]] = []
        self.environments: list[dict[str, str] | None] = []
        self.closed = False

    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
        on_chunk: Callable[[ProcessChunk], Awaitable[None]] | None = None,
        retain_output: bool = True,
    ) -> ProcessResult:
        del cwd, timeout_seconds, max_output_bytes
        call = tuple(argv)
        self.calls.append(call)
        self.environments.append(dict(environment) if environment is not None else None)
        if call[1:4] == ("info", "--format", "json"):
            chunk = ProcessChunk(
                channel=ToolOutputChannel.STDOUT,
                text=(f'{{"host":{{"security":{{"rootless":{str(self.rootless).lower()}}}}}}}'),
            )
            return ProcessResult(chunks=(chunk,), exit_code=0)
        if call[1:3] == ("image", "inspect"):
            return ProcessResult(
                chunks=(
                    ProcessChunk(
                        channel=ToolOutputChannel.STDOUT,
                        text="sha256:" + "a" * 64 + "\n",
                    ),
                ),
                exit_code=0,
            )
        if call[1] == "rm":
            if self.remove_failures:
                self.remove_failures -= 1
                return ProcessResult(chunks=(), exit_code=1)
            return ProcessResult(chunks=(), exit_code=0)
        if call[1] == "run":
            chunks = (
                ProcessChunk(channel=ToolOutputChannel.STDOUT, text="output"),
                ProcessChunk(channel=ToolOutputChannel.STDERR, text="warning"),
            )
            if on_chunk is not None:
                for chunk in chunks:
                    await on_chunk(chunk)
            return ProcessResult(
                chunks=chunks if retain_output else (),
                exit_code=self.run_exit_code,
            )
        raise AssertionError(f"unexpected Podman invocation: {call!r}")

    async def close(self) -> None:
        self.closed = True


class _BlockingPodmanRunner(_FakePodmanRunner):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = False

    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
        on_chunk: Callable[[ProcessChunk], Awaitable[None]] | None = None,
        retain_output: bool = True,
    ) -> ProcessResult:
        if len(argv) > 1 and argv[1] == "run":
            self.calls.append(tuple(argv))
            self.environments.append(dict(environment) if environment is not None else None)
            self.started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        return await super().run(
            argv,
            cwd=cwd,
            timeout_seconds=timeout_seconds,
            max_output_bytes=max_output_bytes,
            environment=environment,
            on_chunk=on_chunk,
            retain_output=retain_output,
        )


def _config(**updates: object) -> PodmanSandboxConfig:
    values: dict[str, object] = {
        "image": "agent-platform-sandbox:test",
        "environment": "test",
        "podman_executable": sys.executable,
    }
    values.update(updates)
    return PodmanSandboxConfig.model_validate(values)


async def _create(
    tmp_path: Path,
    runner: _FakePodmanRunner,
    *,
    config: PodmanSandboxConfig | None = None,
) -> tuple[PodmanSandbox, _FakeWorkspace]:
    workspace = _FakeWorkspace(tmp_path)
    sandbox = await PodmanSandbox.create(
        cast("GitWorktreeWorkspace", workspace),
        config=config or _config(),
        runner=cast("BoundedProcessRunner", runner),
        control_environment={
            "HOME": os.fspath(tmp_path),
            "PATH": "/usr/bin:/bin",
            "PROVIDER_API_KEY": "must-not-propagate",
        },
        id_factory=lambda: SANDBOX_ID,
    )
    return sandbox, workspace


async def _collect(
    sandbox: PodmanSandbox,
    command: CommandSpec,
) -> list[CommandOutput | CommandCompleted]:
    return [event async for event in sandbox.execute(command)]


async def test_podman_sandbox_builds_a_fail_closed_command_boundary(
    tmp_path: Path,
) -> None:
    runner = _FakePodmanRunner()
    sandbox, workspace = await _create(tmp_path, runner)
    try:
        events = await _collect(
            sandbox,
            CommandSpec(
                argv=("python", "-c", "print('bounded')"),
                cwd=".",
                timeout_seconds=5.1,
                max_output_bytes=1024,
            ),
        )
    finally:
        await sandbox.destroy()

    output = [event for event in events if isinstance(event, CommandOutput)]
    assert [event.chunk for event in output] == ["output", "warning"]
    assert isinstance(events[-1], CommandCompleted)
    assert events[-1].exit_code == 0
    run = next(call for call in runner.calls if call[1] == "run")
    assert "--read-only" in run
    assert run[run.index("--network") + 1] == "none"
    assert "--http-proxy=false" in run
    assert "--image-volume=ignore" in run
    assert "--unsetenv-all" in run
    assert run[run.index("--cap-drop=ALL")] == "--cap-drop=ALL"
    assert run[run.index("--security-opt") + 1] == "no-new-privileges"
    assert run[run.index("--seccomp-policy") + 1] == "default"
    assert run[run.index("--pid") + 1] == "private"
    assert run[run.index("--cgroupns") + 1] == "private"
    assert run[run.index("--pids-limit") + 1] == "256"
    assert run[run.index("--memory") + 1] == str(512 * 1024 * 1024)
    assert run[run.index("--memory-swap") + 1] == str(512 * 1024 * 1024)
    assert run[run.index("--cpus") + 1] == "1"
    assert run[run.index("--user") + 1] == "10001:10001"
    assert run[run.index("--userns") + 1] == "keep-id:uid=10001,gid=10001"
    assert "--privileged" not in run
    assert "--no-healthcheck" in run
    assert "--systemd=false" in run
    assert "--log-driver=none" in run
    assert "--restart=no" in run
    assert run[run.index("--timeout") + 1] == "6"
    assert all("seccomp=unconfined" not in argument for argument in run)
    mounts = [run[index + 1] for index, value in enumerate(run) if value == "--mount"]
    assert mounts == [f"type=bind,source={tmp_path},destination=/workspace,rw,nodev,nosuid"]
    container_environment = [run[index + 1] for index, value in enumerate(run) if value == "--env"]
    assert set(container_environment) == {
        "HOME=/tmp",
        "LANG=C.UTF-8",
        "LC_ALL=C.UTF-8",
        "PATH=/usr/local/bin:/usr/bin:/bin",
    }
    assert all("PROVIDER_API_KEY" not in argument for argument in run)
    assert runner.environments[0] == {
        "HOME": os.fspath(tmp_path),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }
    assert any(call[1] == "rm" and "--force" in call for call in runner.calls)
    assert workspace.destroyed is True


async def test_podman_sandbox_requires_a_rootless_engine(tmp_path: Path) -> None:
    runner = _FakePodmanRunner(rootless=False)
    with pytest.raises(DomainOperationError) as rootless:
        await _create(tmp_path, runner)
    assert rootless.value.code == "sandbox_rootless_required"
    assert not any(call[1:3] == ("image", "inspect") for call in runner.calls)


async def test_podman_command_runtime_failure_is_structured(tmp_path: Path) -> None:
    runner = _FakePodmanRunner(run_exit_code=125)
    sandbox, _ = await _create(tmp_path, runner)
    try:
        with pytest.raises(DomainOperationError) as failed:
            await _collect(
                sandbox,
                CommandSpec(
                    argv=("python", "-c", "print('never')"),
                    timeout_seconds=1,
                    max_output_bytes=100,
                ),
            )
        assert failed.value.code == "sandbox_command_start_failed"
        assert failed.value.retryable is True
    finally:
        await sandbox.destroy()


async def test_podman_sandbox_composes_with_the_typed_command_tool(
    tmp_path: Path,
) -> None:
    runner = _FakePodmanRunner(run_exit_code=7)
    sandbox, workspace = await _create(tmp_path, runner)
    registry = WorkspaceToolset(
        cast("RootedWorkspace", workspace),
        sandbox=sandbox,
    ).registry(include_command=True)
    prepared = registry.prepare(
        "run_command",
        FrozenJsonObject({"argv": ["python", "-c", "raise SystemExit(7)"]}),
    )
    context = ToolExecutionContext(
        run_id=UUID("20000000-0000-0000-0000-000000000001"),
        tool_call_id="podman-command-1",
        max_output_bytes=1024,
        max_result_bytes=1024,
    )
    try:
        with pytest.raises(DomainOperationError) as failed:
            _ = [event async for event in prepared.stream(context)]
        assert failed.value.code == "command_failed"
        assert failed.value.details["exit_code"] == 7
    finally:
        await sandbox.destroy()


async def test_podman_destroy_retries_a_failed_targeted_container_removal(
    tmp_path: Path,
) -> None:
    runner = _FakePodmanRunner(remove_failures=1)
    sandbox, workspace = await _create(tmp_path, runner)
    with pytest.raises(DomainOperationError) as cleanup:
        await _collect(
            sandbox,
            CommandSpec(
                argv=("python", "-c", "print('completed')"),
                timeout_seconds=1,
                max_output_bytes=100,
            ),
        )
    assert cleanup.value.code == "sandbox_cleanup_failed"
    assert cleanup.value.retryable is True
    assert sandbox._pending_container_cleanup

    await sandbox.destroy()

    removals = [call for call in runner.calls if call[1] == "rm"]
    assert len(removals) == 2
    assert sandbox._pending_container_cleanup == set()
    assert workspace.destroyed is True


async def test_podman_cancellation_waits_for_targeted_container_removal(
    tmp_path: Path,
) -> None:
    runner = _BlockingPodmanRunner()
    sandbox, _ = await _create(tmp_path, runner)
    execution = asyncio.create_task(
        _collect(
            sandbox,
            CommandSpec(
                argv=("python", "-c", "import time; time.sleep(10)"),
                timeout_seconds=20,
                max_output_bytes=100,
            ),
        )
    )
    try:
        await runner.started.wait()
        await sandbox.cancel_active()
        result = await asyncio.gather(execution, return_exceptions=True)
        assert isinstance(result[0], asyncio.CancelledError)
        assert runner.cancelled is True
        assert any(call[1] == "rm" and "--ignore" in call for call in runner.calls)
    finally:
        execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)
        await sandbox.destroy()


def test_podman_config_is_closed_and_requires_production_digest() -> None:
    with pytest.raises(ValidationError, match="pinned"):
        PodmanSandboxConfig(image="agent-platform-sandbox:latest")
    pinned = PodmanSandboxConfig(
        image="registry.invalid/sandbox@sha256:" + "a" * 64,
    )
    assert pinned.environment == "production"

    with pytest.raises(ValidationError, match="extra"):
        PodmanSandboxConfig.model_validate(
            {
                "image": "agent-platform-sandbox:test",
                "environment": "test",
                "privileged": True,
            }
        )
    with pytest.raises(ValidationError, match="canonical"):
        _config(image="--privileged")


async def test_podman_sandbox_rejects_ambiguous_mount_path(tmp_path: Path) -> None:
    workspace = _FakeWorkspace(tmp_path / "comma,path")
    workspace.root.mkdir()
    with pytest.raises(ValueError, match="mount separator"):
        await PodmanSandbox.create(
            cast("GitWorktreeWorkspace", workspace),
            config=_config(),
            runner=cast("BoundedProcessRunner", _FakePodmanRunner()),
        )
