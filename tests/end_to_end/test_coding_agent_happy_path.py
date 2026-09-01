from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from agent_core.domain.status import ToolCallStatus
from agent_core.events import (
    CheckpointCreatedEvent,
    ModelToolCallReceivedEvent,
    RunCompletedEvent,
    ToolCompletedEvent,
    ToolStderrEvent,
    ToolStdoutEvent,
)
from agent_core.fakes import SequentialIdGenerator, SteppingClock
from agent_core.gateway import GatewayMessage, MessageRole
from agent_core.loop import AgentLoop, AgentLoopInput
from agent_core.sandbox import CommandCompleted, CommandOutput, CommandSpec
from agent_core.settings import PlatformSettings
from agents_sdk_adapter import create_openai_compatible_agents_gateway
from gateway_client import GatewayClient
from platform_telemetry import Redactor
from sandbox_runtime import (
    GitWorktreeManager,
    InMemoryCheckpointCoordinator,
    PodmanSandbox,
    PodmanSandboxConfig,
    WorkspaceToolset,
)

pytestmark = [
    pytest.mark.end_to_end,
    pytest.mark.skipif(
        os.getenv("AGENT_PLATFORM_RUN_CODING_E2E") != "1",
        reason="set AGENT_PLATFORM_RUN_CODING_E2E=1 to run the Podman E2E test",
    ),
]

_FIXTURE = Path(__file__).parent / "fixtures" / "calculator_bug"
_TASK = (
    "[fixture:calculator-bug-v1] Fix the implementation of add so the repository tests pass. "
    "Do not modify the test file. Run python -m unittest -v to verify the fix."
)
_CANARY_SECRET = "sk-" + "e2e-canary-secret"
_GIT = shutil.which("git") or "/usr/bin/git"
_TENANT_ID = UUID("00000000-0000-0000-0000-000000000010")
_SESSION_ID = UUID("00000000-0000-0000-0000-000000000020")


@dataclass(frozen=True, slots=True)
class _RunEvidence:
    patch: bytes
    event_json: str


def _git(repository: Path, *arguments: str, input_bytes: bytes | None = None) -> str:
    result = subprocess.run(  # noqa: S603 - resolved Git only, never a shell
        (_GIT, "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        input=input_bytes,
    )
    return result.stdout.decode("utf-8", errors="strict").strip()


def _create_source_repository(path: Path) -> None:
    shutil.copytree(_FIXTURE, path)
    _git(path, "init", "-q")
    _git(path, "config", "user.name", "E2E Test")
    _git(path, "config", "user.email", "e2e@example.invalid")
    _git(path, "add", ".gitignore", "calculator.py", "test_calculator.py")
    _git(path, "commit", "-q", "-m", "broken calculator fixture")


async def _sandbox_command(
    sandbox: PodmanSandbox,
    *argv: str,
) -> tuple[CommandCompleted, str]:
    terminal: CommandCompleted | None = None
    output: list[str] = []
    async for event in sandbox.execute(
        CommandSpec(
            argv=argv,
            timeout_seconds=30,
            max_output_bytes=256 * 1024,
        )
    ):
        if isinstance(event, CommandOutput):
            output.append(event.chunk)
        else:
            terminal = event
    assert terminal is not None
    return terminal, "".join(output)


async def _run_agent_once(  # noqa: PLR0915 - one complete E2E evidence boundary
    *,
    source: Path,
    worktree_parent: Path,
    run_id: UUID,
    settings: PlatformSettings,
    sandbox_image: str,
) -> _RunEvidence:
    source_head = _git(source, "rev-parse", "HEAD")
    source_status = _git(source, "status", "--porcelain=v2", "--branch")
    original_calculator = (source / "calculator.py").read_bytes()
    original_test = (source / "test_calculator.py").read_bytes()

    workspace = GitWorktreeManager(worktree_parent=worktree_parent).create(
        source,
        run_id=str(run_id),
    )
    sandbox: PodmanSandbox | None = None
    adapter = create_openai_compatible_agents_gateway(settings)
    gateway = GatewayClient(adapter, close=adapter.aclose)
    try:
        sandbox = await PodmanSandbox.create(
            workspace,
            config=PodmanSandboxConfig(
                image=sandbox_image,
                environment="test",
            ),
        )
        initial_terminal, initial_output = await _sandbox_command(
            sandbox,
            "python",
            "-m",
            "unittest",
            "-v",
        )
        assert initial_terminal.exit_code != 0
        assert "FAILED" in initial_output

        checkpoints = InMemoryCheckpointCoordinator(
            run_id=run_id,
            session_id=_SESSION_ID,
            workspace=workspace,
            clock=SteppingClock(datetime(2026, 8, 19, 12, tzinfo=UTC)),
            cancel_active=sandbox.cancel_active,
        )
        tools = WorkspaceToolset(workspace, sandbox=sandbox).registry(
            include_edit=True,
            include_command=True,
        )
        loop = AgentLoop(
            gateway=gateway,
            tools=tools,
            clock=SteppingClock(datetime(2026, 8, 19, 12, tzinfo=UTC)),
            id_generator=SequentialIdGenerator(),
            checkpoints=checkpoints,
            redactor=Redactor((*settings.redaction_values(), _CANARY_SECRET)),
        )
        events = [
            event
            async for event in loop.run(
                AgentLoopInput(
                    tenant_id=_TENANT_ID,
                    session_id=_SESSION_ID,
                    run_id=run_id,
                    attempt=1,
                    worker_id="e2e-worker",
                    route_name="coding-default",
                    messages=(
                        GatewayMessage(
                            role=MessageRole.SYSTEM,
                            content="Use repository tools and verify changes in the sandbox.",
                        ),
                        GatewayMessage(
                            role=MessageRole.USER,
                            content=f"{_TASK} Redaction canary: {_CANARY_SECRET}",
                        ),
                    ),
                    task_plan={"steps": [{"title": "fix and verify", "done": False}]},
                )
            )
        ]

        assert isinstance(events[-1], RunCompletedEvent)
        assert events[-1].payload.final_text == (
            "Fixed calculator.add and verified the repository test suite in the sandbox."
        )
        calls = [
            event.payload.tool_call_id
            for event in events
            if isinstance(event, ModelToolCallReceivedEvent)
        ]
        assert calls == [
            "e2e-read-calculator",
            "e2e-read-test",
            "e2e-edit-calculator",
            "e2e-run-tests",
        ]
        completions = [event for event in events if isinstance(event, ToolCompletedEvent)]
        assert len(completions) == 4
        assert all(event.payload.status is ToolCallStatus.COMPLETED for event in completions)
        assert len([event for event in events if isinstance(event, CheckpointCreatedEvent)]) == 2
        command_output = "".join(
            event.payload.chunk
            for event in events
            if isinstance(event, (ToolStdoutEvent, ToolStderrEvent))
        )
        assert "OK" in command_output
        assert [event.sequence for event in events] == list(range(1, len(events) + 1))

        final_terminal, final_output = await _sandbox_command(
            sandbox,
            "python",
            "-m",
            "unittest",
            "-v",
        )
        assert final_terminal.exit_code == 0
        assert "OK" in final_output
        assert workspace.file_bytes("calculator.py").endswith(b"return left + right\n")
        assert workspace.file_bytes("test_calculator.py") == original_test
        patch = workspace.final_patch()
        event_json = "\n".join(event.model_dump_json() for event in events)
        assert _CANARY_SECRET not in event_json
        assert _CANARY_SECRET.encode() not in patch
    finally:
        try:
            await gateway.aclose()
        finally:
            if sandbox is None:
                await workspace.destroy()
            else:
                await sandbox.destroy()

    assert _git(source, "rev-parse", "HEAD") == source_head
    assert _git(source, "status", "--porcelain=v2", "--branch") == source_status
    assert (source / "calculator.py").read_bytes() == original_calculator
    assert (source / "test_calculator.py").read_bytes() == original_test
    return _RunEvidence(patch=patch, event_json=event_json)


@pytest.mark.asyncio
async def test_coding_agent_happy_path_through_litellm_and_podman(tmp_path: Path) -> None:
    source = tmp_path / "source"
    worktrees = tmp_path / "worktrees"
    worktrees.mkdir()
    _create_source_repository(source)
    settings = PlatformSettings()
    sandbox_image = os.getenv(
        "AGENT_PLATFORM_SANDBOX_IMAGE",
        "localhost/agent-platform-sandbox:sequence-10",
    )

    first = await _run_agent_once(
        source=source,
        worktree_parent=worktrees,
        run_id=UUID("30000000-0000-0000-0000-000000000001"),
        settings=settings,
        sandbox_image=sandbox_image,
    )
    assert tuple(worktrees.iterdir()) == ()
    second = await _run_agent_once(
        source=source,
        worktree_parent=worktrees,
        run_id=UUID("30000000-0000-0000-0000-000000000002"),
        settings=settings,
        sandbox_image=sandbox_image,
    )
    assert tuple(worktrees.iterdir()) == ()

    assert first.patch == second.patch
    assert first.event_json != second.event_json
    applied = tmp_path / "applied"
    _git(tmp_path, "clone", "-q", str(source), str(applied))
    _git(applied, "apply", "--check", "-", input_bytes=first.patch)
    _git(applied, "apply", "-", input_bytes=first.patch)
    assert _git(applied, "diff", "--name-only") == "calculator.py"
    assert (applied / "calculator.py").read_bytes().endswith(b"return left + right\n")
    assert (applied / "test_calculator.py").read_bytes() == (
        source / "test_calculator.py"
    ).read_bytes()
