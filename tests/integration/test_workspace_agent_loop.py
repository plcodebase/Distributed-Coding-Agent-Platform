import hashlib
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from agent_core.events import (
    CheckpointCreatedEvent,
    RunCompletedEvent,
    ToolCompletedEvent,
)
from agent_core.fakes import (
    ScriptedGatewayTurn,
    ScriptedModelGateway,
    SequentialIdGenerator,
    SteppingClock,
)
from agent_core.gateway import GatewayMessage, GatewayToolCall, MessageRole
from agent_core.loop import AgentLoop, AgentLoopInput
from sandbox_runtime import (
    GitWorktreeManager,
    InMemoryCheckpointCoordinator,
    LocalSandbox,
    WorkspaceToolset,
)

pytestmark = pytest.mark.integration

GIT = shutil.which("git") or "/usr/bin/git"
RUN_ID = UUID("10000000-0000-0000-0000-000000000001")
SESSION_ID = UUID("20000000-0000-0000-0000-000000000002")
NOW = datetime(2026, 7, 28, 12, tzinfo=UTC)


def git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(  # noqa: S603 - tests invoke a resolved Git executable
        (GIT, "-C", str(repository), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def create_repository(path: Path) -> None:
    path.mkdir()
    git(path, "init", "-q")
    git(path, "config", "user.name", "Test")
    git(path, "config", "user.email", "test@example.invalid")
    (path / "main.py").write_text("value = 1\n", encoding="utf-8")
    git(path, "add", ".")
    git(path, "commit", "-q", "-m", "initial")


async def test_fake_model_executes_read_edit_and_command_in_isolated_workspace(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    source_head = git(source, "rev-parse", "HEAD")
    source_status = git(source, "status", "--porcelain=v2", "--branch")
    original_hash = hashlib.sha256(b"value = 1\n").hexdigest()

    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="integration-tools",
    )
    sandbox = LocalSandbox(
        workspace,
        unsafe_allow_host_execution=True,
        runtime_environment="test",
    )
    checkpoints = InMemoryCheckpointCoordinator(
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace=workspace,
        clock=SteppingClock(NOW),
        cancel_active=sandbox.cancel_active,
    )
    tools = WorkspaceToolset(workspace, sandbox=sandbox).registry(
        include_edit=True,
        include_command=True,
    )
    gateway = ScriptedModelGateway(
        [
            ScriptedGatewayTurn.tool_calls(
                GatewayToolCall(
                    id="read-1",
                    name="read_file",
                    arguments={"path": "main.py"},
                )
            ),
            ScriptedGatewayTurn.tool_calls(
                GatewayToolCall(
                    id="edit-1",
                    name="edit_file",
                    arguments={
                        "path": "main.py",
                        "expected_sha256": original_hash,
                        "old_text": "value = 1",
                        "new_text": "value = 2",
                    },
                )
            ),
            ScriptedGatewayTurn.tool_calls(
                GatewayToolCall(
                    id="command-1",
                    name="run_command",
                    arguments={
                        "argv": [
                            sys.executable,
                            "-c",
                            (
                                "from pathlib import Path;"
                                "Path('command.txt').write_text('verified\\n');"
                                "print('verified')"
                            ),
                        ],
                        "timeout_seconds": 2,
                    },
                )
            ),
            ScriptedGatewayTurn.text("Workspace changes are ready for review."),
        ]
    )
    loop = AgentLoop(
        gateway=gateway,
        tools=tools,
        clock=SteppingClock(NOW),
        id_generator=SequentialIdGenerator(),
        checkpoints=checkpoints,
    )

    try:
        events = [
            event
            async for event in loop.run(
                AgentLoopInput(
                    run_id=RUN_ID,
                    attempt=1,
                    worker_id="worker-1",
                    route_name="coding-default",
                    messages=(
                        GatewayMessage(
                            role=MessageRole.USER,
                            content="Update the value and verify it.",
                        ),
                    ),
                    task_plan={"steps": [{"title": "update and verify", "done": False}]},
                )
            )
        ]

        assert isinstance(events[-1], RunCompletedEvent)
        assert events[-1].payload.final_text == "Workspace changes are ready for review."
        assert sum(isinstance(event, ToolCompletedEvent) for event in events) == 3
        assert sum(isinstance(event, CheckpointCreatedEvent) for event in events) == 2
        assert workspace.file_bytes("main.py") == b"value = 2\n"
        assert workspace.file_bytes("command.txt") == b"verified\n"
        patch = workspace.final_patch()
        assert b"value = 2" in patch
        assert b"command.txt" in patch

        assert git(source, "rev-parse", "HEAD") == source_head
        assert git(source, "status", "--porcelain=v2", "--branch") == source_status
        assert (source / "main.py").read_bytes() == b"value = 1\n"
        assert not (source / "command.txt").exists()
    finally:
        await sandbox.destroy()
