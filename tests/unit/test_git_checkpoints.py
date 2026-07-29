import asyncio
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from agent_core.domain import DomainOperationError, FrozenJsonObject
from agent_core.fakes import SteppingClock
from agent_core.gateway import GatewayMessage, MessageRole
from sandbox_runtime import GitWorktreeManager, InMemoryCheckpointCoordinator

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
    (path / "tracked.txt").write_text("base\n", encoding="utf-8")
    (path / "other.txt").write_text("other\n", encoding="utf-8")
    git(path, "add", ".")
    git(path, "commit", "-q", "-m", "initial")


def source_state(path: Path) -> tuple[str, str]:
    return (
        git(path, "rev-parse", "HEAD"),
        git(path, "status", "--porcelain=v2", "--branch"),
    )


def test_worktree_captures_dirty_source_without_modifying_checkout(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    (source / "tracked.txt").write_text("staged\n", encoding="utf-8")
    git(source, "add", "tracked.txt")
    (source / "other.txt").write_text("unstaged\n", encoding="utf-8")
    (source / "untracked.txt").write_text("untracked\n", encoding="utf-8")
    before = source_state(source)

    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="run-dirty",
    )
    try:
        assert workspace.file_bytes("tracked.txt") == b"staged\n"
        assert workspace.file_bytes("other.txt") == b"unstaged\n"
        assert workspace.file_bytes("untracked.txt") == b"untracked\n"
        assert source_state(source) == before
        assert workspace.current_revision == workspace.baseline_revision
    finally:
        asyncio.run(workspace.destroy())

    assert source_state(source) == before
    assert not workspace.root.exists()


def test_workspace_snapshots_restore_and_final_patch_is_baseline_relative(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="run-snapshot",
    )
    try:
        snapshot = workspace.create_snapshot(label="before mutation")
        workspace.write_file_atomic("tracked.txt", b"changed\n")
        changed_revision = workspace.commit_state(label="change")
        assert changed_revision != snapshot.revision
        patch = workspace.final_patch()
        assert b"changed" in patch
        assert b"base" in patch

        workspace.restore_revision(snapshot.revision)
        assert workspace.file_bytes("tracked.txt") == b"base\n"
        assert workspace.current_revision == snapshot.revision

        workspace.write_file_atomic("other.txt", b"uncommitted final state\n")
        final_patch = workspace.final_patch()
        assert b"uncommitted final state" in final_patch
        assert git(workspace.root, "status", "--porcelain") == ""
    finally:
        asyncio.run(workspace.destroy())


def test_final_patch_is_incrementally_bounded(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(
        worktree_parent=tmp_path,
        max_patch_bytes=64,
    ).create(source, run_id="run-patch-limit")
    try:
        workspace.write_file_atomic("tracked.txt", b"x" * 1024)
        workspace.commit_state(label="large change")
        with pytest.raises(DomainOperationError) as limited:
            workspace.final_patch()
        assert limited.value.code == "final_patch_limit"
    finally:
        asyncio.run(workspace.destroy())


async def test_checkpoint_restores_workspace_and_conversation_state(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="run-checkpoint",
    )
    cancelled = 0

    async def cancel_active() -> None:
        nonlocal cancelled
        cancelled += 1

    coordinator = InMemoryCheckpointCoordinator(
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace=workspace,
        clock=SteppingClock(NOW),
        cancel_active=cancel_active,
    )
    messages = (GatewayMessage(role=MessageRole.USER, content="change the file"),)
    task_plan = FrozenJsonObject({"steps": [{"title": "edit", "done": False}]})
    try:
        checkpoint = await coordinator.create_before_tool(
            run_id=RUN_ID,
            tool_call_id="edit-1",
            messages=messages,
            task_plan=task_plan,
            context_summary="working context",
        )
        workspace.write_file_atomic("tracked.txt", b"changed\n")
        completed_revision = await coordinator.complete_tool(
            checkpoint,
            tool_call_id="edit-1",
        )
        assert completed_revision != checkpoint.workspace_revision

        state = await coordinator.rewind(checkpoint.id)
        assert state.messages == messages
        assert state.task_plan == task_plan
        assert state.context_summary == "working context"
        assert state.workspace_revision == checkpoint.workspace_revision
        assert workspace.file_bytes("tracked.txt") == b"base\n"
        assert cancelled == 1

        workspace.write_file_atomic("tracked.txt", b"failed mutation\n")
        await coordinator.rollback(checkpoint)
        assert workspace.file_bytes("tracked.txt") == b"base\n"
        assert cancelled == 2

        with pytest.raises(DomainOperationError) as missing:
            await coordinator.rewind(UUID("30000000-0000-0000-0000-000000000003"))
        assert missing.value.code == "checkpoint_not_found"
    finally:
        await workspace.destroy()


async def test_checkpoint_rejects_a_different_run(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="run-mismatch",
    )
    coordinator = InMemoryCheckpointCoordinator(
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace=workspace,
        clock=SteppingClock(NOW),
    )
    try:
        with pytest.raises(DomainOperationError) as mismatch:
            await coordinator.create_before_tool(
                run_id=UUID("40000000-0000-0000-0000-000000000004"),
                tool_call_id="edit-1",
                messages=(),
                task_plan=FrozenJsonObject({}),
                context_summary=None,
            )
        assert mismatch.value.code == "checkpoint_run_mismatch"
    finally:
        await workspace.destroy()
