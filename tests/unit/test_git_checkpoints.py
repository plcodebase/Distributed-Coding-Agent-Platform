import asyncio
import os
import shutil
import stat
import subprocess
from collections.abc import Sequence, Set
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
    (source / "untracked.txt").chmod(0o755)
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
        assert stat.S_IMODE(workspace.root.stat().st_mode) == 0o700
        assert stat.S_IMODE(workspace.root.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE((workspace.root / "untracked.txt").stat().st_mode) == 0o755
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


def test_git_boundary_disables_repository_code_execution(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    attributes = source / ".gitattributes"
    attributes.write_text("tracked.txt diff=hostile\n", encoding="utf-8")
    git(source, "add", ".gitattributes")
    git(source, "commit", "-q", "-m", "attributes")

    marker = tmp_path / "executed"
    helper = tmp_path / "hostile"
    helper.write_text(
        f"#!/bin/sh\nprintf executed > {marker!s}\nexit 0\n",
        encoding="utf-8",
    )
    helper.chmod(0o755)
    hooks = tmp_path / "hooks"
    hooks.mkdir()
    (hooks / "pre-commit").write_text(
        f"#!/bin/sh\nprintf hook > {marker!s}\nexit 0\n",
        encoding="utf-8",
    )
    (hooks / "pre-commit").chmod(0o755)
    git(source, "config", "diff.external", os.fspath(helper))
    git(source, "config", "diff.hostile.textconv", os.fspath(helper))
    git(source, "config", "core.fsmonitor", os.fspath(helper))
    git(source, "config", "core.hooksPath", os.fspath(hooks))

    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="hostile-git-config",
    )
    try:
        workspace.write_file_atomic("tracked.txt", b"changed\n")
        patch = workspace.final_patch()
        assert b"changed" in patch
        assert not marker.exists()
    finally:
        asyncio.run(workspace.destroy())


@pytest.mark.parametrize(
    "filter_key",
    ["filter.hostile.clean", "filter.hostile.smudge", "filter.hostile.process"],
)
def test_git_boundary_rejects_external_filters(
    tmp_path: Path,
    filter_key: str,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    marker = tmp_path / "filter-executed"
    helper = tmp_path / "filter"
    helper.write_text(
        f"#!/bin/sh\nprintf filter > {marker!s}\ncat\n",
        encoding="utf-8",
    )
    helper.chmod(0o755)
    git(source, "config", filter_key, os.fspath(helper))

    with pytest.raises(DomainOperationError) as unsupported:
        GitWorktreeManager(worktree_parent=tmp_path).create(
            source,
            run_id="external-filter",
        )
    assert unsupported.value.code == "workspace_external_filter_unsupported"
    assert not marker.exists()


def test_untracked_symlinks_are_rejected_without_entering_the_worktree(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    outside = tmp_path / "outside"
    outside.write_text("secret", encoding="utf-8")
    (source / "untracked-link").symlink_to(outside)

    with pytest.raises(DomainOperationError) as rejected:
        GitWorktreeManager(worktree_parent=tmp_path).create(
            source,
            run_id="untracked-link",
        )
    assert rejected.value.code == "workspace_untracked_type"
    assert not tuple(tmp_path.glob("agent-run-*"))


def test_untracked_source_swap_to_symlink_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    candidate = source / "swap.txt"
    candidate.write_text("initial", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.write_text("outside", encoding="utf-8")
    real_open = os.open
    swapped = False

    def swap_before_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        if path == "swap.txt" and dir_fd is not None and not swapped:
            swapped = True
            candidate.unlink()
            candidate.symlink_to(outside)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", swap_before_open)
    with pytest.raises(DomainOperationError) as changed:
        GitWorktreeManager(worktree_parent=tmp_path).create(
            source,
            run_id="untracked-swap",
        )
    assert changed.value.code == "source_repository_changed"
    assert outside.read_text(encoding="utf-8") == "outside"
    assert not tuple(tmp_path.glob("agent-run-*"))


@pytest.mark.parametrize(
    ("manager_options", "expected_code"),
    [
        ({"max_untracked_files": 1}, "workspace_untracked_limit"),
        ({"max_untracked_file_bytes": 3}, "workspace_untracked_file_limit"),
        ({"max_untracked_bytes": 7}, "workspace_untracked_limit"),
    ],
)
def test_untracked_file_count_and_streamed_byte_limits(
    tmp_path: Path,
    manager_options: dict[str, int],
    expected_code: str,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    (source / "first.txt").write_bytes(b"first")
    (source / "second.txt").write_bytes(b"second")

    with pytest.raises(DomainOperationError) as limited:
        GitWorktreeManager(
            worktree_parent=tmp_path,
            **manager_options,  # type: ignore[arg-type]
        ).create(source, run_id="untracked-limits")
    assert limited.value.code == expected_code
    assert not tuple(tmp_path.glob("agent-run-*"))


def test_source_content_fingerprints_detect_changes_during_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    (source / "untracked.txt").write_text("initial\n", encoding="utf-8")
    manager = GitWorktreeManager(worktree_parent=tmp_path)
    original_copy = manager._copy_manifest

    def copy_then_mutate(*args: object, **kwargs: object) -> None:
        original_copy(*args, **kwargs)  # type: ignore[arg-type]
        (source / "tracked.txt").write_text("changed during capture\n", encoding="utf-8")
        (source / "untracked.txt").write_text("also changed\n", encoding="utf-8")

    monkeypatch.setattr(manager, "_copy_manifest", copy_then_mutate)
    with pytest.raises(DomainOperationError) as changed:
        manager.create(source, run_id="changing-source")
    assert changed.value.code == "source_repository_changed"
    assert not tuple(tmp_path.glob("agent-run-*"))


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("max_untracked_files", 0),
        ("max_untracked_bytes", -1),
        ("max_untracked_file_bytes", 0),
        ("max_file_bytes", -1),
        ("max_patch_bytes", 0),
        ("git_output_limit_bytes", -1),
        ("git_timeout_seconds", float("nan")),
    ],
)
def test_worktree_manager_rejects_invalid_limits(
    tmp_path: Path,
    name: str,
    value: int | float,
) -> None:
    with pytest.raises(ValueError):
        GitWorktreeManager(worktree_parent=tmp_path, **{name: value})  # type: ignore[arg-type]


def test_run_ids_are_bounded_and_only_hashed_tokens_reach_paths(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    manager = GitWorktreeManager(worktree_parent=tmp_path)
    with pytest.raises(ValueError):
        manager.create(source, run_id="")
    with pytest.raises(ValueError):
        manager.create(source, run_id="x" * 1025)

    workspace = manager.create(source, run_id="../../outside\nseparator")
    try:
        assert workspace.root.is_relative_to(tmp_path)
        assert ".." not in workspace.root.parent.name
        assert "outside" not in workspace.root.parent.name
    finally:
        asyncio.run(workspace.destroy())


def test_repository_paths_preserve_significant_spaces(tmp_path: Path) -> None:
    source = tmp_path / " source with trailing space "
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="spaced-source",
    )
    try:
        assert workspace.file_bytes("tracked.txt") == b"base\n"
    finally:
        asyncio.run(workspace.destroy())


def test_early_private_allocation_failure_is_cleaned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    real_chmod = Path.chmod

    def fail_private_chmod(path: Path, mode: int, *, follow_symlinks: bool = True) -> None:
        if path.name.startswith("agent-run-"):
            raise OSError("simulated allocation permission failure")
        real_chmod(path, mode, follow_symlinks=follow_symlinks)

    monkeypatch.setattr(Path, "chmod", fail_private_chmod)
    with pytest.raises(OSError, match="allocation permission"):
        GitWorktreeManager(worktree_parent=tmp_path).create(
            source,
            run_id="allocation-failure",
        )
    assert not tuple(tmp_path.glob("agent-run-*"))


def test_empty_patch_for_differing_revisions_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="empty-commit",
    )
    try:
        git(workspace.root, "commit", "--allow-empty", "-q", "-m", "empty")
        with pytest.raises(DomainOperationError) as protocol:
            workspace.final_patch()
        assert protocol.value.code == "final_patch_protocol_error"
    finally:
        asyncio.run(workspace.destroy())


async def test_destroy_is_targeted_idempotent_and_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="retry-cleanup",
    )
    original_run = workspace._git_runner.run
    failures_remaining = 1

    def fail_first_remove(
        repository: Path,
        arguments: Sequence[str],
        *,
        text: bool = True,
        output_limit_bytes: int | None = None,
        allowed_exit_codes: Set[int] = frozenset({0}),
    ) -> str | bytes:
        nonlocal failures_remaining
        if tuple(arguments[:3]) == ("worktree", "remove", "--force") and failures_remaining:
            failures_remaining -= 1
            raise DomainOperationError(
                code="git_operation_failed",
                message="simulated targeted cleanup failure",
            )
        return original_run(
            repository,
            arguments,
            text=text,
            output_limit_bytes=output_limit_bytes,
            allowed_exit_codes=allowed_exit_codes,
        )

    monkeypatch.setattr(workspace._git_runner, "run", fail_first_remove)
    with pytest.raises(DomainOperationError) as retryable:
        await workspace.destroy()
    assert retryable.value.code == "workspace_cleanup_failed"
    assert retryable.value.details["retryable"] is True
    assert workspace.root.exists()

    await workspace.destroy()
    await workspace.destroy()
    assert not workspace.root.parent.exists()


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
