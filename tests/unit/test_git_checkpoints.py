import asyncio
import os
import shutil
import stat
import subprocess
import threading
from collections.abc import Sequence, Set
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest

from agent_core.domain import Checkpoint, DomainOperationError, FrozenJsonObject
from agent_core.fakes import SteppingClock
from agent_core.gateway import GatewayMessage, MessageRole
from agent_core.sandbox import WorkspaceSnapshot
from sandbox_runtime import GitWorktreeManager, InMemoryCheckpointCoordinator

if TYPE_CHECKING:
    from sandbox_runtime.git_workspace import GitWorktreeWorkspace

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


def test_final_patch_captures_add_delete_binary_and_mode_changes(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="run-patch-shapes",
    )
    try:
        workspace.write_file_atomic(
            "added.txt",
            b"added\n",
            require_absent=True,
        )
        workspace.write_file_atomic(
            "binary.dat",
            b"\x00\x01changed\xff",
            require_absent=True,
        )
        (workspace.root / "tracked.txt").unlink()
        (workspace.root / "other.txt").chmod(0o755)

        patch = workspace.final_patch()

        assert b"new file mode 100644" in patch
        assert b"deleted file mode 100644" in patch
        assert b"GIT binary patch" in patch
        assert b"old mode 100644" in patch
        assert b"new mode 100755" in patch
    finally:
        asyncio.run(workspace.destroy())


def test_unchanged_workspace_has_an_empty_final_patch(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="run-empty-patch",
    )
    try:
        assert workspace.final_patch() == b""
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
    with pytest.raises(ValueError, match="valid UTF-8"):
        manager.create(source, run_id="\ud800")

    workspace = manager.create(source, run_id="../../outside\nseparator")
    try:
        assert workspace.root.is_relative_to(tmp_path)
        assert ".." not in workspace.root.parent.name
        assert "outside" not in workspace.root.parent.name
    finally:
        asyncio.run(workspace.destroy())


def test_snapshot_labels_reject_invalid_unicode(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="snapshot-label-validation",
    )
    try:
        with pytest.raises(ValueError, match="valid UTF-8"):
            workspace.commit_state(label="\ud800")
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


async def test_destroy_is_cancellation_safe_and_serializes_concurrent_callers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="cancelled-cleanup",
    )
    original_destroy = workspace._destroy_sync
    destroy_started = threading.Event()
    allow_destroy = threading.Event()

    def blocked_destroy() -> None:
        destroy_started.set()
        assert allow_destroy.wait(timeout=5)
        original_destroy()

    monkeypatch.setattr(workspace, "_destroy_sync", blocked_destroy)
    cancelled = asyncio.create_task(workspace.destroy())
    assert await asyncio.to_thread(destroy_started.wait, 2)
    concurrent = asyncio.create_task(workspace.destroy())
    cancelled.cancel()
    await asyncio.sleep(0)
    assert not cancelled.done()
    assert not concurrent.done()

    allow_destroy.set()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await concurrent
    await workspace.destroy()
    assert not workspace.root.parent.exists()


async def test_destroy_retries_owned_search_runner_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="runner-cleanup-retry",
    )
    original_close = workspace._search_runner.close
    failures_remaining = 1

    async def fail_first_close() -> None:
        nonlocal failures_remaining
        if failures_remaining:
            failures_remaining -= 1
            raise RuntimeError("simulated runner cleanup failure")
        await original_close()

    monkeypatch.setattr(workspace._search_runner, "close", fail_first_close)
    with pytest.raises(DomainOperationError) as retryable:
        await workspace.destroy()
    assert retryable.value.code == "workspace_cleanup_failed"
    assert retryable.value.details["retryable"] is True
    assert workspace.root.exists()

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


async def test_checkpoint_identity_and_tool_call_are_bound_to_stored_state(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="checkpoint-identity",
    )
    coordinator = InMemoryCheckpointCoordinator(
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace=workspace,
        clock=SteppingClock(NOW),
    )
    try:
        checkpoint = await coordinator.create_before_tool(
            run_id=RUN_ID,
            tool_call_id="edit-1",
            messages=(),
            task_plan=FrozenJsonObject({}),
            context_summary=None,
        )
        forged = checkpoint.model_copy(update={"workspace_revision": "forged"})

        with pytest.raises(DomainOperationError) as identity:
            await coordinator.rollback(forged)
        assert identity.value.code == "checkpoint_identity_mismatch"

        with pytest.raises(DomainOperationError) as tool_call:
            await coordinator.complete_tool(
                checkpoint,
                tool_call_id="edit-2",
            )
        assert tool_call.value.code == "checkpoint_tool_call_mismatch"
    finally:
        await workspace.destroy()


async def test_checkpoint_ids_are_unique_and_state_is_bounded(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="checkpoint-bounds",
    )
    fixed_id = UUID("30000000-0000-0000-0000-000000000003")
    coordinator = InMemoryCheckpointCoordinator(
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace=workspace,
        clock=SteppingClock(NOW),
        id_factory=lambda: fixed_id,
        max_checkpoints=2,
        max_messages=1,
        max_state_bytes=512,
    )
    try:
        await coordinator.create_before_tool(
            run_id=RUN_ID,
            tool_call_id="edit-1",
            messages=(),
            task_plan=FrozenJsonObject({}),
            context_summary=None,
        )
        with pytest.raises(DomainOperationError) as duplicate:
            await coordinator.create_before_tool(
                run_id=RUN_ID,
                tool_call_id="edit-2",
                messages=(),
                task_plan=FrozenJsonObject({}),
                context_summary=None,
            )
        assert duplicate.value.code == "checkpoint_id_conflict"

        messages = (
            GatewayMessage(role=MessageRole.USER, content="one"),
            GatewayMessage(role=MessageRole.USER, content="two"),
        )
        with pytest.raises(DomainOperationError) as message_limit:
            await coordinator.create_before_tool(
                run_id=RUN_ID,
                tool_call_id="edit-3",
                messages=messages,
                task_plan=FrozenJsonObject({}),
                context_summary=None,
            )
        assert message_limit.value.code == "checkpoint_state_limit"

        with pytest.raises(DomainOperationError) as byte_limit:
            await coordinator.create_before_tool(
                run_id=RUN_ID,
                tool_call_id="edit-4",
                messages=(),
                task_plan=FrozenJsonObject({}),
                context_summary="x" * 1024,
            )
        assert byte_limit.value.code == "checkpoint_state_limit"
    finally:
        await workspace.destroy()


async def test_rewind_truncates_the_later_checkpoint_branch(tmp_path: Path) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="checkpoint-branch",
    )
    checkpoint_ids = iter(
        (
            UUID("30000000-0000-0000-0000-000000000003"),
            UUID("40000000-0000-0000-0000-000000000004"),
        )
    )
    coordinator = InMemoryCheckpointCoordinator(
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace=workspace,
        clock=SteppingClock(NOW),
        id_factory=lambda: next(checkpoint_ids),
    )
    try:
        first = await coordinator.create_before_tool(
            run_id=RUN_ID,
            tool_call_id="edit-1",
            messages=(),
            task_plan=FrozenJsonObject({}),
            context_summary=None,
        )
        workspace.write_file_atomic("tracked.txt", b"first\n")
        await coordinator.complete_tool(first, tool_call_id="edit-1")
        second = await coordinator.create_before_tool(
            run_id=RUN_ID,
            tool_call_id="edit-2",
            messages=(),
            task_plan=FrozenJsonObject({}),
            context_summary=None,
        )
        workspace.write_file_atomic("tracked.txt", b"second\n")
        await coordinator.complete_tool(second, tool_call_id="edit-2")

        await coordinator.rewind(first.id)
        assert workspace.file_bytes("tracked.txt") == b"base\n"
        with pytest.raises(DomainOperationError) as removed:
            await coordinator.rewind(second.id)
        assert removed.value.code == "checkpoint_not_found"
    finally:
        await workspace.destroy()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("max_checkpoints", 0),
        ("max_messages", -1),
        ("max_state_bytes", 0),
    ],
)
def test_checkpoint_coordinator_rejects_invalid_limits(
    tmp_path: Path,
    name: str,
    value: int,
) -> None:
    source = tmp_path / "source"
    create_repository(source)
    workspace = GitWorktreeManager(worktree_parent=tmp_path).create(
        source,
        run_id="checkpoint-invalid-limits",
    )
    try:
        with pytest.raises(ValueError):
            InMemoryCheckpointCoordinator(
                run_id=RUN_ID,
                session_id=SESSION_ID,
                workspace=workspace,
                clock=SteppingClock(NOW),
                **{name: value},  # type: ignore[arg-type]
            )
    finally:
        asyncio.run(workspace.destroy())


class _BlockingCheckpointWorkspace:
    def __init__(self) -> None:
        self.commit_started = threading.Event()
        self.release_commit = threading.Event()
        self.restored: list[str] = []
        self.snapshot_calls = 0

    def create_snapshot(self, *, label: str) -> WorkspaceSnapshot:
        assert label
        self.snapshot_calls += 1
        return WorkspaceSnapshot(
            id="snapshot-before",
            uri="git-worktree://local/before",
            revision="revision-before",
        )

    def commit_state(self, *, label: str) -> str:
        assert label
        self.commit_started.set()
        if not self.release_commit.wait(timeout=5):
            raise RuntimeError("test did not release checkpoint commit")
        return "revision-after"

    def restore_revision(self, revision: str) -> None:
        self.restored.append(revision)


async def _fake_checkpoint(
    coordinator: InMemoryCheckpointCoordinator,
    *,
    tool_call_id: str = "edit-1",
) -> Checkpoint:
    return await coordinator.create_before_tool(
        run_id=RUN_ID,
        tool_call_id=tool_call_id,
        messages=(),
        task_plan=FrozenJsonObject({}),
        context_summary=None,
    )


async def test_checkpoint_finalization_cancellation_restores_before_propagating() -> None:
    workspace = _BlockingCheckpointWorkspace()
    coordinator = InMemoryCheckpointCoordinator(
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace=cast("GitWorktreeWorkspace", workspace),
        clock=SteppingClock(NOW),
    )
    checkpoint = await _fake_checkpoint(coordinator)
    completion = asyncio.create_task(coordinator.complete_tool(checkpoint, tool_call_id="edit-1"))
    assert await asyncio.to_thread(workspace.commit_started.wait, 2)

    completion.cancel()
    workspace.release_commit.set()
    with pytest.raises(asyncio.CancelledError):
        await completion

    assert workspace.restored == ["revision-before"]
    with pytest.raises(DomainOperationError) as rolled_back:
        await coordinator.complete_tool(checkpoint, tool_call_id="edit-1")
    assert rolled_back.value.code == "checkpoint_state_conflict"


async def test_concurrent_checkpoint_creation_cannot_overwrite_duplicate_ids() -> None:
    workspace = _BlockingCheckpointWorkspace()
    fixed_id = UUID("30000000-0000-0000-0000-000000000003")
    coordinator = InMemoryCheckpointCoordinator(
        run_id=RUN_ID,
        session_id=SESSION_ID,
        workspace=cast("GitWorktreeWorkspace", workspace),
        clock=SteppingClock(NOW),
        id_factory=lambda: fixed_id,
    )

    results = await asyncio.gather(
        _fake_checkpoint(coordinator, tool_call_id="edit-1"),
        _fake_checkpoint(coordinator, tool_call_id="edit-2"),
        return_exceptions=True,
    )

    assert sum(isinstance(result, Checkpoint) for result in results) == 1
    conflicts = [result for result in results if isinstance(result, DomainOperationError)]
    assert len(conflicts) == 1
    assert conflicts[0].code == "checkpoint_id_conflict"
    assert workspace.snapshot_calls == 1
