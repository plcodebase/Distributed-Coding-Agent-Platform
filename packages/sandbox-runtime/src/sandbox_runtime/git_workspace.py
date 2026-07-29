"""Isolated Git worktrees with dirty-baseline capture and reversible revisions."""

from __future__ import annotations

import os
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from select import select
from typing import TYPE_CHECKING

from agent_core.domain.errors import DomainOperationError
from agent_core.sandbox import WorkspaceSnapshot
from sandbox_runtime.workspace import RootedWorkspace

if TYPE_CHECKING:
    from collections.abc import Sequence

_GIT_TIMEOUT_SECONDS = 30
_GIT_EXECUTABLE = shutil.which("git") or "/usr/bin/git"
_GIT_OUTPUT_LIMIT_BYTES = 8 * 1024 * 1024
_GIT_AUTHOR_ARGS = (
    "-c",
    "user.name=Agent Platform",
    "-c",
    "user.email=agent-platform@invalid.local",
    "-c",
    "commit.gpgSign=false",
)


def _git(
    repository: Path,
    arguments: Sequence[str],
    *,
    text: bool = True,
    output_limit_bytes: int = _GIT_OUTPUT_LIMIT_BYTES,
) -> str | bytes:
    try:
        process = subprocess.Popen(  # noqa: S603 - argv is fixed by trusted platform code
            (_GIT_EXECUTABLE, "-C", str(repository), *arguments),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as error:
        raise DomainOperationError(
            code="git_operation_failed",
            message="the isolated Git operation could not be completed",
        ) from error
    if process.stdout is None:
        _terminate_git(process)
        raise DomainOperationError(
            code="git_operation_failed",
            message="the isolated Git operation did not expose bounded output",
        )

    try:
        output, return_code = _collect_git_output(process, output_limit_bytes)
    except DomainOperationError:
        _terminate_git(process)
        raise
    except (TimeoutError, subprocess.TimeoutExpired) as error:
        _terminate_git(process)
        raise DomainOperationError(
            code="git_operation_failed",
            message="the isolated Git operation exceeded its timeout",
        ) from error
    finally:
        process.stdout.close()

    if return_code != 0:
        raise DomainOperationError(
            code="git_operation_failed",
            message="the isolated Git operation failed",
            details={"exit_code": return_code},
        )
    if text:
        try:
            return output.decode("utf-8", errors="strict").strip()
        except UnicodeDecodeError as error:
            raise DomainOperationError(
                code="git_operation_failed",
                message="the isolated Git operation returned invalid text",
            ) from error
    return output


def _collect_git_output(
    process: subprocess.Popen[bytes],
    output_limit_bytes: int,
) -> tuple[bytes, int]:
    if process.stdout is None:
        raise DomainOperationError(
            code="git_operation_failed",
            message="the isolated Git operation did not expose bounded output",
        )
    output = bytearray()
    deadline = time.monotonic() + _GIT_TIMEOUT_SECONDS
    while True:
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            raise TimeoutError
        readable, _, _ = select((process.stdout,), (), (), remaining_seconds)
        if not readable:
            raise TimeoutError
        chunk = os.read(
            process.stdout.fileno(),
            min(65_536, output_limit_bytes + 1 - len(output)),
        )
        if not chunk:
            break
        output.extend(chunk)
        if len(output) > output_limit_bytes:
            raise DomainOperationError(
                code="git_output_limit",
                message="the isolated Git operation exceeded its output limit",
                details={"limit_bytes": output_limit_bytes},
            )
    remaining_seconds = max(0.001, deadline - time.monotonic())
    return bytes(output), process.wait(timeout=remaining_seconds)


def _terminate_git(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except PermissionError:
        with suppress(ProcessLookupError):
            process.kill()
    except ProcessLookupError:
        pass
    with suppress(subprocess.TimeoutExpired):
        process.wait(timeout=1)


class GitWorktreeWorkspace(RootedWorkspace):
    """Rooted workspace backed by one detached linked Git worktree."""

    def __init__(
        self,
        *,
        source_repository: Path,
        worktree: Path,
        baseline_revision: str,
        max_file_bytes: int,
        ripgrep_path: str,
        max_patch_bytes: int,
    ) -> None:
        super().__init__(
            worktree,
            max_file_bytes=max_file_bytes,
            ripgrep_path=ripgrep_path,
        )
        self._source_repository = source_repository
        self._baseline_revision = baseline_revision
        self._max_patch_bytes = max_patch_bytes
        self._destroyed = False

    @property
    def baseline_revision(self) -> str:
        return self._baseline_revision

    @property
    def current_revision(self) -> str:
        return str(_git(self.root, ("rev-parse", "HEAD")))

    def commit_state(self, *, label: str) -> str:
        _git(self.root, ("add", "-A"))
        status = str(_git(self.root, ("status", "--porcelain=v1")))
        if status:
            _git(
                self.root,
                (
                    *_GIT_AUTHOR_ARGS,
                    "commit",
                    "--no-verify",
                    "-m",
                    label,
                ),
            )
        return self.current_revision

    def create_snapshot(self, *, label: str) -> WorkspaceSnapshot:
        revision = self.commit_state(label=label)
        return WorkspaceSnapshot(
            id=f"snapshot-{revision[:16]}",
            uri=f"git-worktree://local/{revision}",
            revision=revision,
        )

    def restore_revision(self, revision: str) -> None:
        _git(self.root, ("cat-file", "-e", f"{revision}^{{commit}}"))
        _git(self.root, ("reset", "--hard", revision))
        _git(self.root, ("clean", "-fdx"))

    def final_patch(self) -> bytes:
        final_revision = self.commit_state(label="final workspace state")
        try:
            patch = _git(
                self.root,
                (
                    "diff",
                    "--binary",
                    "--full-index",
                    f"{self._baseline_revision}..{final_revision}",
                ),
                text=False,
                output_limit_bytes=self._max_patch_bytes,
            )
        except DomainOperationError as error:
            if error.code != "git_output_limit":
                raise
            raise DomainOperationError(
                code="final_patch_limit",
                message="the final workspace patch exceeds the configured byte limit",
                details={"limit_bytes": self._max_patch_bytes},
            ) from error
        if not isinstance(patch, bytes):
            raise TypeError("binary Git operation returned text")
        return patch

    async def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        await self.close()
        try:
            _git(
                self._source_repository,
                ("worktree", "remove", "--force", str(self.root)),
            )
        finally:
            shutil.rmtree(self.root, ignore_errors=True)
            _git(self._source_repository, ("worktree", "prune"))


class GitWorktreeManager:
    """Create isolated per-run worktrees without changing source checkout state."""

    def __init__(
        self,
        *,
        worktree_parent: Path | None = None,
        max_untracked_files: int = 1000,
        max_untracked_bytes: int = 20 * 1024 * 1024,
        max_file_bytes: int = 4 * 1024 * 1024,
        max_patch_bytes: int = 10 * 1024 * 1024,
        ripgrep_path: str = "rg",
    ) -> None:
        self._worktree_parent = worktree_parent
        self._max_untracked_files = max_untracked_files
        self._max_untracked_bytes = max_untracked_bytes
        self._max_file_bytes = max_file_bytes
        self._max_patch_bytes = max_patch_bytes
        self._ripgrep_path = ripgrep_path

    def create(self, source_repository: Path, *, run_id: str) -> GitWorktreeWorkspace:
        source = Path(str(_git(source_repository, ("rev-parse", "--show-toplevel")))).resolve(
            strict=True
        )
        initial_status = str(_git(source, ("status", "--porcelain=v2", "--branch")))
        initial_head = str(_git(source, ("rev-parse", "HEAD")))
        snapshot = str(_git(source, ("stash", "create", f"agent-platform-{run_id}")))
        snapshot_revision = snapshot or initial_head
        worktree = Path(
            tempfile.mkdtemp(
                prefix=f"agent-worktree-{run_id[:12]}-",
                dir=self._worktree_parent,
            )
        )
        shutil.rmtree(worktree)
        try:
            _git(
                source,
                ("worktree", "add", "--detach", str(worktree), snapshot_revision),
            )
            self._copy_untracked(source, worktree)
            workspace = GitWorktreeWorkspace(
                source_repository=source,
                worktree=worktree,
                baseline_revision=snapshot_revision,
                max_file_bytes=self._max_file_bytes,
                ripgrep_path=self._ripgrep_path,
                max_patch_bytes=self._max_patch_bytes,
            )
            baseline = workspace.commit_state(label=f"agent baseline {run_id}")
            workspace._baseline_revision = baseline
            final_status = str(_git(source, ("status", "--porcelain=v2", "--branch")))
            final_head = str(_git(source, ("rev-parse", "HEAD")))
            self._require_source_unchanged(
                initial_status=initial_status,
                final_status=final_status,
                initial_head=initial_head,
                final_head=final_head,
            )
        except Exception:
            if worktree.exists():
                try:
                    _git(source, ("worktree", "remove", "--force", str(worktree)))
                except DomainOperationError:
                    shutil.rmtree(worktree, ignore_errors=True)
            raise
        else:
            return workspace

    def _copy_untracked(self, source: Path, worktree: Path) -> None:
        raw = _git(
            source,
            ("ls-files", "--others", "--exclude-standard", "-z"),
            text=False,
        )
        if not isinstance(raw, bytes):
            raise TypeError("binary Git operation returned text")
        paths = [item.decode("utf-8") for item in raw.split(b"\0") if item]
        if len(paths) > self._max_untracked_files:
            raise DomainOperationError(
                code="workspace_untracked_limit",
                message="the source repository contains too many untracked files",
                details={"limit": self._max_untracked_files},
            )
        total_bytes = 0
        for relative_value in paths:
            relative = Path(relative_value)
            source_path = source / relative
            metadata = source_path.lstat()
            if not stat.S_ISREG(metadata.st_mode):
                raise DomainOperationError(
                    code="workspace_untracked_type",
                    message="only regular untracked files can enter an isolated workspace",
                    details={"path": relative.as_posix()},
                )
            total_bytes += metadata.st_size
            if total_bytes > self._max_untracked_bytes:
                raise DomainOperationError(
                    code="workspace_untracked_limit",
                    message="untracked source files exceed the aggregate byte limit",
                    details={"limit_bytes": self._max_untracked_bytes},
                )
            destination = worktree / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, destination, follow_symlinks=False)

    @staticmethod
    def _require_source_unchanged(
        *,
        initial_status: str,
        final_status: str,
        initial_head: str,
        final_head: str,
    ) -> None:
        if final_status != initial_status or final_head != initial_head:
            raise DomainOperationError(
                code="source_repository_changed",
                message="isolated workspace creation changed the source checkout",
            )


__all__ = ["GitWorktreeManager", "GitWorktreeWorkspace"]
