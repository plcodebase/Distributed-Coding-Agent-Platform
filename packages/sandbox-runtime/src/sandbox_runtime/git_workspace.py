"""Private Git worktrees with deterministic process and snapshot boundaries."""

from __future__ import annotations

import asyncio
import errno
import hashlib
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from select import select
from typing import TYPE_CHECKING, Protocol

from agent_core.domain.errors import DomainOperationError
from agent_core.sandbox import WorkspaceSnapshot
from sandbox_runtime.access import WorkspaceAccessPolicy, normalize_workspace_path
from sandbox_runtime.workspace import RootedWorkspace

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence, Set

    from agent_core.domain.base import JsonObject

_DEFAULT_GIT_TIMEOUT_SECONDS = 30.0
_DEFAULT_GIT_OUTPUT_LIMIT_BYTES = 8 * 1024 * 1024
_MAX_GIT_TIMEOUT_SECONDS = 3600.0
_MAX_RUN_ID_BYTES = 1024
_MAX_LABEL_BYTES = 1024
_COPY_CHUNK_BYTES = 64 * 1024
_DIRECTORY_OPEN_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_FILE_OPEN_FLAGS = os.O_RDONLY | os.O_NOFOLLOW
_REVISION_PATTERN = re.compile(r"^[a-f0-9]{40}(?:[a-f0-9]{24})?$")
_GIT_CONFIG_ARGUMENTS = (
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "core.fsmonitor=false",
    "-c",
    "commit.gpgSign=false",
    "-c",
    "tag.gpgSign=false",
    "-c",
    "diff.external=",
    "-c",
    "interactive.diffFilter=",
)
_GIT_AUTHOR_ARGUMENTS = (
    "-c",
    "user.name=Agent Platform",
    "-c",
    "user.email=agent-platform@invalid.local",
)
_GIT_ENVIRONMENT: Mapping[str, str] = {
    "LANG": "C",
    "LC_ALL": "C",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_TERMINAL_PROMPT": "0",
    "GIT_PAGER": "cat",
    "PAGER": "cat",
    "GIT_OPTIONAL_LOCKS": "0",
}


def _git_error(
    code: str,
    message: str,
    *,
    details: JsonObject | None = None,
) -> DomainOperationError:
    return DomainOperationError(code=code, message=message, details=details)


def _positive_integer(name: str, value: int) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _positive_timeout(value: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
        or value > _MAX_GIT_TIMEOUT_SECONDS
    ):
        raise ValueError("git_timeout_seconds must be positive, finite, and at most 3600")
    return float(value)


def _resolve_executable(value: str, *, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be non-empty text")
    candidate = value if Path(value).is_absolute() else shutil.which(value)
    if candidate is None:
        raise ValueError(f"{name} must resolve to an executable")
    try:
        resolved = Path(candidate).resolve(strict=True)
        metadata = resolved.stat()
    except OSError as error:
        raise ValueError(f"{name} must resolve to an executable") from error
    if not stat.S_ISREG(metadata.st_mode) or not os.access(resolved, os.X_OK):
        raise ValueError(f"{name} must be an absolute regular executable")
    return os.fspath(resolved)


def _validate_revision(value: str) -> str:
    if not _REVISION_PATTERN.fullmatch(value):
        raise _git_error(
            "workspace_revision_invalid",
            "the workspace revision is not a full Git object identifier",
        )
    return value


class _GitRunner(Protocol):
    def run(
        self,
        repository: Path,
        arguments: Sequence[str],
        *,
        text: bool = True,
        output_limit_bytes: int | None = None,
        allowed_exit_codes: Set[int] = frozenset({0}),
    ) -> str | bytes: ...


class _BoundedGitRunner:
    """Synchronous Git runner with fixed configuration, environment, and limits."""

    def __init__(
        self,
        executable: str,
        *,
        timeout_seconds: float,
        output_limit_bytes: int,
    ) -> None:
        self._executable = executable
        self._timeout_seconds = timeout_seconds
        self._output_limit_bytes = output_limit_bytes

    def run(
        self,
        repository: Path,
        arguments: Sequence[str],
        *,
        text: bool = True,
        output_limit_bytes: int | None = None,
        allowed_exit_codes: Set[int] = frozenset({0}),
    ) -> str | bytes:
        limit = output_limit_bytes or self._output_limit_bytes
        if type(limit) is not int or limit <= 0:
            raise ValueError("Git output limits must be positive integers")
        argv = (
            self._executable,
            "--no-pager",
            *_GIT_CONFIG_ARGUMENTS,
            "-C",
            os.fspath(repository),
            *arguments,
        )
        try:
            process = subprocess.Popen(  # noqa: S603 - fixed executable and platform argv
                argv,
                env=_GIT_ENVIRONMENT,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
        except (OSError, ValueError) as error:
            raise _git_error(
                "git_operation_failed",
                "the isolated Git operation could not be started",
            ) from error
        try:
            output, return_code = self._collect(process, limit)
        except BaseException:
            self._terminate(process)
            raise
        finally:
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
        if return_code not in allowed_exit_codes:
            raise _git_error(
                "git_operation_failed",
                "the isolated Git operation failed",
                details={"exit_code": return_code},
            )
        if not text:
            return output
        try:
            decoded = output.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise _git_error(
                "git_protocol_error",
                "the isolated Git operation returned invalid UTF-8",
            ) from error
        return decoded[:-1] if decoded.endswith("\n") else decoded

    def _collect(
        self,
        process: subprocess.Popen[bytes],
        output_limit_bytes: int,
    ) -> tuple[bytes, int]:
        if process.stdout is None or process.stderr is None:
            raise _git_error(
                "git_operation_failed",
                "the isolated Git operation did not expose bounded output streams",
            )
        stdout = bytearray()
        total_bytes = 0
        streams = {process.stdout, process.stderr}
        deadline = time.monotonic() + self._timeout_seconds
        while streams:
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                raise _git_error(
                    "git_timeout",
                    "the isolated Git operation exceeded its timeout",
                )
            readable, _, _ = select(tuple(streams), (), (), remaining_seconds)
            if not readable:
                raise _git_error(
                    "git_timeout",
                    "the isolated Git operation exceeded its timeout",
                )
            for stream in readable:
                chunk = os.read(
                    stream.fileno(),
                    min(_COPY_CHUNK_BYTES, output_limit_bytes + 1 - total_bytes),
                )
                if not chunk:
                    streams.remove(stream)
                    continue
                total_bytes += len(chunk)
                if total_bytes > output_limit_bytes:
                    raise _git_error(
                        "git_output_limit",
                        "the isolated Git operation exceeded its output limit",
                        details={"limit_bytes": output_limit_bytes},
                    )
                if stream is process.stdout:
                    stdout.extend(chunk)
        remaining_seconds = max(0.001, deadline - time.monotonic())
        try:
            return bytes(stdout), process.wait(timeout=remaining_seconds)
        except subprocess.TimeoutExpired as error:
            raise _git_error(
                "git_timeout",
                "the isolated Git operation exceeded its timeout",
            ) from error

    @staticmethod
    def _terminate(process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        with suppress(ProcessLookupError, PermissionError):
            os.killpg(process.pid, signal.SIGKILL)
        with suppress(ProcessLookupError):
            process.kill()
        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=1)


@dataclass(frozen=True, slots=True, order=True)
class _UntrackedEntry:
    path: str
    mode: int
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _TrackedFingerprint:
    head: str
    snapshot_revision: str
    worktree_tree: str
    index_tree: str


def _require_bytes(value: str | bytes) -> bytes:
    if not isinstance(value, bytes):
        raise TypeError("binary Git operation returned text")
    return value


def _require_text(value: str | bytes) -> str:
    if not isinstance(value, str):
        raise TypeError("text Git operation returned bytes")
    return value


def _open_relative_parent(root_fd: int, relative: PurePosixPath) -> tuple[int, str]:
    parts = relative.parts
    current_fd = os.dup(root_fd)
    try:
        for component in parts[:-1]:
            next_fd = os.open(component, _DIRECTORY_OPEN_FLAGS, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        return current_fd, parts[-1]
    except BaseException:
        os.close(current_fd)
        raise


def _open_or_create_relative_parent(root_fd: int, relative: PurePosixPath) -> tuple[int, str]:
    parts = relative.parts
    current_fd = os.dup(root_fd)
    try:
        for component in parts[:-1]:
            with suppress(FileExistsError):
                os.mkdir(component, mode=0o700, dir_fd=current_fd)
            next_fd = os.open(component, _DIRECTORY_OPEN_FLAGS, dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        return current_fd, parts[-1]
    except BaseException:
        os.close(current_fd)
        raise


def _write_all(descriptor: int, value: bytes) -> None:
    view = memoryview(value)
    written = 0
    while written < len(view):
        count = os.write(descriptor, view[written:])
        if count <= 0:
            raise OSError("short snapshot write")
        written += count


class GitWorktreeWorkspace(RootedWorkspace):
    """Rooted workspace backed by one private detached linked Git worktree."""

    def __init__(
        self,
        *,
        source_repository: Path,
        allocation_root: Path,
        worktree: Path,
        baseline_revision: str,
        max_file_bytes: int,
        ripgrep_path: str,
        max_patch_bytes: int,
        access_policy: WorkspaceAccessPolicy,
        git_runner: _GitRunner,
        git_output_limit_bytes: int,
    ) -> None:
        super().__init__(
            worktree,
            max_file_bytes=max_file_bytes,
            ripgrep_path=ripgrep_path,
            access_policy=access_policy,
        )
        self._source_repository = source_repository
        self._allocation_root = allocation_root
        self._baseline_revision = _validate_revision(baseline_revision)
        self._max_patch_bytes = max_patch_bytes
        self._git_runner = git_runner
        self._git_output_limit_bytes = git_output_limit_bytes
        self._worktree_removed = False
        self._destroyed = False

    @property
    def baseline_revision(self) -> str:
        return self._baseline_revision

    @property
    def current_revision(self) -> str:
        return _validate_revision(
            _require_text(self._git_runner.run(self.root, ("rev-parse", "HEAD")))
        )

    def _require_no_external_filters(self) -> None:
        configured = _require_text(
            self._git_runner.run(
                self.root,
                ("config", "--get-regexp", r"^filter\..*\.(clean|smudge|process)$"),
                allowed_exit_codes=frozenset({0, 1}),
            )
        )
        if configured:
            raise _git_error(
                "workspace_external_filter_unsupported",
                "repositories with external Git clean, smudge, or process filters are unsupported",
            )

    def commit_state(self, *, label: str) -> str:
        if (
            not isinstance(label, str)
            or not label
            or "\x00" in label
            or len(label.encode("utf-8")) > _MAX_LABEL_BYTES
        ):
            raise ValueError("Git snapshot labels must be non-empty bounded text")
        self._require_no_external_filters()
        self._git_runner.run(self.root, ("add", "-A"))
        status = _require_text(self._git_runner.run(self.root, ("status", "--porcelain=v1")))
        if status:
            self._git_runner.run(
                self.root,
                (
                    *_GIT_AUTHOR_ARGUMENTS,
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
        validated = _validate_revision(revision)
        self._require_no_external_filters()
        self._git_runner.run(self.root, ("cat-file", "-e", f"{validated}^{{commit}}"))
        self._git_runner.run(self.root, ("reset", "--hard", validated))
        self._git_runner.run(self.root, ("clean", "-fdx"))

    def final_patch(self) -> bytes:
        final_revision = self.commit_state(label="final workspace state")
        limit = min(self._max_patch_bytes, self._git_output_limit_bytes)
        try:
            patch = _require_bytes(
                self._git_runner.run(
                    self.root,
                    (
                        "diff",
                        "--no-ext-diff",
                        "--no-textconv",
                        "--binary",
                        "--full-index",
                        f"{self._baseline_revision}..{final_revision}",
                    ),
                    text=False,
                    output_limit_bytes=limit,
                )
            )
        except DomainOperationError as error:
            if error.code != "git_output_limit":
                raise
            raise _git_error(
                "final_patch_limit",
                "the final workspace patch exceeds the configured byte limit",
                details={"limit_bytes": limit},
            ) from error
        if final_revision != self._baseline_revision and not patch:
            raise _git_error(
                "final_patch_protocol_error",
                "Git returned an empty patch for differing workspace revisions",
            )
        return patch

    async def destroy(self) -> None:
        if self._destroyed:
            return
        await self.close()
        await asyncio.to_thread(self._destroy_sync)

    def _destroy_sync(self) -> None:
        try:
            if not self._worktree_removed:
                self._git_runner.run(
                    self._source_repository,
                    ("worktree", "remove", "--force", os.fspath(self.root)),
                )
                self._worktree_removed = True
            if self._allocation_root.exists():
                shutil.rmtree(self._allocation_root)
        except (DomainOperationError, OSError) as error:
            raise _git_error(
                "workspace_cleanup_failed",
                "the isolated workspace could not be removed; cleanup may be retried",
                details={"retryable": True},
            ) from error
        self._destroyed = True


class GitWorktreeManager:
    """Create consistent private worktrees without changing the source checkout."""

    def __init__(
        self,
        *,
        worktree_parent: Path | None = None,
        max_untracked_files: int = 1000,
        max_untracked_bytes: int = 20 * 1024 * 1024,
        max_untracked_file_bytes: int = 4 * 1024 * 1024,
        max_file_bytes: int = 4 * 1024 * 1024,
        max_patch_bytes: int = 10 * 1024 * 1024,
        ripgrep_path: str = "rg",
        git_executable: str = "git",
        git_timeout_seconds: float = _DEFAULT_GIT_TIMEOUT_SECONDS,
        git_output_limit_bytes: int = _DEFAULT_GIT_OUTPUT_LIMIT_BYTES,
        access_policy: WorkspaceAccessPolicy | None = None,
        _runner: _GitRunner | None = None,
    ) -> None:
        self._max_untracked_files = _positive_integer("max_untracked_files", max_untracked_files)
        self._max_untracked_bytes = _positive_integer("max_untracked_bytes", max_untracked_bytes)
        self._max_untracked_file_bytes = _positive_integer(
            "max_untracked_file_bytes", max_untracked_file_bytes
        )
        self._max_file_bytes = _positive_integer("max_file_bytes", max_file_bytes)
        self._max_patch_bytes = _positive_integer("max_patch_bytes", max_patch_bytes)
        self._git_output_limit_bytes = _positive_integer(
            "git_output_limit_bytes", git_output_limit_bytes
        )
        timeout = _positive_timeout(git_timeout_seconds)
        if access_policy is not None and not isinstance(access_policy, WorkspaceAccessPolicy):
            raise TypeError("access_policy must be a WorkspaceAccessPolicy")
        self._access_policy = access_policy or WorkspaceAccessPolicy()
        parent = Path(tempfile.gettempdir()) if worktree_parent is None else worktree_parent
        try:
            self._worktree_parent = parent.resolve(strict=True)
        except OSError as error:
            raise ValueError("worktree_parent must be an existing directory") from error
        if not self._worktree_parent.is_dir():
            raise ValueError("worktree_parent must be an existing directory")
        if not os.access(self._worktree_parent, os.W_OK | os.X_OK):
            raise ValueError("worktree_parent must be writable and searchable")
        self._git_executable = _resolve_executable(
            git_executable,
            name="git_executable",
        )
        self._ripgrep_path = _resolve_executable(ripgrep_path, name="ripgrep_path")
        self._runner = _runner or _BoundedGitRunner(
            self._git_executable,
            timeout_seconds=timeout,
            output_limit_bytes=self._git_output_limit_bytes,
        )

    def create(
        self,
        source_repository: Path,
        *,
        run_id: str,
    ) -> GitWorktreeWorkspace:
        run_token = self._run_token(run_id)
        try:
            source = Path(
                _require_text(
                    self._runner.run(
                        source_repository,
                        ("rev-parse", "--show-toplevel"),
                    )
                )
            ).resolve(strict=True)
        except OSError as error:
            raise _git_error(
                "git_operation_failed",
                "the source repository path returned by Git is unavailable",
            ) from error
        self._require_no_external_filters(source)
        initial_tracked = self._tracked_fingerprint(source, run_token=run_token)

        allocation_root = Path(
            tempfile.mkdtemp(
                prefix=f"agent-run-{run_token}-",
                dir=self._worktree_parent,
            )
        )
        staging = allocation_root / "staging"
        worktree = allocation_root / "workspace"
        registered = False
        try:
            allocation_root.chmod(0o700)
            staging.mkdir(mode=0o700)
            initial_untracked = self._capture_untracked(source, destination=staging)
            self._runner.run(
                source,
                (
                    "worktree",
                    "add",
                    "--detach",
                    os.fspath(worktree),
                    initial_tracked.snapshot_revision,
                ),
            )
            registered = True
            worktree.chmod(0o700)
            self._copy_manifest(staging, worktree, initial_untracked)
            baseline = self._commit_state(
                worktree,
                label=f"agent baseline {run_token}",
            )
            final_tracked = self._tracked_fingerprint(source, run_token=run_token)
            final_untracked = self._capture_untracked(source, destination=None)
            self._require_consistent_source(
                initial_tracked=initial_tracked,
                final_tracked=final_tracked,
                initial_untracked=initial_untracked,
                final_untracked=final_untracked,
            )
            shutil.rmtree(staging)
            workspace = GitWorktreeWorkspace(
                source_repository=source,
                allocation_root=allocation_root,
                worktree=worktree,
                baseline_revision=baseline,
                max_file_bytes=self._max_file_bytes,
                ripgrep_path=self._ripgrep_path,
                max_patch_bytes=self._max_patch_bytes,
                access_policy=self._access_policy,
                git_runner=self._runner,
                git_output_limit_bytes=self._git_output_limit_bytes,
            )
        except BaseException as primary:
            cleanup_error = self._cleanup_incomplete(
                source=source,
                allocation_root=allocation_root,
                worktree=worktree,
                registered=registered or worktree.exists(),
            )
            if cleanup_error is not None:
                raise _git_error(
                    "workspace_cleanup_failed",
                    "workspace creation failed and targeted cleanup was incomplete",
                    details={"retryable": True},
                ) from primary
            raise
        return workspace

    @staticmethod
    def _run_token(run_id: str) -> str:
        if (
            not isinstance(run_id, str)
            or not run_id
            or "\x00" in run_id
            or len(run_id.encode("utf-8")) > _MAX_RUN_ID_BYTES
        ):
            raise ValueError("run_id must be non-empty bounded text")
        return hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:20]

    def _require_no_external_filters(self, repository: Path) -> None:
        configured = _require_text(
            self._runner.run(
                repository,
                ("config", "--get-regexp", r"^filter\..*\.(clean|smudge|process)$"),
                allowed_exit_codes=frozenset({0, 1}),
            )
        )
        if configured:
            raise _git_error(
                "workspace_external_filter_unsupported",
                "repositories with external Git clean, smudge, or process filters are unsupported",
            )

    def _tracked_fingerprint(
        self,
        source: Path,
        *,
        run_token: str,
    ) -> _TrackedFingerprint:
        head = _validate_revision(_require_text(self._runner.run(source, ("rev-parse", "HEAD"))))
        snapshot = _require_text(
            self._runner.run(
                source,
                ("stash", "create", f"agent-platform-{run_token}"),
            )
        )
        snapshot_revision = _validate_revision(snapshot or head)
        if snapshot:
            worktree_tree = _validate_revision(
                _require_text(
                    self._runner.run(
                        source,
                        ("rev-parse", f"{snapshot_revision}^{{tree}}"),
                    )
                )
            )
            index_tree = _validate_revision(
                _require_text(
                    self._runner.run(
                        source,
                        ("rev-parse", f"{snapshot_revision}^2^{{tree}}"),
                    )
                )
            )
        else:
            worktree_tree = _validate_revision(
                _require_text(self._runner.run(source, ("rev-parse", "HEAD^{tree}")))
            )
            index_tree = _validate_revision(
                _require_text(self._runner.run(source, ("write-tree",)))
            )
        return _TrackedFingerprint(
            head=head,
            snapshot_revision=snapshot_revision,
            worktree_tree=worktree_tree,
            index_tree=index_tree,
        )

    @staticmethod
    def _require_consistent_source(
        *,
        initial_tracked: _TrackedFingerprint,
        final_tracked: _TrackedFingerprint,
        initial_untracked: tuple[_UntrackedEntry, ...],
        final_untracked: tuple[_UntrackedEntry, ...],
    ) -> None:
        if (
            final_tracked.head != initial_tracked.head
            or final_tracked.worktree_tree != initial_tracked.worktree_tree
            or final_tracked.index_tree != initial_tracked.index_tree
            or final_untracked != initial_untracked
        ):
            raise _git_error(
                "source_repository_changed",
                "the source repository changed during workspace capture",
            )

    def _untracked_paths(self, source: Path) -> tuple[PurePosixPath, ...]:
        raw = _require_bytes(
            self._runner.run(
                source,
                ("ls-files", "--others", "--exclude-standard", "-z"),
                text=False,
            )
        )
        paths: list[PurePosixPath] = []
        start = 0
        while start < len(raw):
            end = raw.find(b"\0", start)
            if end < 0:
                raise _git_error(
                    "git_protocol_error",
                    "Git returned an unterminated untracked path",
                )
            encoded = raw[start:end]
            start = end + 1
            if not encoded:
                continue
            if len(paths) >= self._max_untracked_files:
                raise _git_error(
                    "workspace_untracked_limit",
                    "the source repository contains too many untracked files",
                    details={"limit": self._max_untracked_files},
                )
            try:
                value = encoded.decode("utf-8", errors="strict")
            except UnicodeDecodeError as error:
                raise _git_error(
                    "workspace_untracked_path_invalid",
                    "untracked paths must use UTF-8",
                ) from error
            try:
                relative = normalize_workspace_path(value)
            except DomainOperationError as error:
                raise _git_error(
                    "workspace_untracked_path_invalid",
                    "Git returned an unsafe untracked path",
                ) from error
            if relative.as_posix() != value or self._access_policy.is_repository_metadata(relative):
                raise _git_error(
                    "workspace_untracked_path_invalid",
                    "Git returned a noncanonical untracked path",
                )
            paths.append(relative)
        return tuple(sorted(paths, key=PurePosixPath.as_posix))

    def _capture_untracked(
        self,
        source: Path,
        *,
        destination: Path | None,
    ) -> tuple[_UntrackedEntry, ...]:
        paths = self._untracked_paths(source)
        source_fd = os.open(source, _DIRECTORY_OPEN_FLAGS)
        destination_fd = (
            os.open(destination, _DIRECTORY_OPEN_FLAGS) if destination is not None else None
        )
        total_bytes = 0
        entries: list[_UntrackedEntry] = []
        try:
            for relative in paths:
                entry, copied_bytes = self._capture_one_untracked(
                    source_fd,
                    relative,
                    destination_fd=destination_fd,
                    aggregate_bytes=total_bytes,
                )
                total_bytes += copied_bytes
                entries.append(entry)
        finally:
            os.close(source_fd)
            if destination_fd is not None:
                os.close(destination_fd)
        return tuple(entries)

    def _capture_one_untracked(  # noqa: PLR0912, PLR0915 - descriptor lifecycle
        self,
        source_root_fd: int,
        relative: PurePosixPath,
        *,
        destination_fd: int | None,
        aggregate_bytes: int,
    ) -> tuple[_UntrackedEntry, int]:
        source_parent_fd = -1
        output_parent_fd = -1
        output_fd = -1
        source_fd = -1
        try:
            source_parent_fd, source_name = _open_relative_parent(source_root_fd, relative)
            path_metadata = os.stat(
                source_name,
                dir_fd=source_parent_fd,
                follow_symlinks=False,
            )
            if not stat.S_ISREG(path_metadata.st_mode):
                raise _git_error(
                    "workspace_untracked_type",
                    "only regular untracked files can enter an isolated workspace",
                    details={"path": relative.as_posix()},
                )
            source_fd = os.open(source_name, _FILE_OPEN_FLAGS, dir_fd=source_parent_fd)
            before = os.fstat(source_fd)
            if not stat.S_ISREG(before.st_mode):
                raise _git_error(
                    "workspace_untracked_type",
                    "only regular untracked files can enter an isolated workspace",
                    details={"path": relative.as_posix()},
                )
            if before.st_size > self._max_untracked_file_bytes:
                raise _git_error(
                    "workspace_untracked_file_limit",
                    "an untracked source file exceeds its byte limit",
                    details={
                        "path": relative.as_posix(),
                        "limit_bytes": self._max_untracked_file_bytes,
                    },
                )
            if destination_fd is not None:
                output_parent_fd, output_name = _open_or_create_relative_parent(
                    destination_fd,
                    relative,
                )
                output_fd = os.open(
                    output_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=output_parent_fd,
                )
            digest = hashlib.sha256()
            size = 0
            while chunk := os.read(source_fd, _COPY_CHUNK_BYTES):
                size += len(chunk)
                if size > self._max_untracked_file_bytes:
                    raise _git_error(
                        "workspace_untracked_file_limit",
                        "an untracked source file grew beyond its byte limit",
                        details={
                            "path": relative.as_posix(),
                            "limit_bytes": self._max_untracked_file_bytes,
                        },
                    )
                if aggregate_bytes + size > self._max_untracked_bytes:
                    raise _git_error(
                        "workspace_untracked_limit",
                        "untracked source files exceed the aggregate byte limit",
                        details={"limit_bytes": self._max_untracked_bytes},
                    )
                digest.update(chunk)
                if output_fd >= 0:
                    _write_all(output_fd, chunk)
            after = os.fstat(source_fd)
            if (before.st_dev, before.st_ino) != (
                after.st_dev,
                after.st_ino,
            ) or after.st_size != size:
                raise _git_error(
                    "source_repository_changed",
                    "an untracked file changed during workspace capture",
                    details={"path": relative.as_posix()},
                )
            mode = stat.S_IMODE(after.st_mode) & 0o777
            if output_fd >= 0:
                os.fsync(output_fd)
            return (
                _UntrackedEntry(
                    path=relative.as_posix(),
                    mode=mode,
                    size=size,
                    sha256=digest.hexdigest(),
                ),
                size,
            )
        except FileNotFoundError as error:
            raise _git_error(
                "source_repository_changed",
                "an untracked file disappeared during workspace capture",
                details={"path": relative.as_posix()},
            ) from error
        except OSError as error:
            if error.errno in {errno.ELOOP, errno.ENOENT, errno.ENOTDIR}:
                raise _git_error(
                    "source_repository_changed",
                    "an untracked path changed during workspace capture",
                    details={"path": relative.as_posix()},
                ) from error
            raise _git_error(
                "workspace_untracked_copy_failed",
                "an untracked file could not be copied safely",
                details={"path": relative.as_posix()},
            ) from error
        finally:
            if source_fd >= 0:
                os.close(source_fd)
            if source_parent_fd >= 0:
                os.close(source_parent_fd)
            if output_fd >= 0:
                os.close(output_fd)
            if output_parent_fd >= 0:
                os.close(output_parent_fd)

    def _copy_manifest(
        self,
        staging: Path,
        worktree: Path,
        manifest: tuple[_UntrackedEntry, ...],
    ) -> None:
        staging_fd = os.open(staging, _DIRECTORY_OPEN_FLAGS)
        worktree_fd = os.open(worktree, _DIRECTORY_OPEN_FLAGS)
        try:
            for expected in manifest:
                relative = PurePosixPath(expected.path)
                source_parent_fd, source_name = _open_relative_parent(staging_fd, relative)
                destination_parent_fd, destination_name = _open_or_create_relative_parent(
                    worktree_fd,
                    relative,
                )
                source_fd = -1
                destination_fd = -1
                try:
                    source_fd = os.open(
                        source_name,
                        _FILE_OPEN_FLAGS,
                        dir_fd=source_parent_fd,
                    )
                    destination_fd = os.open(
                        destination_name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                        0o600,
                        dir_fd=destination_parent_fd,
                    )
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := os.read(source_fd, _COPY_CHUNK_BYTES):
                        size += len(chunk)
                        digest.update(chunk)
                        _write_all(destination_fd, chunk)
                    if size != expected.size or digest.hexdigest() != expected.sha256:
                        raise _git_error(
                            "source_repository_changed",
                            "the private untracked staging snapshot changed",
                            details={"path": expected.path},
                        )
                    os.fchmod(destination_fd, expected.mode)
                    os.fsync(destination_fd)
                finally:
                    if source_fd >= 0:
                        os.close(source_fd)
                    if destination_fd >= 0:
                        os.close(destination_fd)
                    os.close(source_parent_fd)
                    os.close(destination_parent_fd)
        except OSError as error:
            raise _git_error(
                "workspace_untracked_copy_failed",
                "the untracked snapshot could not enter the isolated worktree",
            ) from error
        finally:
            os.close(staging_fd)
            os.close(worktree_fd)

    def _commit_state(self, worktree: Path, *, label: str) -> str:
        self._require_no_external_filters(worktree)
        self._runner.run(worktree, ("add", "-A"))
        status = _require_text(self._runner.run(worktree, ("status", "--porcelain=v1")))
        if status:
            self._runner.run(
                worktree,
                (
                    *_GIT_AUTHOR_ARGUMENTS,
                    "commit",
                    "--no-verify",
                    "-m",
                    label,
                ),
            )
        return _validate_revision(_require_text(self._runner.run(worktree, ("rev-parse", "HEAD"))))

    def _cleanup_incomplete(
        self,
        *,
        source: Path,
        allocation_root: Path,
        worktree: Path,
        registered: bool,
    ) -> BaseException | None:
        cleanup_error: BaseException | None = None
        worktree_removed = not registered
        if registered:
            try:
                self._runner.run(
                    source,
                    ("worktree", "remove", "--force", os.fspath(worktree)),
                )
                worktree_removed = True
            except BaseException as error:
                cleanup_error = error
        if worktree_removed:
            try:
                if allocation_root.exists():
                    shutil.rmtree(allocation_root)
            except OSError as error:
                cleanup_error = cleanup_error or error
        return cleanup_error


__all__ = ["GitWorktreeManager", "GitWorktreeWorkspace"]
