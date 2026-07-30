"""Rooted filesystem operations with containment and resource limits."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import secrets
import shutil
import stat
import threading
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import AfterValidator, Field, StringConstraints, field_validator, model_validator

from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError
from sandbox_runtime._process import BoundedProcessRunner
from sandbox_runtime.access import WorkspaceAccessPolicy, normalize_workspace_path

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence
    from contextlib import AbstractContextManager
    from types import TracebackType

    from agent_core.domain.base import JsonObject

_LINE_BOUNDARIES = frozenset(
    {"\n", "\r", "\v", "\f", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"}
)


def _line_spans(value: str) -> Iterator[tuple[int, int]]:
    start = 0
    index = 0
    while index < len(value):
        character = value[index]
        if character not in _LINE_BOUNDARIES:
            index += 1
            continue
        end = index + 1
        if character == "\r" and end < len(value) and value[end] == "\n":
            end += 1
        yield start, end
        start = end
        index = end
    if start < len(value):
        yield start, len(value)


def _validate_canonical_path(value: str) -> str:
    normalized = normalize_workspace_path(value)
    if value != normalized.as_posix():
        raise ValueError("workspace result paths must be canonical")
    if WorkspaceAccessPolicy().is_repository_metadata(normalized):
        raise ValueError("workspace result paths may not address repository metadata")
    return value


type CanonicalWorkspacePath = Annotated[
    str,
    StringConstraints(min_length=1, max_length=4096),
    AfterValidator(_validate_canonical_path),
]
type Sha256Hash = Annotated[
    str,
    StringConstraints(pattern=r"^[a-f0-9]{64}$"),
]

_MAX_SEARCH_LINE_BYTES = 4096
_MAX_PERMISSION_MODE = 0o7777
_SEARCH_EVENT_TYPES = frozenset({"begin", "context", "end", "summary"})
_DIRECTORY_OPEN_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_FILE_OPEN_FLAGS = os.O_RDONLY | os.O_NOFOLLOW


@dataclass(frozen=True, slots=True)
class EditTransaction:
    """Validated values produced by one locked workspace edit transaction."""

    path: str
    created: bool
    previous_sha256: str | None
    sha256: str
    replacement_count: int
    patch_sha256: str
    bytes_written: int


class WorkspaceEntryType(StrEnum):
    """Stable entry kinds exposed on the tool wire."""

    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"


class WorkspaceEntry(DomainModel):
    """One immutable, validated workspace listing entry."""

    path: CanonicalWorkspacePath
    type: WorkspaceEntryType


class ReadResult(DomainModel):
    """A complete-line, losslessly continuable file read."""

    path: CanonicalWorkspacePath
    content: str
    sha256: Sha256Hash
    start_line: int = Field(ge=1)
    end_line: int = Field(ge=0)
    total_lines: int = Field(ge=0)
    truncated: bool
    next_start_line: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_line_contract(self) -> ReadResult:
        if self.total_lines == 0:
            if (
                self.start_line != 1
                or self.end_line != 0
                or self.truncated
                or self.next_start_line is not None
                or self.content
            ):
                raise ValueError("empty-file read metadata is inconsistent")
            return self
        if self.end_line < self.start_line or self.end_line > self.total_lines:
            raise ValueError("read line range is inconsistent")
        if not self.content:
            raise ValueError("a nonempty read range must contain content")
        if sum(1 for _ in _line_spans(self.content)) != self.end_line - self.start_line + 1:
            raise ValueError("read content does not match its line metadata")
        unread = self.end_line < self.total_lines
        if unread != self.truncated:
            raise ValueError("truncated must indicate unread lines")
        if unread and self.content[-1] not in _LINE_BOUNDARIES:
            raise ValueError("truncated reads must end at a complete line boundary")
        expected_next = self.end_line + 1 if unread else None
        if self.next_start_line != expected_next:
            raise ValueError("next_start_line must identify the first unread line")
        return self


class SearchMatch(DomainModel):
    """One normalized ripgrep match."""

    path: CanonicalWorkspacePath
    line: int = Field(ge=1)
    column: int = Field(ge=1)
    text: str

    @field_validator("text")
    @classmethod
    def validate_text_bytes(cls, value: str) -> str:
        if len(value.encode("utf-8")) > _MAX_SEARCH_LINE_BYTES:
            raise ValueError("search match text exceeds its UTF-8 byte limit")
        return value


def _workspace_error(
    code: str,
    message: str,
    *,
    path: str | None = None,
    details: JsonObject | None = None,
) -> DomainOperationError:
    values: JsonObject = dict(details or {})
    if path is not None:
        values["path"] = path
    return DomainOperationError(
        code=code,
        message=message,
        details=values or None,
    )


def _serialized_size(value: JsonObject) -> int:
    return len(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )


def _json_string_content_size(value: str) -> int:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    return len(encoded.encode("utf-8")) - 2


def _read_result_size(
    *,
    path: str,
    content_json_bytes: int,
    sha256: str,
    start_line: int,
    end_line: int,
    total_lines: int,
    truncated: bool,
    next_start_line: int | None,
) -> int:
    values: tuple[tuple[str, int], ...] = (
        ("content", content_json_bytes + 2),
        ("end_line", len(str(end_line))),
        (
            "next_start_line",
            4 if next_start_line is None else len(str(next_start_line)),
        ),
        ("path", _json_string_content_size(path) + 2),
        ("sha256", len(sha256) + 2),
        ("start_line", len(str(start_line))),
        ("total_lines", len(str(total_lines))),
        ("truncated", 4 if truncated else 5),
    )
    return (
        2 + len(values) - 1 + sum(_json_string_content_size(key) + 3 + size for key, size in values)
    )


def _bounded_utf8_prefix(value: str, limit: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="ignore")


class RootedWorkspace:
    """Filesystem view that never intentionally addresses data outside its root."""

    def __init__(
        self,
        root: Path,
        *,
        max_file_bytes: int = 4 * 1024 * 1024,
        max_scanned_entries: int = 20_000,
        max_search_output_bytes: int = 8 * 1024 * 1024,
        ripgrep_path: str = "rg",
        access_policy: WorkspaceAccessPolicy | None = None,
        search_runner: BoundedProcessRunner | None = None,
        directory_scanner: Callable[
            [int],
            AbstractContextManager[Iterable[Any]],
        ] = os.scandir,
    ) -> None:
        for name, value in (
            ("max_file_bytes", max_file_bytes),
            ("max_scanned_entries", max_scanned_entries),
            ("max_search_output_bytes", max_search_output_bytes),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if not isinstance(ripgrep_path, str) or not ripgrep_path:
            raise ValueError("ripgrep_path must be non-empty text")
        if access_policy is not None and not isinstance(access_policy, WorkspaceAccessPolicy):
            raise TypeError("access_policy must be a WorkspaceAccessPolicy")
        if not callable(directory_scanner):
            raise TypeError("directory_scanner must be callable")
        if (
            os.open not in os.supports_dir_fd
            or os.scandir not in os.supports_fd
            or not hasattr(os, "O_NOFOLLOW")
            or not hasattr(os, "O_DIRECTORY")
        ):
            raise ValueError("descriptor-relative no-follow filesystem access is required")

        resolved = root.resolve(strict=True)
        if not resolved.is_dir():
            raise ValueError("workspace root must be a directory")
        executable = (
            shutil.which(ripgrep_path) if not Path(ripgrep_path).is_absolute() else ripgrep_path
        )
        if executable is None:
            raise ValueError("ripgrep_path must resolve to an executable")
        try:
            resolved_executable = Path(executable).resolve(strict=True)
        except OSError as error:
            raise ValueError("ripgrep_path must resolve to an executable") from error
        executable_stat = resolved_executable.stat()
        if not stat.S_ISREG(executable_stat.st_mode) or not os.access(resolved_executable, os.X_OK):
            raise ValueError("ripgrep_path must be an absolute regular executable")

        self._root = resolved
        self._root_fd = os.open(resolved, _DIRECTORY_OPEN_FLAGS)
        self._max_file_bytes = max_file_bytes
        self._max_scanned_entries = max_scanned_entries
        self._max_search_output_bytes = max_search_output_bytes
        self._ripgrep_path = os.fspath(resolved_executable)
        self._access_policy = access_policy or WorkspaceAccessPolicy()
        self._search_runner = search_runner or BoundedProcessRunner()
        self._owns_search_runner = search_runner is None
        self._directory_scanner = directory_scanner
        self._mutation_lock = threading.Lock()
        self._close_lock = asyncio.Lock()
        self._root_fd_closed = False
        self._search_runner_closed = not self._owns_search_runner
        self._closed = False

    @property
    def root(self) -> Path:
        return self._root

    @property
    def access_policy(self) -> WorkspaceAccessPolicy:
        return self._access_policy

    def resolve_path(
        self,
        value: str,
        *,
        must_exist: bool = True,
        for_write: bool = False,
    ) -> Path:
        relative = normalize_workspace_path(value)
        self._access_policy.require_accessible(relative, display_path=value)
        candidate = self._root.joinpath(*relative.parts)
        if for_write:
            self._reject_write_symlinks(candidate)
        try:
            resolved = candidate.resolve(strict=must_exist)
        except FileNotFoundError as error:
            raise _workspace_error(
                "workspace_file_not_found",
                "the requested workspace path does not exist",
                path=value,
            ) from error
        except (OSError, RuntimeError) as error:
            raise _workspace_error(
                "workspace_path_invalid",
                "the requested workspace path could not be resolved",
                path=value,
            ) from error
        if not resolved.is_relative_to(self._root):
            raise _workspace_error(
                "workspace_path_escape",
                "the requested path resolves outside the workspace",
                path=value,
            )
        canonical = PurePosixPath(resolved.relative_to(self._root).as_posix())
        self._access_policy.require_accessible(canonical, display_path=value)
        return resolved

    def list_entries(  # noqa: PLR0915 - one bounded descriptor traversal
        self,
        path: str,
        *,
        max_depth: int,
        max_entries: int,
    ) -> tuple[str, tuple[WorkspaceEntry, ...], bool, bool]:
        if max_depth < 0 or max_entries <= 0:
            raise ValueError("list limits must be nonnegative and positive")
        start, canonical = self._open_path(path, require_directory=True)
        entries: list[WorkspaceEntry] = []
        scanned = 0
        protected_omitted = False

        def visit(  # noqa: PLR0912 - entry types require fail-closed handling
            directory_fd: int,
            relative: PurePosixPath,
            depth: int,
        ) -> None:
            nonlocal protected_omitted, scanned
            children: list[Any] = []
            try:
                with self._directory_scanner(directory_fd) as iterator:
                    for child in iterator:
                        scanned += 1
                        if scanned > self._max_scanned_entries:
                            raise _workspace_error(
                                "workspace_scan_limit",
                                "workspace listing exceeded its scan ceiling",
                                details={"limit_entries": self._max_scanned_entries},
                            )
                        children.append(child)
            except DomainOperationError:
                raise
            except OSError as error:
                raise _workspace_error(
                    "workspace_read_failed",
                    "the workspace directory could not be read",
                    path=relative.as_posix(),
                ) from error

            for child in sorted(children, key=lambda item: item.name):
                child_relative = (
                    PurePosixPath(child.name)
                    if relative == PurePosixPath(".")
                    else relative / child.name
                )
                if self._access_policy.is_protected(child_relative):
                    protected_omitted = True
                    continue
                try:
                    if child.is_symlink():
                        target = self._root.joinpath(*child_relative.parts).resolve(strict=True)
                        if not target.is_relative_to(self._root):
                            continue
                        canonical_target = PurePosixPath(target.relative_to(self._root).as_posix())
                        if self._access_policy.is_protected(canonical_target):
                            protected_omitted = True
                            continue
                        entries.append(
                            WorkspaceEntry(
                                path=child_relative.as_posix(),
                                type=WorkspaceEntryType.SYMLINK,
                            )
                        )
                        continue
                    if child.is_dir(follow_symlinks=False):
                        entries.append(
                            WorkspaceEntry(
                                path=child_relative.as_posix(),
                                type=WorkspaceEntryType.DIRECTORY,
                            )
                        )
                        if depth < max_depth:
                            child_fd = os.open(
                                child.name,
                                _DIRECTORY_OPEN_FLAGS,
                                dir_fd=directory_fd,
                            )
                            try:
                                visit(child_fd, child_relative, depth + 1)
                            finally:
                                os.close(child_fd)
                    elif child.is_file(follow_symlinks=False):
                        entries.append(
                            WorkspaceEntry(
                                path=child_relative.as_posix(),
                                type=WorkspaceEntryType.FILE,
                            )
                        )
                except (OSError, RuntimeError):
                    continue

        try:
            visit(start, PurePosixPath(canonical), 0)
        finally:
            os.close(start)
        ordered = tuple(sorted(entries, key=lambda entry: entry.path))
        return canonical, ordered[:max_entries], len(ordered) > max_entries, protected_omitted

    def read_text(  # noqa: PLR0912 - single-pass line selection contract
        self,
        path: str,
        *,
        start_line: int,
        end_line: int | None,
        result_byte_limit: int,
    ) -> ReadResult:
        if start_line < 1 or (end_line is not None and end_line < start_line):
            raise ValueError("invalid read line range")
        if result_byte_limit <= 0:
            raise ValueError("result_byte_limit must be positive")
        descriptor, canonical = self._open_path(path, require_file=True)
        try:
            data = self._read_bounded_descriptor(descriptor, path=path)
        finally:
            os.close(descriptor)
        if b"\x00" in data:
            raise _workspace_error(
                "workspace_binary_file",
                "binary workspace files cannot be returned as model text",
                path=path,
            )
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as error:
            raise _workspace_error(
                "workspace_encoding_error",
                "workspace text files must use UTF-8 encoding",
                path=path,
            ) from error

        sha256 = hashlib.sha256(data).hexdigest()
        if not text:
            result = ReadResult(
                path=canonical,
                content="",
                sha256=sha256,
                start_line=1,
                end_line=0,
                total_lines=0,
                truncated=False,
            )
            self._require_result_fits(result, result_byte_limit)
            return result

        total_lines = sum(1 for _ in _line_spans(text))
        if start_line > total_lines:
            raise _workspace_error(
                "workspace_line_out_of_range",
                "the requested start line is beyond the end of the file",
                path=canonical,
                details={"start_line": start_line, "total_lines": total_lines},
            )
        selected_last = min(end_line or total_lines, total_lines)
        content = io.StringIO()
        content_json_bytes = 0
        returned_end = start_line - 1
        for line_number, (offset, next_offset) in enumerate(_line_spans(text), start=1):
            if line_number >= start_line:
                if line_number > selected_last:
                    break
                line = text[offset:next_offset]
                line_json_bytes = _json_string_content_size(line)
                candidate_end = line_number
                candidate_truncated = candidate_end < total_lines
                candidate_next = candidate_end + 1 if candidate_truncated else None
                candidate_size = _read_result_size(
                    path=canonical,
                    content_json_bytes=content_json_bytes + line_json_bytes,
                    sha256=sha256,
                    start_line=start_line,
                    end_line=candidate_end,
                    total_lines=total_lines,
                    truncated=candidate_truncated,
                    next_start_line=candidate_next,
                )
                if candidate_size > result_byte_limit:
                    if returned_end < start_line:
                        raise _workspace_error(
                            "workspace_line_too_large",
                            "the first requested line cannot fit in the result budget",
                            path=canonical,
                            details={
                                "line": start_line,
                                "limit_bytes": result_byte_limit,
                            },
                        )
                    break
                content.write(line)
                content_json_bytes += line_json_bytes
                returned_end = line_number

        if returned_end < start_line:
            raise _workspace_error(
                "tool_result_limit",
                "the configured result limit cannot contain file metadata",
                details={"limit_bytes": result_byte_limit},
            )
        truncated = returned_end < total_lines
        result = ReadResult(
            path=canonical,
            content=content.getvalue(),
            sha256=sha256,
            start_line=start_line,
            end_line=returned_end,
            total_lines=total_lines,
            truncated=truncated,
            next_start_line=returned_end + 1 if truncated else None,
        )
        self._require_result_fits(result, result_byte_limit)
        return result

    async def search(  # noqa: PLR0912 - process and protocol outcomes are explicit
        self,
        *,
        query: str,
        path: str,
        globs: Sequence[str],
        regex: bool,
        case_sensitive: bool,
        max_results: int,
        timeout_seconds: float,
        output_byte_limit: int,
    ) -> tuple[str, tuple[SearchMatch, ...], bool, bool]:
        if max_results <= 0 or timeout_seconds <= 0 or output_byte_limit <= 0:
            raise ValueError("search limits must be positive")
        target = self.resolve_path(path)
        if not target.is_dir():
            raise _workspace_error(
                "workspace_not_directory",
                "the requested search path is not a directory",
                path=path,
            )
        canonical_target = target.relative_to(self._root).as_posix()
        if canonical_target == ".":
            canonical_target = "."
        argv = [
            self._ripgrep_path,
            "--json",
            "--hidden",
            "--no-messages",
            "--no-config",
            "--no-follow",
            "--max-filesize",
            str(self._max_file_bytes),
            "--max-columns",
            str(_MAX_SEARCH_LINE_BYTES),
            "--max-columns-preview",
        ]
        if not regex:
            argv.append("--fixed-strings")
        if not case_sensitive:
            argv.append("--ignore-case")
        for pattern in globs:
            argv.extend(("--glob", pattern))
        for pattern in self._access_policy.ripgrep_exclusion_globs():
            argv.extend(("--iglob", pattern))
        argv.extend(("--regexp", query, "--", canonical_target))
        try:
            result = await self._search_runner.run(
                argv,
                cwd=self._root,
                timeout_seconds=timeout_seconds,
                max_output_bytes=min(output_byte_limit, self._max_search_output_bytes),
                environment={"LANG": "C", "LC_ALL": "C"},
            )
        except DomainOperationError as error:
            if error.code == "command_start_failed":
                raise DomainOperationError(
                    code="search_unavailable",
                    message="workspace search could not be started",
                ) from error
            raise
        if result.timed_out:
            raise DomainOperationError(
                code="search_timeout",
                message="workspace search exceeded its timeout",
            )
        if result.exit_code not in {0, 1} and not result.output_truncated:
            raise DomainOperationError(
                code="search_failed",
                message="workspace search failed",
            )
        raw = "".join(chunk.text for chunk in result.chunks if chunk.channel.value == "stdout")
        matches: list[SearchMatch] = []
        # The process is always given protected-path exclusion globs. This flag
        # reports that the result is policy-filtered even when ripgrep cannot tell
        # whether an excluded path contained a match.
        protected_omitted = True
        stream = io.StringIO(raw)
        for line in stream:
            try:
                event = json.loads(line)
            except json.JSONDecodeError as error:
                incomplete_truncated_tail = (
                    result.output_truncated
                    and stream.tell() == len(raw)
                    and not line.endswith(("\n", "\r"))
                )
                if incomplete_truncated_tail:
                    break
                raise self._search_protocol_error() from error
            normalized = self._parse_search_match(
                event,
                search_root=target,
            )
            if normalized is None:
                continue
            if isinstance(normalized, bool):
                protected_omitted = True
                continue
            matches.append(normalized)
            if len(matches) > max_results:
                break
        return (
            canonical_target,
            tuple(matches[:max_results]),
            result.output_truncated or len(matches) > max_results,
            protected_omitted,
        )

    def write_file_atomic(
        self,
        path: str,
        content: bytes,
        *,
        mode: int | None = None,
        require_absent: bool = False,
    ) -> None:
        if mode is not None and (type(mode) is not int or mode < 0 or mode > _MAX_PERMISSION_MODE):
            raise ValueError("mode must be a valid permission mode")
        with self._mutation_lock:
            parent_fd, name, canonical = self._open_write_parent(path)
            try:
                existing = self._read_existing_for_write(parent_fd, name, path=canonical)
                if require_absent and existing is not None:
                    raise _workspace_error(
                        "edit_create_conflict",
                        "the new-file destination already exists",
                        path=canonical,
                    )
                existing_mode = stat.S_IMODE(existing[1].st_mode) if existing is not None else mode
                staged_name = self._stage_from_parent_descriptor(
                    parent_fd,
                    content,
                    path=canonical,
                    mode=existing_mode,
                )
                cleanup_name: str | None = staged_name
                try:
                    self._install_staged_file(
                        parent_fd,
                        staged_name,
                        name,
                        path=canonical,
                        require_absent=require_absent,
                    )
                    cleanup_name = None
                finally:
                    self._remove_staged_file(parent_fd, cleanup_name)
            finally:
                os.close(parent_fd)

    def edit_text_transaction(  # noqa: PLR0912, PLR0915 - one auditable transaction
        self,
        path: str,
        *,
        expected_sha256: str | None,
        old_text: str | None,
        new_text: str,
        replace_all: bool,
        max_edit_bytes: int,
    ) -> EditTransaction:
        """Apply one hash-checked text edit without an internal check/write race."""

        if type(max_edit_bytes) is not int or max_edit_bytes <= 0:
            raise ValueError("max_edit_bytes must be a positive integer")
        with self._mutation_lock:
            parent_fd, name, canonical = self._open_write_parent(path)
            try:
                existing = self._read_existing_for_write(parent_fd, name, path=canonical)
                if existing is None:
                    if expected_sha256 is not None:
                        raise _workspace_error(
                            "edit_file_missing",
                            "the hash-checked edit target no longer exists",
                            path=canonical,
                        )
                    if old_text is not None or replace_all:
                        raise _workspace_error(
                            "malformed_tool_arguments",
                            "new-file creation may only supply path and new_text",
                            path=canonical,
                        )
                    previous = None
                    previous_stat = None
                    replacement_count = 0
                    updated = new_text.encode("utf-8")
                else:
                    if expected_sha256 is None:
                        raise _workspace_error(
                            "edit_hash_required",
                            "existing-file edits require the hash returned by read_file",
                            path=canonical,
                        )
                    previous, previous_stat = existing
                    actual_sha256 = hashlib.sha256(previous).hexdigest()
                    if actual_sha256 != expected_sha256:
                        raise _workspace_error(
                            "edit_hash_conflict",
                            "the workspace file changed after it was read",
                            path=canonical,
                            details={"actual_sha256": actual_sha256},
                        )
                    try:
                        previous_text = previous.decode("utf-8")
                    except UnicodeDecodeError as error:
                        raise _workspace_error(
                            "workspace_encoding_error",
                            "workspace text files must use UTF-8 encoding",
                            path=canonical,
                        ) from error
                    if not old_text:
                        raise _workspace_error(
                            "malformed_tool_arguments",
                            "existing-file edits require old_text",
                            path=canonical,
                        )
                    occurrences = previous_text.count(old_text)
                    if occurrences == 0:
                        raise _workspace_error(
                            "edit_text_not_found",
                            "the requested old text was not found",
                            path=canonical,
                        )
                    if occurrences > 1 and not replace_all:
                        raise _workspace_error(
                            "edit_text_ambiguous",
                            "the requested old text occurs more than once",
                            path=canonical,
                            details={"occurrences": occurrences},
                        )
                    replacement_count = occurrences if replace_all else 1
                    updated = previous_text.replace(
                        old_text,
                        new_text,
                        -1 if replace_all else 1,
                    ).encode("utf-8")

                if len(updated) > max_edit_bytes:
                    raise _workspace_error(
                        "edit_size_limit",
                        "the edited file exceeds its configured byte limit",
                        path=canonical,
                        details={"limit_bytes": max_edit_bytes},
                    )
                previous_sha256 = (
                    hashlib.sha256(previous).hexdigest() if previous is not None else None
                )
                new_sha256 = hashlib.sha256(updated).hexdigest()
                patch_sha256 = self._edit_patch_identity(
                    path=canonical,
                    previous_sha256=previous_sha256,
                    new_sha256=new_sha256,
                )

                staged_name = self._stage_from_parent_descriptor(
                    parent_fd,
                    updated,
                    path=canonical,
                    mode=(
                        stat.S_IMODE(previous_stat.st_mode) if previous_stat is not None else None
                    ),
                )
                cleanup_name: str | None = staged_name
                try:
                    if previous is not None and previous_stat is not None:
                        current = self._read_existing_for_write(
                            parent_fd,
                            name,
                            path=canonical,
                        )
                        if current is None:
                            raise _workspace_error(
                                "edit_hash_conflict",
                                "the workspace file changed after it was read",
                                path=canonical,
                            )
                        current_bytes, current_stat = current
                        current_identity = (current_stat.st_dev, current_stat.st_ino)
                        previous_identity = (previous_stat.st_dev, previous_stat.st_ino)
                        current_sha256 = hashlib.sha256(current_bytes).hexdigest()
                        if (
                            current_identity != previous_identity
                            or current_sha256 != previous_sha256
                        ):
                            raise _workspace_error(
                                "edit_hash_conflict",
                                "the workspace file changed after it was read",
                                path=canonical,
                                details={"actual_sha256": current_sha256},
                            )
                    self._install_staged_file(
                        parent_fd,
                        staged_name,
                        name,
                        path=canonical,
                        require_absent=previous is None,
                    )
                    cleanup_name = None
                finally:
                    self._remove_staged_file(parent_fd, cleanup_name)
                return EditTransaction(
                    path=canonical,
                    created=previous is None,
                    previous_sha256=previous_sha256,
                    sha256=new_sha256,
                    replacement_count=replacement_count,
                    patch_sha256=patch_sha256,
                    bytes_written=len(updated),
                )
            finally:
                os.close(parent_fd)

    def _open_write_parent(self, path: str) -> tuple[int, str, str]:
        target = self.resolve_path(path, must_exist=False, for_write=True)
        canonical = target.relative_to(self._root).as_posix()
        parts = PurePosixPath(canonical).parts
        if not parts or canonical == ".":
            raise _workspace_error(
                "workspace_not_file",
                "the workspace root cannot be replaced as a file",
                path=path,
            )
        current_fd = os.dup(self._root_fd)
        try:
            for component in parts[:-1]:
                next_fd = os.open(component, _DIRECTORY_OPEN_FLAGS, dir_fd=current_fd)
                os.close(current_fd)
                current_fd = next_fd
            return current_fd, parts[-1], canonical
        except FileNotFoundError as error:
            raise _workspace_error(
                "workspace_parent_missing",
                "the destination parent directory does not exist",
                path=path,
            ) from error
        except OSError as error:
            raise _workspace_error(
                "workspace_path_invalid",
                "the destination parent changed while it was being opened",
                path=path,
            ) from error

    def _read_existing_for_write(
        self,
        parent_fd: int,
        name: str,
        *,
        path: str,
    ) -> tuple[bytes, os.stat_result] | None:
        try:
            descriptor = os.open(name, _FILE_OPEN_FLAGS, dir_fd=parent_fd)
        except FileNotFoundError:
            return None
        except OSError as error:
            raise _workspace_error(
                "workspace_path_invalid",
                "the destination changed while it was being opened",
                path=path,
            ) from error
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise _workspace_error(
                    "workspace_not_file",
                    "only regular workspace files can be replaced",
                    path=path,
                )
            return self._read_bounded_descriptor(descriptor, path=path), metadata
        finally:
            os.close(descriptor)

    @staticmethod
    def _edit_patch_identity(
        *,
        path: str,
        previous_sha256: str | None,
        new_sha256: str,
    ) -> str:
        digest = hashlib.sha256()
        for component in (
            b"agent-edit-v1",
            path.encode("utf-8"),
            (previous_sha256 or "create").encode("ascii"),
            new_sha256.encode("ascii"),
        ):
            digest.update(len(component).to_bytes(8, "big"))
            digest.update(component)
        return digest.hexdigest()

    @staticmethod
    def _write_all(descriptor: int, content: bytes) -> None:
        view = memoryview(content)
        written = 0
        while written < len(view):
            count = os.write(descriptor, view[written:])
            if count <= 0:
                raise OSError("short workspace write")
            written += count

    def _stage_from_parent_descriptor(
        self,
        parent_fd: int,
        content: bytes,
        *,
        path: str,
        mode: int | None,
    ) -> str:
        temporary_name = f".agent-edit-{secrets.token_hex(16)}"
        temporary_fd = -1
        temporary_exists = False
        try:
            temporary_fd = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=parent_fd,
            )
            temporary_exists = True
            self._write_all(temporary_fd, content)
            if mode is not None:
                os.fchmod(temporary_fd, mode)
            os.fsync(temporary_fd)
            os.close(temporary_fd)
            temporary_fd = -1
            temporary_exists = False
        except DomainOperationError:
            raise
        except OSError as error:
            raise _workspace_error(
                "workspace_write_failed",
                "the workspace file could not be written atomically",
                path=path,
            ) from error
        finally:
            if temporary_fd >= 0:
                with suppress(OSError):
                    os.close(temporary_fd)
            if temporary_exists:
                with suppress(OSError):
                    os.unlink(temporary_name, dir_fd=parent_fd)
        return temporary_name

    @staticmethod
    def _install_staged_file(
        parent_fd: int,
        temporary_name: str,
        name: str,
        *,
        path: str,
        require_absent: bool,
    ) -> None:
        backup_name: str | None = None
        installed = False
        try:
            if require_absent:
                try:
                    os.link(
                        temporary_name,
                        name,
                        src_dir_fd=parent_fd,
                        dst_dir_fd=parent_fd,
                        follow_symlinks=False,
                    )
                except FileExistsError as error:
                    raise _workspace_error(
                        "edit_create_conflict",
                        "the new-file destination already exists",
                        path=path,
                    ) from error
                installed = True
                os.unlink(temporary_name, dir_fd=parent_fd)
            else:
                backup_name = f".agent-backup-{secrets.token_hex(16)}"
                os.link(
                    name,
                    backup_name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                os.replace(
                    temporary_name,
                    name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
                installed = True
            os.fsync(parent_fd)
            if backup_name is not None:
                os.unlink(backup_name, dir_fd=parent_fd)
                backup_name = None
        except DomainOperationError:
            RootedWorkspace._rollback_staged_install(
                parent_fd,
                name,
                backup_name=backup_name,
                remove_created=require_absent and installed,
            )
            raise
        except OSError as error:
            RootedWorkspace._rollback_staged_install(
                parent_fd,
                name,
                backup_name=backup_name,
                remove_created=require_absent and installed,
            )
            raise _workspace_error(
                "workspace_write_failed",
                "the workspace file could not be written atomically",
                path=path,
            ) from error
        finally:
            if backup_name is not None:
                with suppress(OSError):
                    os.unlink(backup_name, dir_fd=parent_fd)

    @staticmethod
    def _rollback_staged_install(
        parent_fd: int,
        name: str,
        *,
        backup_name: str | None,
        remove_created: bool,
    ) -> None:
        with suppress(OSError):
            if backup_name is not None:
                os.replace(
                    backup_name,
                    name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
            elif remove_created:
                os.unlink(name, dir_fd=parent_fd)
            os.fsync(parent_fd)

    @staticmethod
    def _remove_staged_file(parent_fd: int, temporary_name: str | None) -> None:
        if temporary_name is not None:
            with suppress(OSError):
                os.unlink(temporary_name, dir_fd=parent_fd)

    def file_bytes(self, path: str) -> bytes:
        descriptor, _ = self._open_path(path, require_file=True)
        try:
            return self._read_bounded_descriptor(descriptor, path=path)
        finally:
            os.close(descriptor)

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            if not self._root_fd_closed:
                os.close(self._root_fd)
                self._root_fd_closed = True
            if not self._search_runner_closed:
                await self._search_runner.close()
                self._search_runner_closed = True
            self._closed = True

    async def __aenter__(self) -> RootedWorkspace:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        await self.close()

    def _open_path(
        self,
        value: str,
        *,
        require_directory: bool = False,
        require_file: bool = False,
    ) -> tuple[int, str]:
        target = self.resolve_path(value)
        canonical = target.relative_to(self._root).as_posix()
        parts = () if canonical == "." else PurePosixPath(canonical).parts
        current_fd = os.dup(self._root_fd)
        try:
            if not parts:
                descriptor = current_fd
                current_fd = -1
            else:
                for component in parts[:-1]:
                    next_fd = os.open(
                        component,
                        _DIRECTORY_OPEN_FLAGS,
                        dir_fd=current_fd,
                    )
                    os.close(current_fd)
                    current_fd = next_fd
                flags = _DIRECTORY_OPEN_FLAGS if require_directory else _FILE_OPEN_FLAGS
                descriptor = os.open(parts[-1], flags, dir_fd=current_fd)
            mode = os.fstat(descriptor).st_mode
            if require_directory and not stat.S_ISDIR(mode):
                raise _workspace_error(
                    "workspace_not_directory",
                    "the requested workspace path is not a directory",
                    path=value,
                )
            if require_file and not stat.S_ISREG(mode):
                raise _workspace_error(
                    "workspace_not_file",
                    "the requested workspace path is not a regular file",
                    path=value,
                )
            return descriptor, canonical  # noqa: TRY300 - descriptor lifetime is local
        except DomainOperationError:
            if "descriptor" in locals():
                os.close(descriptor)
            raise
        except OSError as error:
            raise _workspace_error(
                "workspace_path_invalid",
                "the requested workspace path changed while it was being opened",
                path=value,
            ) from error
        finally:
            if current_fd >= 0:
                os.close(current_fd)

    def _read_bounded_descriptor(self, descriptor: int, *, path: str) -> bytes:
        remaining = self._max_file_bytes + 1
        chunks: list[bytes] = []
        try:
            while remaining:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
        except OSError as error:
            raise _workspace_error(
                "workspace_read_failed",
                "the workspace file could not be read",
                path=path,
            ) from error
        data = b"".join(chunks)
        if len(data) > self._max_file_bytes:
            raise _workspace_error(
                "workspace_file_too_large",
                "the workspace file exceeds the configured size limit",
                path=path,
            )
        return data

    def _reject_write_symlinks(self, candidate: Path) -> None:
        current = self._root
        for component in candidate.relative_to(self._root).parts:
            current /= component
            if current.is_symlink():
                raise _workspace_error(
                    "workspace_symlink_rejected",
                    "workspace writes may not traverse symlinks",
                    path=candidate.relative_to(self._root).as_posix(),
                )
            if not current.exists():
                break

    def _parse_search_match(  # noqa: PLR0912 - fail-closed protocol validation
        self,
        event: Any,
        *,
        search_root: Path,
    ) -> SearchMatch | bool | None:
        if not isinstance(event, dict):
            raise self._search_protocol_error()
        event_type = event.get("type")
        if event_type in _SEARCH_EVENT_TYPES:
            return None
        if event_type != "match":
            raise self._search_protocol_error()
        data = event.get("data")
        if not isinstance(data, dict):
            raise self._search_protocol_error()
        path_data = data.get("path")
        lines_data = data.get("lines")
        line_number = data.get("line_number")
        submatches = data.get("submatches")
        if (
            not isinstance(path_data, dict)
            or not isinstance(lines_data, dict)
            or not isinstance(path_data.get("text"), str)
            or not isinstance(lines_data.get("text"), str)
            or isinstance(line_number, bool)
            or not isinstance(line_number, int)
            or line_number <= 0
            or not isinstance(submatches, list)
            or not submatches
            or not isinstance(submatches[0], dict)
        ):
            raise self._search_protocol_error()
        byte_offset = submatches[0].get("start")
        if isinstance(byte_offset, bool) or not isinstance(byte_offset, int) or byte_offset < 0:
            raise self._search_protocol_error()

        raw_path = path_data["text"]
        try:
            normalized_path = normalize_workspace_path(raw_path)
        except DomainOperationError as error:
            raise self._search_protocol_error() from error
        if self._access_policy.is_protected(normalized_path):
            return False
        try:
            resolved = self.resolve_path(normalized_path.as_posix())
        except DomainOperationError as error:
            raise self._search_protocol_error() from error
        if not resolved.is_relative_to(search_root):
            raise self._search_protocol_error()
        canonical = resolved.relative_to(self._root).as_posix()
        if self._access_policy.is_protected(PurePosixPath(canonical)):
            return False

        line_text = lines_data["text"]
        encoded = line_text.encode("utf-8")
        if byte_offset > len(encoded):
            raise self._search_protocol_error()
        try:
            prefix = encoded[:byte_offset].decode("utf-8")
        except UnicodeDecodeError as error:
            raise self._search_protocol_error() from error
        preview = _bounded_utf8_prefix(line_text.rstrip("\r\n"), _MAX_SEARCH_LINE_BYTES)
        return SearchMatch(
            path=canonical,
            line=line_number,
            column=len(prefix) + 1,
            text=preview,
        )

    @staticmethod
    def _search_protocol_error() -> DomainOperationError:
        return DomainOperationError(
            code="search_protocol_error",
            message="workspace search returned an invalid response",
        )

    @staticmethod
    def _require_result_fits(result: ReadResult, limit: int) -> None:
        if _serialized_size(result.model_dump(mode="json")) > limit:
            raise DomainOperationError(
                code="tool_result_limit",
                message="the configured result limit cannot contain file metadata",
                details={"limit_bytes": limit},
            )


__all__ = [
    "CanonicalWorkspacePath",
    "ReadResult",
    "RootedWorkspace",
    "SearchMatch",
    "WorkspaceEntry",
    "WorkspaceEntryType",
]
