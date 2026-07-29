"""Rooted filesystem operations with containment and resource limits."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from agent_core.domain.errors import DomainOperationError
from sandbox_runtime._process import BoundedProcessRunner

if TYPE_CHECKING:
    from collections.abc import Sequence

    from agent_core.domain.base import JsonObject

_MAX_PATH_BYTES = 4096


@dataclass(frozen=True, slots=True)
class WorkspaceEntry:
    path: str
    type: str


@dataclass(frozen=True, slots=True)
class ReadResult:
    path: str
    content: str
    sha256: str
    start_line: int
    end_line: int
    total_lines: int
    truncated: bool


@dataclass(frozen=True, slots=True)
class SearchMatch:
    path: str
    line: int
    column: int
    text: str


def _workspace_error(code: str, message: str, *, path: str | None = None) -> DomainOperationError:
    details: JsonObject | None = {"path": path} if path is not None else None
    return DomainOperationError(code=code, message=message, details=details)


class RootedWorkspace:
    """Filesystem view that never intentionally addresses data outside its root."""

    def __init__(
        self,
        root: Path,
        *,
        max_file_bytes: int = 4 * 1024 * 1024,
        ripgrep_path: str = "rg",
        search_runner: BoundedProcessRunner | None = None,
    ) -> None:
        resolved = root.resolve(strict=True)
        if not resolved.is_dir():
            raise ValueError("workspace root must be a directory")
        self._root = resolved
        self._max_file_bytes = max_file_bytes
        self._ripgrep_path = ripgrep_path
        self._search_runner = search_runner or BoundedProcessRunner()
        self._owns_search_runner = search_runner is None

    @property
    def root(self) -> Path:
        return self._root

    def resolve_path(
        self,
        value: str,
        *,
        must_exist: bool = True,
        for_write: bool = False,
    ) -> Path:
        relative = self._validate_relative_path(value)
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
        except OSError as error:
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
        return resolved

    def list_entries(
        self,
        path: str,
        *,
        max_depth: int,
        max_entries: int,
    ) -> tuple[tuple[WorkspaceEntry, ...], bool]:
        start = self.resolve_path(path)
        if not start.is_dir():
            raise _workspace_error(
                "workspace_not_directory",
                "the requested list path is not a directory",
                path=path,
            )
        entries: list[WorkspaceEntry] = []
        truncated = False

        def visit(directory: Path, depth: int) -> None:
            nonlocal truncated
            if truncated:
                return
            try:
                with os.scandir(directory) as iterator:
                    children = sorted(iterator, key=lambda item: item.name)
            except OSError as error:
                raise _workspace_error(
                    "workspace_read_failed",
                    "the workspace directory could not be read",
                    path=directory.relative_to(self._root).as_posix(),
                ) from error
            for child in children:
                if child.name == ".git":
                    continue
                relative = Path(child.path).relative_to(self._root).as_posix()
                try:
                    if child.is_symlink():
                        target = Path(child.path).resolve(strict=True)
                        if not target.is_relative_to(self._root):
                            continue
                        entry_type = "symlink"
                        is_directory = False
                    elif child.is_dir(follow_symlinks=False):
                        entry_type = "directory"
                        is_directory = True
                    elif child.is_file(follow_symlinks=False):
                        entry_type = "file"
                        is_directory = False
                    else:
                        continue
                except OSError:
                    continue
                if len(entries) >= max_entries:
                    truncated = True
                    return
                entries.append(WorkspaceEntry(path=relative, type=entry_type))
                if is_directory and depth < max_depth:
                    visit(Path(child.path), depth + 1)

        visit(start, 0)
        return tuple(entries), truncated

    def read_text(
        self,
        path: str,
        *,
        start_line: int,
        end_line: int | None,
        content_byte_limit: int,
    ) -> ReadResult:
        target = self.resolve_path(path)
        if not target.is_file():
            raise _workspace_error(
                "workspace_not_file",
                "the requested workspace path is not a regular file",
                path=path,
            )
        data = self._read_bounded(target, path=path)
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
        lines = text.splitlines(keepends=True)
        total_lines = len(lines)
        selected_end = min(end_line or total_lines, total_lines)
        selected = "".join(lines[start_line - 1 : selected_end])
        encoded = selected.encode("utf-8")
        truncated = len(encoded) > content_byte_limit
        if truncated:
            selected = encoded[:content_byte_limit].decode("utf-8", errors="ignore")
        actual_end = start_line - 1 + selected.count("\n")
        if selected and not selected.endswith("\n"):
            actual_end += 1
        return ReadResult(
            path=path,
            content=selected,
            sha256=hashlib.sha256(data).hexdigest(),
            start_line=start_line,
            end_line=max(start_line - 1, min(actual_end, selected_end)),
            total_lines=total_lines,
            truncated=truncated or selected_end < total_lines,
        )

    async def search(
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
    ) -> tuple[tuple[SearchMatch, ...], bool]:
        target = self.resolve_path(path)
        argv = [
            self._ripgrep_path,
            "--json",
            "--hidden",
            "--no-messages",
        ]
        if not regex:
            argv.append("--fixed-strings")
        if not case_sensitive:
            argv.append("--ignore-case")
        for pattern in globs:
            argv.extend(("--glob", pattern))
        argv.extend(("--glob", "!.git/**"))
        argv.extend(("--regexp", query, "--", target.relative_to(self._root).as_posix()))
        result = await self._search_runner.run(
            argv,
            cwd=self._root,
            timeout_seconds=timeout_seconds,
            max_output_bytes=output_byte_limit,
        )
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
        for line in raw.splitlines():
            if len(matches) >= max_results:
                break
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            match = self._parse_search_match(event)
            if match is not None:
                matches.append(match)
        truncated = result.output_truncated or len(matches) >= max_results
        return tuple(matches), truncated

    def write_file_atomic(
        self,
        path: str,
        content: bytes,
        *,
        mode: int | None = None,
        require_absent: bool = False,
    ) -> None:
        target = self.resolve_path(path, must_exist=False, for_write=True)
        parent = target.parent
        if not parent.is_dir():
            raise _workspace_error(
                "workspace_parent_missing",
                "the destination parent directory does not exist",
                path=path,
            )
        existing_mode = mode
        if target.exists():
            current = target.stat()
            if not stat.S_ISREG(current.st_mode):
                raise _workspace_error(
                    "workspace_not_file",
                    "only regular workspace files can be replaced",
                    path=path,
                )
            existing_mode = stat.S_IMODE(current.st_mode)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=".agent-edit-",
                dir=parent,
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
            if existing_mode is not None:
                temporary_path.chmod(existing_mode)
            if require_absent:
                try:
                    os.link(temporary_path, target)
                except FileExistsError as error:
                    raise _workspace_error(
                        "edit_create_conflict",
                        "the new-file destination already exists",
                        path=path,
                    ) from error
                temporary_path.unlink()
                temporary_path = None
            else:
                temporary_path.replace(target)
                temporary_path = None
            directory_fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError as error:
            raise _workspace_error(
                "workspace_write_failed",
                "the workspace file could not be written atomically",
                path=path,
            ) from error
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def file_bytes(self, path: str) -> bytes:
        target = self.resolve_path(path)
        return self._read_bounded(target, path=path)

    async def close(self) -> None:
        if self._owns_search_runner:
            await self._search_runner.close()

    def _read_bounded(self, target: Path, *, path: str) -> bytes:
        try:
            with target.open("rb") as file:
                data = file.read(self._max_file_bytes + 1)
        except OSError as error:
            raise _workspace_error(
                "workspace_read_failed",
                "the workspace file could not be read",
                path=path,
            ) from error
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

    @staticmethod
    def _validate_relative_path(value: str) -> PurePosixPath:
        if not value or "\x00" in value or len(value.encode("utf-8")) > _MAX_PATH_BYTES:
            raise _workspace_error(
                "workspace_path_invalid",
                "workspace paths must be non-empty bounded text",
                path=value,
            )
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts or ".git" in path.parts:
            raise _workspace_error(
                "workspace_path_invalid",
                "workspace paths must remain relative and may not address repository metadata",
                path=value,
            )
        return path

    def _parse_search_match(self, event: Any) -> SearchMatch | None:
        if not isinstance(event, dict) or event.get("type") != "match":
            return None
        data = event.get("data")
        if not isinstance(data, dict):
            return None
        path_data = data.get("path")
        lines_data = data.get("lines")
        if not isinstance(path_data, dict) or not isinstance(lines_data, dict):
            return None
        path = path_data.get("text")
        lines = lines_data.get("text")
        line_number = data.get("line_number")
        submatches = data.get("submatches")
        if (
            not isinstance(path, str)
            or not isinstance(lines, str)
            or not isinstance(line_number, int)
            or not isinstance(submatches, list)
            or not submatches
            or not isinstance(submatches[0], dict)
            or not isinstance(submatches[0].get("start"), int)
        ):
            return None
        relative = Path(path)
        if relative.is_absolute():
            try:
                relative = relative.relative_to(self._root)
            except ValueError:
                return None
        return SearchMatch(
            path=relative.as_posix(),
            line=line_number,
            column=submatches[0]["start"] + 1,
            text=lines.rstrip("\r\n")[:4096],
        )


__all__ = [
    "ReadResult",
    "RootedWorkspace",
    "SearchMatch",
    "WorkspaceEntry",
]
