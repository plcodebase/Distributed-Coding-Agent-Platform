"""Typed coding tools backed by one contained workspace."""

from __future__ import annotations

import asyncio
import json
import math
from typing import TYPE_CHECKING, Annotated, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError
from agent_core.sandbox import CommandCompleted, CommandOutput, CommandSpec
from agent_core.tools import (
    RegisteredTool,
    ToolArguments,
    ToolEffect,
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolExecutionEvent,
    ToolOutputChunk,
    ToolRegistry,
)
from sandbox_runtime.workspace import (  # noqa: TC001 - Pydantic resolves result types
    CanonicalWorkspacePath,
    RootedWorkspace,
    SearchMatch,
    WorkspaceEntry,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agent_core.domain.base import JsonObject
    from agent_core.tools import ToolRegistration
    from sandbox_runtime.local import LocalSandbox

type WorkspacePath = Annotated[str, StringConstraints(min_length=1, max_length=4096)]
type BoundedText = Annotated[str, StringConstraints(max_length=4 * 1024 * 1024)]
type FileHash = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]
_MAX_LINE_NUMBER = 10_000_000
_MAX_TOOL_TIMEOUT_SECONDS = 3600


class ListFilesArguments(ToolArguments):
    """Arguments for a bounded, deterministic workspace listing."""

    path: WorkspacePath = "."
    max_depth: int = Field(default=2, ge=0, le=20)
    max_entries: int = Field(default=500, ge=1, le=2000)


class ReadFileArguments(ToolArguments):
    """Arguments for a bounded UTF-8 workspace file read."""

    path: WorkspacePath
    start_line: int = Field(default=1, ge=1, le=_MAX_LINE_NUMBER)
    end_line: int | None = Field(default=None, ge=1, le=_MAX_LINE_NUMBER)

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.end_line is not None and self.end_line < self.start_line:
            raise ValueError("end_line must be greater than or equal to start_line")
        return self


class SearchFilesArguments(ToolArguments):
    """Arguments for fixed-string or regular-expression workspace search."""

    query: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    path: WorkspacePath = "."
    globs: tuple[
        Annotated[str, StringConstraints(min_length=1, max_length=1024)],
        ...,
    ] = Field(default=(), max_length=32)
    regex: bool = False
    case_sensitive: bool = False
    max_results: int = Field(default=200, ge=1, le=1000)


class EditFileArguments(ToolArguments):
    """Arguments for an optimistic, atomic single-file replacement."""

    path: WorkspacePath
    expected_sha256: FileHash | None = None
    old_text: BoundedText | None = None
    new_text: BoundedText
    replace_all: bool = False

    @model_validator(mode="after")
    def validate_edit_mode(self) -> Self:
        if self.expected_sha256 is None and self.old_text is not None:
            raise ValueError("old_text requires expected_sha256")
        if self.expected_sha256 is None and self.replace_all:
            raise ValueError("replace_all is invalid for new-file creation")
        if self.expected_sha256 is not None and not self.old_text:
            raise ValueError("existing-file edits require non-empty old_text")
        return self


class RunCommandArguments(ToolArguments):
    """Arguments for an argv-only command in an explicitly enabled sandbox."""

    argv: tuple[
        Annotated[str, StringConstraints(min_length=1, max_length=16_384)],
        ...,
    ] = Field(min_length=1, max_length=256)
    cwd: WorkspacePath = "."
    timeout_seconds: float | None = Field(default=None, gt=0, le=3600)


class ListFilesResult(DomainModel):
    """Validated wire result for one bounded workspace listing."""

    path: CanonicalWorkspacePath
    entries: tuple[WorkspaceEntry, ...] = ()
    truncated: bool = False
    protected_entries_omitted: bool = False

    @model_validator(mode="after")
    def validate_entries(self) -> Self:
        paths = tuple(entry.path for entry in self.entries)
        if paths != tuple(sorted(set(paths))):
            raise ValueError("workspace entries must be unique and path-sorted")
        return self


class SearchFilesResult(DomainModel):
    """Validated wire result for one bounded workspace search."""

    query: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    matches: tuple[SearchMatch, ...] = Field(default=(), max_length=1000)
    truncated: bool = False
    protected_entries_omitted: bool = False


class EditFileResult(DomainModel):
    """Closed result contract for a completed optimistic text edit."""

    path: CanonicalWorkspacePath
    created: bool
    previous_sha256: FileHash | None
    sha256: FileHash
    replacement_count: int = Field(ge=0)
    patch_sha256: FileHash
    bytes_written: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_edit_result(self) -> Self:
        if self.created:
            if self.previous_sha256 is not None or self.replacement_count != 0:
                raise ValueError("created-file result metadata is inconsistent")
        elif self.previous_sha256 is None or self.replacement_count < 1:
            raise ValueError("existing-file result metadata is inconsistent")
        return self


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


def _bounded_result(value: JsonObject, limit: int) -> ToolExecutionCompleted:
    size = _serialized_size(value)
    if size > limit:
        raise DomainOperationError(
            code="tool_result_limit",
            message="the tool result exceeds its configured byte limit",
            details={"limit_bytes": limit},
        )
    return ToolExecutionCompleted(result=value)


def _fit_result_items[ResultItem: DomainModel](
    *,
    base: JsonObject,
    field: str,
    items: tuple[ResultItem, ...],
    limit: int,
) -> tuple[tuple[ResultItem, ...], bool]:
    """Fit a validated model sequence with one serialization per item."""

    empty = dict(base)
    empty[field] = []
    retained_size = _serialized_size(empty)
    retained: list[ResultItem] = []
    for item in items:
        item_json = item.model_dump(mode="json")
        item_size = _serialized_size(item_json)
        candidate_size = retained_size + item_size + (1 if retained else 0)
        if candidate_size > limit:
            return tuple(retained), True
        retained.append(item)
        retained_size = candidate_size
    return tuple(retained), False


class WorkspaceToolset:
    """Build the platform's typed read, edit, search, and command registrations."""

    def __init__(
        self,
        workspace: RootedWorkspace,
        *,
        sandbox: LocalSandbox | None = None,
        search_timeout_seconds: float = 10,
        default_command_timeout_seconds: float = 30,
        max_edit_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        for name, value in (
            ("search_timeout_seconds", search_timeout_seconds),
            ("default_command_timeout_seconds", default_command_timeout_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be a positive finite number")
        if type(max_edit_bytes) is not int or max_edit_bytes <= 0:
            raise ValueError("max_edit_bytes must be a positive integer")
        if (
            search_timeout_seconds > _MAX_TOOL_TIMEOUT_SECONDS
            or default_command_timeout_seconds > _MAX_TOOL_TIMEOUT_SECONDS
        ):
            raise ValueError("tool timeouts may not exceed 3600 seconds")
        self._workspace = workspace
        self._sandbox = sandbox
        self._search_timeout_seconds = search_timeout_seconds
        self._default_command_timeout_seconds = default_command_timeout_seconds
        self._max_edit_bytes = max_edit_bytes

    def registry(
        self,
        *,
        include_edit: bool = False,
        include_command: bool = False,
    ) -> ToolRegistry:
        """Return a read-only registry unless mutation capabilities are explicit."""

        registrations: list[ToolRegistration] = [
            RegisteredTool(
                name="list_files",
                description="List contained workspace files and directories.",
                arguments_type=ListFilesArguments,
                handler=self.list_files,
                effect=ToolEffect.READ_ONLY,
            ),
            RegisteredTool(
                name="read_file",
                description="Read a bounded line range from one UTF-8 workspace file.",
                arguments_type=ReadFileArguments,
                handler=self.read_file,
                effect=ToolEffect.READ_ONLY,
            ),
            RegisteredTool(
                name="search_files",
                description="Search contained workspace text using ripgrep.",
                arguments_type=SearchFilesArguments,
                handler=self.search_files,
                effect=ToolEffect.READ_ONLY,
            ),
        ]
        if include_edit:
            registrations.append(
                RegisteredTool(
                    name="edit_file",
                    description="Atomically create or hash-check and replace one workspace file.",
                    arguments_type=EditFileArguments,
                    handler=self.edit_file,
                    effect=ToolEffect.WORKSPACE_MUTATION,
                )
            )
        if include_command:
            if self._sandbox is None:
                raise ValueError("command registration requires a sandbox")
            registrations.append(
                RegisteredTool(
                    name="run_command",
                    description="Run an argv-only command in the configured sandbox.",
                    arguments_type=RunCommandArguments,
                    handler=self.run_command,
                    effect=ToolEffect.COMMAND,
                )
            )
        return ToolRegistry(registrations)

    async def list_files(
        self,
        arguments: ListFilesArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        canonical_path, entries, workspace_truncated, protected_omitted = await asyncio.to_thread(
            self._workspace.list_entries,
            arguments.path,
            max_depth=arguments.max_depth,
            max_entries=arguments.max_entries,
        )
        base: JsonObject = {
            "path": canonical_path,
            "entries": [],
            "truncated": workspace_truncated,
            "protected_entries_omitted": protected_omitted,
        }
        retained, result_limit_truncated = await asyncio.to_thread(
            _fit_result_items,
            base=base,
            field="entries",
            items=entries,
            limit=context.max_result_bytes,
        )
        result = ListFilesResult(
            path=canonical_path,
            entries=retained,
            truncated=workspace_truncated or result_limit_truncated,
            protected_entries_omitted=protected_omitted,
        )
        yield _bounded_result(result.model_dump(mode="json"), context.max_result_bytes)

    async def read_file(
        self,
        arguments: ReadFileArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        read = await asyncio.to_thread(
            self._workspace.read_text,
            arguments.path,
            start_line=arguments.start_line,
            end_line=arguments.end_line,
            result_byte_limit=context.max_result_bytes,
        )
        yield _bounded_result(read.model_dump(mode="json"), context.max_result_bytes)

    async def search_files(
        self,
        arguments: SearchFilesArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        _, matches, workspace_truncated, protected_omitted = await self._workspace.search(
            query=arguments.query,
            path=arguments.path,
            globs=arguments.globs,
            regex=arguments.regex,
            case_sensitive=arguments.case_sensitive,
            max_results=arguments.max_results,
            timeout_seconds=self._search_timeout_seconds,
            output_byte_limit=min(max(context.max_result_bytes * 4, 4096), 8 * 1024 * 1024),
        )
        base: JsonObject = {
            "query": arguments.query,
            "matches": [],
            "truncated": workspace_truncated,
            "protected_entries_omitted": protected_omitted,
        }
        retained, result_limit_truncated = await asyncio.to_thread(
            _fit_result_items,
            base=base,
            field="matches",
            items=matches,
            limit=context.max_result_bytes,
        )
        result = SearchFilesResult(
            query=arguments.query,
            matches=retained,
            truncated=workspace_truncated or result_limit_truncated,
            protected_entries_omitted=protected_omitted,
        )
        yield _bounded_result(result.model_dump(mode="json"), context.max_result_bytes)

    async def edit_file(
        self,
        arguments: EditFileArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        edit = await asyncio.to_thread(
            self._workspace.edit_text_transaction,
            arguments.path,
            expected_sha256=arguments.expected_sha256,
            old_text=arguments.old_text,
            new_text=arguments.new_text,
            replace_all=arguments.replace_all,
            max_edit_bytes=self._max_edit_bytes,
        )
        result = EditFileResult(
            path=edit.path,
            created=edit.created,
            previous_sha256=edit.previous_sha256,
            sha256=edit.sha256,
            replacement_count=edit.replacement_count,
            patch_sha256=edit.patch_sha256,
            bytes_written=edit.bytes_written,
        )
        yield _bounded_result(result.model_dump(mode="json"), context.max_result_bytes)

    async def run_command(
        self,
        arguments: RunCommandArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        if self._sandbox is None:
            raise DomainOperationError(
                code="sandbox_unavailable",
                message="command execution is not configured",
            )
        command = CommandSpec(
            argv=arguments.argv,
            cwd=arguments.cwd,
            timeout_seconds=arguments.timeout_seconds or self._default_command_timeout_seconds,
            max_output_bytes=context.max_output_bytes,
        )
        terminal: CommandCompleted | None = None
        async for event in self._sandbox.execute(command):
            if isinstance(event, CommandOutput):
                yield ToolOutputChunk(channel=event.channel, chunk=event.chunk)
            else:
                terminal = event
        if terminal is None:
            raise DomainOperationError(
                code="command_protocol_error",
                message="the sandbox command stream ended without a terminal outcome",
            )
        if terminal.output_truncated:
            raise DomainOperationError(
                code="tool_output_limit",
                message="command output exceeded its configured byte limit",
                details={"limit_bytes": context.max_output_bytes},
            )
        if terminal.timed_out:
            raise DomainOperationError(
                code="tool_timeout",
                message="command execution exceeded its timeout",
            )
        if terminal.exit_code != 0:
            raise DomainOperationError(
                code="command_failed",
                message="command execution returned a non-zero exit code",
                details={"exit_code": terminal.exit_code},
            )
        yield _bounded_result(
            {"exit_code": terminal.exit_code},
            context.max_result_bytes,
        )


__all__ = [
    "EditFileArguments",
    "EditFileResult",
    "ListFilesArguments",
    "ListFilesResult",
    "ReadFileArguments",
    "RunCommandArguments",
    "SearchFilesArguments",
    "SearchFilesResult",
    "WorkspaceToolset",
]
