"""Typed coding tools backed by one contained workspace."""

from __future__ import annotations

import asyncio
import difflib
import hashlib
import json
from typing import TYPE_CHECKING, Annotated, Self

from pydantic import Field, StringConstraints, model_validator

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

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from pydantic import JsonValue

    from agent_core.domain.base import JsonObject
    from agent_core.tools import ToolRegistration
    from sandbox_runtime.local import LocalSandbox
    from sandbox_runtime.workspace import RootedWorkspace, SearchMatch

type WorkspacePath = Annotated[str, StringConstraints(min_length=1, max_length=4096)]
type BoundedText = Annotated[str, StringConstraints(max_length=4 * 1024 * 1024)]
type FileHash = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]


class ListFilesArguments(ToolArguments):
    """Arguments for a bounded, deterministic workspace listing."""

    path: WorkspacePath = "."
    max_depth: int = Field(default=2, ge=0, le=20)
    max_entries: int = Field(default=500, ge=1, le=2000)


class ReadFileArguments(ToolArguments):
    """Arguments for a bounded UTF-8 workspace file read."""

    path: WorkspacePath
    start_line: int = Field(default=1, ge=1)
    end_line: int | None = Field(default=None, ge=1)

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
        self._workspace = workspace
        self._sandbox = sandbox
        self._search_timeout_seconds = search_timeout_seconds
        self._default_command_timeout_seconds = default_command_timeout_seconds
        self._max_edit_bytes = max_edit_bytes

    def registry(self, *, include_command: bool = False) -> ToolRegistry:
        """Return an immutable registry; command execution must be requested explicitly."""

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
            RegisteredTool(
                name="edit_file",
                description="Atomically create or hash-check and replace one workspace file.",
                arguments_type=EditFileArguments,
                handler=self.edit_file,
                effect=ToolEffect.WORKSPACE_MUTATION,
            ),
        ]
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
        entries, workspace_truncated = await asyncio.to_thread(
            self._workspace.list_entries,
            arguments.path,
            max_depth=arguments.max_depth,
            max_entries=arguments.max_entries,
        )
        retained: list[JsonValue] = []
        result: JsonObject = {
            "path": arguments.path,
            "entries": retained,
            "truncated": workspace_truncated,
        }
        result_limit_truncated = False
        for entry in entries:
            candidate: JsonObject = {"path": entry.path, "type": entry.type}
            retained.append(candidate)
            if _serialized_size(result) > context.max_result_bytes:
                retained.pop()
                result_limit_truncated = True
                break
        result["truncated"] = workspace_truncated or result_limit_truncated
        yield _bounded_result(result, context.max_result_bytes)

    async def read_file(
        self,
        arguments: ReadFileArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        fixed_result: JsonObject = {
            "path": arguments.path,
            "content": "",
            "sha256": "0" * 64,
            "start_line": arguments.start_line,
            "end_line": arguments.start_line - 1,
            "total_lines": 0,
            "truncated": True,
        }
        overhead = _serialized_size(fixed_result)
        if overhead >= context.max_result_bytes:
            raise DomainOperationError(
                code="tool_result_limit",
                message="the configured result limit cannot contain file metadata",
                details={"limit_bytes": context.max_result_bytes},
            )
        read = await asyncio.to_thread(
            self._workspace.read_text,
            arguments.path,
            start_line=arguments.start_line,
            end_line=arguments.end_line,
            content_byte_limit=context.max_result_bytes - overhead,
        )
        result: JsonObject = {
            "path": read.path,
            "content": read.content,
            "sha256": read.sha256,
            "start_line": read.start_line,
            "end_line": read.end_line,
            "total_lines": read.total_lines,
            "truncated": read.truncated,
        }
        while _serialized_size(result) > context.max_result_bytes and result["content"]:
            content = str(result["content"]).encode("utf-8")
            result["content"] = content[: max(0, len(content) - 256)].decode(
                "utf-8",
                errors="ignore",
            )
            result["truncated"] = True
        yield _bounded_result(result, context.max_result_bytes)

    async def search_files(
        self,
        arguments: SearchFilesArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        matches, workspace_truncated = await self._workspace.search(
            query=arguments.query,
            path=arguments.path,
            globs=arguments.globs,
            regex=arguments.regex,
            case_sensitive=arguments.case_sensitive,
            max_results=arguments.max_results,
            timeout_seconds=self._search_timeout_seconds,
            output_byte_limit=min(max(context.max_result_bytes * 4, 4096), 8 * 1024 * 1024),
        )
        retained: list[JsonValue] = []
        result: JsonObject = {
            "query": arguments.query,
            "matches": retained,
            "truncated": workspace_truncated,
        }
        result_limit_truncated = False
        for match in matches:
            retained.append(self._search_match_json(match))
            if _serialized_size(result) > context.max_result_bytes:
                retained.pop()
                result_limit_truncated = True
                break
        result["truncated"] = workspace_truncated or result_limit_truncated
        yield _bounded_result(result, context.max_result_bytes)

    async def edit_file(
        self,
        arguments: EditFileArguments,
        context: ToolExecutionContext,
    ) -> AsyncGenerator[ToolExecutionEvent, None]:
        target = self._workspace.resolve_path(
            arguments.path,
            must_exist=False,
            for_write=True,
        )
        exists = target.exists()
        if exists and arguments.expected_sha256 is None:
            raise DomainOperationError(
                code="edit_hash_required",
                message="existing-file edits require the hash returned by read_file",
                details={"path": arguments.path},
            )
        if not exists and arguments.expected_sha256 is not None:
            raise DomainOperationError(
                code="edit_file_missing",
                message="the hash-checked edit target no longer exists",
                details={"path": arguments.path},
            )

        previous = b""
        if exists:
            previous = await asyncio.to_thread(self._workspace.file_bytes, arguments.path)
            actual_hash = hashlib.sha256(previous).hexdigest()
            if actual_hash != arguments.expected_sha256:
                raise DomainOperationError(
                    code="edit_hash_conflict",
                    message="the workspace file changed after it was read",
                    details={"path": arguments.path, "actual_sha256": actual_hash},
                )
            try:
                previous_text = previous.decode("utf-8")
            except UnicodeDecodeError as error:
                raise DomainOperationError(
                    code="workspace_encoding_error",
                    message="workspace text files must use UTF-8 encoding",
                    details={"path": arguments.path},
                ) from error
            old_text = arguments.old_text
            if old_text is None:
                raise DomainOperationError(
                    code="malformed_tool_arguments",
                    message="existing-file edits require old_text",
                )
            occurrences = previous_text.count(old_text)
            if occurrences == 0:
                raise DomainOperationError(
                    code="edit_text_not_found",
                    message="the requested old text was not found",
                    details={"path": arguments.path},
                )
            if occurrences > 1 and not arguments.replace_all:
                raise DomainOperationError(
                    code="edit_text_ambiguous",
                    message="the requested old text occurs more than once",
                    details={"path": arguments.path, "occurrences": occurrences},
                )
            replacement_count = occurrences if arguments.replace_all else 1
            updated_text = previous_text.replace(
                old_text,
                arguments.new_text,
                -1 if arguments.replace_all else 1,
            )
        else:
            replacement_count = 0
            updated_text = arguments.new_text

        updated = updated_text.encode("utf-8")
        if len(updated) > self._max_edit_bytes:
            raise DomainOperationError(
                code="edit_size_limit",
                message="the edited file exceeds its configured byte limit",
                details={"limit_bytes": self._max_edit_bytes},
            )
        patch = "".join(
            difflib.unified_diff(
                previous.decode("utf-8").splitlines(keepends=True) if previous else [],
                updated_text.splitlines(keepends=True),
                fromfile=f"a/{arguments.path}",
                tofile=f"b/{arguments.path}",
            )
        ).encode("utf-8")
        await asyncio.to_thread(
            self._workspace.write_file_atomic,
            arguments.path,
            updated,
            require_absent=not exists,
        )
        result: JsonObject = {
            "path": arguments.path,
            "created": not exists,
            "previous_sha256": hashlib.sha256(previous).hexdigest() if exists else None,
            "sha256": hashlib.sha256(updated).hexdigest(),
            "replacement_count": replacement_count,
            "patch_sha256": hashlib.sha256(patch).hexdigest(),
            "bytes_written": len(updated),
        }
        yield _bounded_result(result, context.max_result_bytes)

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

    @staticmethod
    def _search_match_json(match: SearchMatch) -> JsonObject:
        return {
            "path": match.path,
            "line": match.line,
            "column": match.column,
            "text": match.text,
        }


__all__ = [
    "EditFileArguments",
    "ListFilesArguments",
    "ReadFileArguments",
    "RunCommandArguments",
    "SearchFilesArguments",
    "WorkspaceToolset",
]
