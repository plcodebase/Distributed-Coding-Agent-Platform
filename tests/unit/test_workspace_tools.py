import asyncio
import hashlib
import json
import os
import shutil
import stat
import threading
from collections.abc import (
    Awaitable,
    Callable,
    Generator,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import contextmanager
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from agent_core.domain import DomainOperationError, FrozenJsonObject, JsonObject
from agent_core.tools import (
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolExecutionEvent,
    ToolOutputChannel,
)
from sandbox_runtime import (
    EditFileArguments,
    EditFileResult,
    ListFilesResult,
    ReadResult,
    RootedWorkspace,
    SearchFilesResult,
    SearchMatch,
    WorkspaceAccessPolicy,
    WorkspaceEntry,
    WorkspaceToolset,
)
from sandbox_runtime._process import BoundedProcessRunner, ProcessChunk, ProcessResult

RUN_ID = UUID("10000000-0000-0000-0000-000000000001")


def execution_context(
    *,
    result_limit: int = 256 * 1024,
    output_limit: int = 64 * 1024,
) -> ToolExecutionContext:
    return ToolExecutionContext(
        run_id=RUN_ID,
        tool_call_id="tool-call-1",
        max_output_bytes=output_limit,
        max_result_bytes=result_limit,
    )


async def invoke(
    toolset: WorkspaceToolset,
    name: str,
    arguments: JsonObject,
    *,
    context: ToolExecutionContext | None = None,
) -> list[ToolExecutionEvent]:
    prepared = toolset.registry(include_edit=name == "edit_file").prepare(
        name,
        FrozenJsonObject(arguments),
    )
    return [item async for item in prepared.stream(context or execution_context())]


def completed(events: Sequence[ToolExecutionEvent]) -> ToolExecutionCompleted:
    terminal = events[-1]
    assert isinstance(terminal, ToolExecutionCompleted)
    return terminal


@pytest.fixture
def rooted_workspace(tmp_path: Path) -> RootedWorkspace:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.py").write_text(
        "alpha = 1\nneedle = '🙂'\nomega = 3\n",
        encoding="utf-8",
    )
    (tmp_path / "README.md").write_text("project\n", encoding="utf-8")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("secret-metadata", encoding="utf-8")
    return RootedWorkspace(tmp_path, ripgrep_path=shutil.which("rg") or "rg")


async def test_list_read_and_search_are_typed_bounded_and_deterministic(
    rooted_workspace: RootedWorkspace,
) -> None:
    toolset = WorkspaceToolset(rooted_workspace)

    listed = await invoke(
        toolset,
        "list_files",
        {"path": ".", "max_depth": 2, "max_entries": 20},
    )
    list_result = completed(listed).result.to_json_object()
    assert list_result == {
        "path": ".",
        "entries": [
            {"path": "README.md", "type": "file"},
            {"path": "src", "type": "directory"},
            {"path": "src/main.py", "type": "file"},
        ],
        "truncated": False,
        "protected_entries_omitted": True,
    }

    read = await invoke(
        toolset,
        "read_file",
        {"path": "src/main.py", "start_line": 2, "end_line": 2},
    )
    read_result = completed(read).result.to_json_object()
    assert read_result["content"] == "needle = '🙂'\n"
    assert (
        read_result["sha256"]
        == hashlib.sha256(rooted_workspace.file_bytes("src/main.py")).hexdigest()
    )
    assert read_result["start_line"] == 2
    assert read_result["end_line"] == 2
    assert read_result["next_start_line"] == 3

    searched = await invoke(
        toolset,
        "search_files",
        {"query": "needle", "path": "src", "max_results": 5},
    )
    search_result = completed(searched).result.to_json_object()
    assert search_result["matches"] == [
        {
            "path": "src/main.py",
            "line": 2,
            "column": 1,
            "text": "needle = '🙂'",
        }
    ]
    assert search_result["truncated"] is False
    assert search_result["protected_entries_omitted"] is True

    metadata_search = await invoke(
        toolset,
        "search_files",
        {
            "query": "secret-metadata",
            "path": ".",
            "globs": [".git/**"],
            "max_results": 5,
        },
    )
    assert completed(metadata_search).result["matches"] == ()


@pytest.mark.security
async def test_workspace_rejects_traversal_metadata_and_symlink_escapes(
    rooted_workspace: RootedWorkspace,
    tmp_path: Path,
) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-outside.txt"
    outside.write_text("outside secret", encoding="utf-8")
    (rooted_workspace.root / "escape").symlink_to(outside)
    toolset = WorkspaceToolset(rooted_workspace)

    for path in ("../outside", "/etc/passwd", ".git/config", "src/../../outside"):
        with pytest.raises(DomainOperationError) as rejected:
            await invoke(toolset, "read_file", {"path": path})
        assert rejected.value.code == "workspace_path_invalid"

    with pytest.raises(DomainOperationError) as symlink:
        await invoke(toolset, "read_file", {"path": "escape"})
    assert symlink.value.code == "workspace_path_escape"

    listed = await invoke(
        toolset,
        "list_files",
        {"path": ".", "max_depth": 1, "max_entries": 20},
    )
    raw_entries = completed(listed).result.to_json_object()["entries"]
    assert isinstance(raw_entries, list)
    paths = {entry["path"] for entry in raw_entries if isinstance(entry, dict)}
    assert ".git" not in paths
    assert "escape" not in paths


async def test_read_and_list_results_respect_serialized_utf8_limit(
    rooted_workspace: RootedWorkspace,
) -> None:
    toolset = WorkspaceToolset(rooted_workspace)
    (rooted_workspace.root / "many.txt").write_text("🙂\n" * 1000, encoding="utf-8")

    read = await invoke(
        toolset,
        "read_file",
        {"path": "many.txt"},
        context=execution_context(result_limit=300),
    )
    read_json = completed(read).result.to_json_object()
    assert read_json["truncated"] is True
    assert isinstance(read_json["content"], str)
    assert read_json["content"].endswith("\n")
    assert isinstance(read_json["end_line"], int)
    assert read_json["next_start_line"] == read_json["end_line"] + 1
    assert (
        len(
            json.dumps(
                read_json,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        <= 300
    )

    listed = await invoke(
        toolset,
        "list_files",
        {"path": ".", "max_depth": 2, "max_entries": 20},
        context=execution_context(result_limit=100),
    )
    listed_json = completed(listed).result.to_json_object()
    assert listed_json["truncated"] is True
    assert len(json.dumps(listed_json, separators=(",", ":")).encode()) <= 100


async def test_edit_requires_read_hash_and_writes_atomically(
    rooted_workspace: RootedWorkspace,
) -> None:
    toolset = WorkspaceToolset(rooted_workspace)
    original = rooted_workspace.file_bytes("src/main.py")
    original_hash = hashlib.sha256(original).hexdigest()

    with pytest.raises(DomainOperationError) as missing_hash:
        await invoke(
            toolset,
            "edit_file",
            {"path": "src/main.py", "new_text": "replacement"},
        )
    assert missing_hash.value.code == "edit_hash_required"

    with pytest.raises(DomainOperationError) as stale:
        await invoke(
            toolset,
            "edit_file",
            {
                "path": "src/main.py",
                "expected_sha256": "0" * 64,
                "old_text": "alpha = 1",
                "new_text": "alpha = 2",
            },
        )
    assert stale.value.code == "edit_hash_conflict"
    assert rooted_workspace.file_bytes("src/main.py") == original

    edited = await invoke(
        toolset,
        "edit_file",
        {
            "path": "src/main.py",
            "expected_sha256": original_hash,
            "old_text": "alpha = 1",
            "new_text": "alpha = 2",
        },
    )
    result = completed(edited).result.to_json_object()
    assert result["created"] is False
    assert result["replacement_count"] == 1
    assert (
        result["sha256"] == hashlib.sha256(rooted_workspace.file_bytes("src/main.py")).hexdigest()
    )
    assert rooted_workspace.file_bytes("src/main.py").startswith(b"alpha = 2\n")
    assert not any(
        path.name.startswith(".agent-edit-") for path in rooted_workspace.root.rglob("*")
    )

    created = await invoke(
        toolset,
        "edit_file",
        {"path": "new.py", "new_text": "created = True\n"},
    )
    assert completed(created).result["created"] is True
    assert rooted_workspace.file_bytes("new.py") == b"created = True\n"


async def test_edit_transactions_serialize_same_hash_and_return_canonical_results(
    rooted_workspace: RootedWorkspace,
) -> None:
    toolset = WorkspaceToolset(rooted_workspace)
    original_hash = hashlib.sha256(rooted_workspace.file_bytes("src/main.py")).hexdigest()
    arguments: JsonObject = {
        "path": "src/./main.py",
        "expected_sha256": original_hash,
        "old_text": "alpha = 1",
        "new_text": "alpha = 2",
    }

    outcomes = await asyncio.gather(
        invoke(toolset, "edit_file", arguments),
        invoke(toolset, "edit_file", arguments),
        return_exceptions=True,
    )

    successes = [value for value in outcomes if isinstance(value, list)]
    failures = [value for value in outcomes if isinstance(value, DomainOperationError)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert failures[0].code == "edit_hash_conflict"
    result = EditFileResult.model_validate(completed(successes[0]).result.to_json_object())
    assert result.path == "src/main.py"
    assert result.previous_sha256 == original_hash
    assert result.replacement_count == 1


async def test_edit_revalidates_after_staging_and_removes_the_exact_temporary(
    rooted_workspace: RootedWorkspace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_hash = hashlib.sha256(rooted_workspace.file_bytes("src/main.py")).hexdigest()
    original_stage = rooted_workspace._stage_from_parent_descriptor

    def mutate_after_stage(
        parent_fd: int,
        content: bytes,
        *,
        path: str,
        mode: int | None,
    ) -> str:
        temporary = original_stage(parent_fd, content, path=path, mode=mode)
        (rooted_workspace.root / "src" / "main.py").write_text(
            "external mutation\n",
            encoding="utf-8",
        )
        return temporary

    monkeypatch.setattr(
        rooted_workspace,
        "_stage_from_parent_descriptor",
        mutate_after_stage,
    )
    with pytest.raises(DomainOperationError) as conflict:
        await invoke(
            WorkspaceToolset(rooted_workspace),
            "edit_file",
            {
                "path": "src/main.py",
                "expected_sha256": original_hash,
                "old_text": "alpha = 1",
                "new_text": "alpha = 2",
            },
        )
    assert conflict.value.code == "edit_hash_conflict"
    assert rooted_workspace.file_bytes("src/main.py") == b"external mutation\n"
    assert not any(
        path.name.startswith(".agent-edit-") for path in rooted_workspace.root.rglob("*")
    )


async def test_edit_restores_original_when_directory_durability_fails(
    rooted_workspace: RootedWorkspace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = rooted_workspace.file_bytes("src/main.py")
    original_hash = hashlib.sha256(original).hexdigest()
    real_fsync = os.fsync

    def fail_directory_fsync(descriptor: int) -> None:
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError("simulated directory fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_directory_fsync)
    with pytest.raises(DomainOperationError) as failed:
        await invoke(
            WorkspaceToolset(rooted_workspace),
            "edit_file",
            {
                "path": "src/main.py",
                "expected_sha256": original_hash,
                "old_text": "alpha = 1",
                "new_text": "alpha = 2",
            },
        )
    assert failed.value.code == "workspace_write_failed"
    assert rooted_workspace.file_bytes("src/main.py") == original
    assert not any(
        path.name.startswith((".agent-edit-", ".agent-backup-"))
        for path in rooted_workspace.root.rglob("*")
    )


async def test_newline_dense_edit_runs_off_the_event_loop(
    rooted_workspace: RootedWorkspace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dense = ("x\n" * 500_000).encode()
    (rooted_workspace.root / "dense.txt").write_bytes(dense)
    toolset = WorkspaceToolset(rooted_workspace)
    original_stage = rooted_workspace._stage_from_parent_descriptor
    entered_stage = threading.Event()
    release_stage = threading.Event()

    def blocking_stage(
        parent_fd: int,
        content: bytes,
        *,
        path: str,
        mode: int | None,
    ) -> str:
        entered_stage.set()
        assert release_stage.wait(timeout=2)
        return original_stage(parent_fd, content, path=path, mode=mode)

    monkeypatch.setattr(
        rooted_workspace,
        "_stage_from_parent_descriptor",
        blocking_stage,
    )
    edit_task = asyncio.create_task(
        invoke(
            toolset,
            "edit_file",
            {
                "path": "dense.txt",
                "expected_sha256": hashlib.sha256(dense).hexdigest(),
                "old_text": "x\n",
                "new_text": "y\n",
                "replace_all": True,
            },
        )
    )
    assert await asyncio.to_thread(entered_stage.wait, 2)
    assert not edit_task.done()
    await asyncio.wait_for(asyncio.sleep(0), timeout=0.1)
    release_stage.set()
    result = completed(await edit_task).result
    assert result["replacement_count"] == 500_000
    assert rooted_workspace.file_bytes("dense.txt").startswith(b"y\ny\n")


def test_edit_result_and_patch_identity_contracts_are_closed_and_stable(
    rooted_workspace: RootedWorkspace,
) -> None:
    previous = hashlib.sha256(b"before").hexdigest()
    after = hashlib.sha256(b"after").hexdigest()
    first = rooted_workspace._edit_patch_identity(
        path="src/main.py",
        previous_sha256=previous,
        new_sha256=after,
    )
    assert first == rooted_workspace._edit_patch_identity(
        path="src/main.py",
        previous_sha256=previous,
        new_sha256=after,
    )
    assert first != rooted_workspace._edit_patch_identity(
        path="src/other.py",
        previous_sha256=previous,
        new_sha256=after,
    )
    assert first != rooted_workspace._edit_patch_identity(
        path="src/main.py",
        previous_sha256=previous,
        new_sha256=hashlib.sha256(b"different").hexdigest(),
    )

    result = EditFileResult(
        path="src/main.py",
        created=False,
        previous_sha256=previous,
        sha256=after,
        replacement_count=1,
        patch_sha256=first,
        bytes_written=5,
    )
    assert EditFileResult.model_validate_json(result.model_dump_json()) == result
    with pytest.raises(ValidationError):
        EditFileResult.model_validate({**result.model_dump(), "unknown": True})
    with pytest.raises(ValidationError):
        EditFileResult(
            path="created.py",
            created=True,
            previous_sha256=previous,
            sha256=after,
            replacement_count=0,
            patch_sha256=first,
            bytes_written=5,
        )
    with pytest.raises(ValidationError):
        EditFileArguments(path="created.py", new_text="x", replace_all=True)


def test_tool_argument_schemas_are_closed_and_edit_modes_are_unambiguous(
    rooted_workspace: RootedWorkspace,
) -> None:
    registry = WorkspaceToolset(rooted_workspace).registry()

    with pytest.raises(DomainOperationError) as extra:
        registry.prepare(
            "list_files",
            FrozenJsonObject({"path": ".", "unknown": True}),
        )
    assert extra.value.code == "malformed_tool_arguments"

    with pytest.raises(DomainOperationError) as invalid_range:
        registry.prepare(
            "read_file",
            FrozenJsonObject({"path": "README.md", "start_line": 3, "end_line": 2}),
        )
    assert invalid_range.value.code == "malformed_tool_arguments"

    with pytest.raises(ValidationError):
        EditFileArguments(path="new.py", old_text="x", new_text="y")


def test_atomic_write_rejects_symlink_destination(
    rooted_workspace: RootedWorkspace,
    tmp_path: Path,
) -> None:
    outside = tmp_path.parent / f"{tmp_path.name}-destination.txt"
    outside.write_text("unchanged", encoding="utf-8")
    (rooted_workspace.root / "destination").symlink_to(outside)

    with pytest.raises(DomainOperationError) as rejected:
        rooted_workspace.write_file_atomic("destination", b"changed")
    assert rejected.value.code == "workspace_symlink_rejected"
    assert outside.read_text(encoding="utf-8") == "unchanged"


def test_atomic_create_never_replaces_an_existing_file(
    rooted_workspace: RootedWorkspace,
) -> None:
    with pytest.raises(DomainOperationError) as conflict:
        rooted_workspace.write_file_atomic(
            "README.md",
            b"replacement",
            require_absent=True,
        )
    assert conflict.value.code == "edit_create_conflict"
    assert rooted_workspace.file_bytes("README.md") == b"project\n"


def test_workspace_rejects_binary_oversized_and_invalid_utf8_files(tmp_path: Path) -> None:
    (tmp_path / "binary").write_bytes(b"a\x00b")
    (tmp_path / "invalid").write_bytes(b"\xff")
    (tmp_path / "large").write_bytes(b"x" * 11)
    workspace = RootedWorkspace(tmp_path, max_file_bytes=10)

    expected = {
        "binary": "workspace_binary_file",
        "invalid": "workspace_encoding_error",
        "large": "workspace_file_too_large",
    }
    for path, code in expected.items():
        with pytest.raises(DomainOperationError) as error:
            workspace.read_text(
                path,
                start_line=1,
                end_line=None,
                result_byte_limit=200,
            )
        assert error.value.code == code


def test_registry_is_read_only_without_explicit_capabilities(
    rooted_workspace: RootedWorkspace,
) -> None:
    toolset = WorkspaceToolset(rooted_workspace)
    assert {tool.name for tool in toolset.registry().definitions} == {
        "list_files",
        "read_file",
        "search_files",
    }
    assert "edit_file" in {tool.name for tool in toolset.registry(include_edit=True).definitions}
    with pytest.raises(ValueError, match="requires a sandbox"):
        toolset.registry(include_command=True)


def test_environment_does_not_affect_root_containment(
    rooted_workspace: RootedWorkspace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HOME", os.fspath(rooted_workspace.root.parent))
    with pytest.raises(DomainOperationError):
        rooted_workspace.resolve_path("~/secret")


@pytest.mark.security
async def test_metadata_matching_is_case_insensitive_and_never_allowlisted(
    rooted_workspace: RootedWorkspace,
) -> None:
    toolset = WorkspaceToolset(rooted_workspace)
    for path in (".git/config", ".GIT/config", ".gIt/config", "src/.GiT/config"):
        with pytest.raises(DomainOperationError) as rejected:
            await invoke(toolset, "read_file", {"path": path})
        assert rejected.value.code == "workspace_path_invalid"

    with pytest.raises(ValueError, match="never be allowlisted"):
        WorkspaceAccessPolicy(allowlisted_directories=[".GIT"])


@pytest.mark.security
async def test_sensitive_paths_are_filtered_and_exact_allowlists_are_narrow(
    tmp_path: Path,
) -> None:
    (tmp_path / ".env").write_text("TOKEN=secret\n", encoding="utf-8")
    (tmp_path / ".env.example").write_text("TOKEN=example\n", encoding="utf-8")
    (tmp_path / ".npmrc").write_text("token=secret\n", encoding="utf-8")
    (tmp_path / ".ssh" / "public").mkdir(parents=True)
    (tmp_path / ".ssh" / "private").mkdir()
    (tmp_path / ".ssh" / "public" / "allowed.txt").write_text(
        "allowed-value\n",
        encoding="utf-8",
    )
    (tmp_path / ".ssh" / "private" / "blocked.txt").write_text(
        "blocked-value\n",
        encoding="utf-8",
    )
    workspace = RootedWorkspace(tmp_path)
    toolset = WorkspaceToolset(workspace)

    listed = completed(
        await invoke(
            toolset,
            "list_files",
            {"path": ".", "max_depth": 3, "max_entries": 100},
        )
    ).result.to_json_object()
    listed_entries = listed["entries"]
    assert isinstance(listed_entries, list)
    listed_paths = {
        entry["path"]
        for entry in listed_entries
        if isinstance(entry, dict) and isinstance(entry.get("path"), str)
    }
    assert ".env.example" in listed_paths
    assert ".env" not in listed_paths
    assert ".npmrc" not in listed_paths
    assert ".ssh" not in listed_paths
    assert listed["protected_entries_omitted"] is True

    for path in (".env", ".npmrc", ".ssh/private/blocked.txt"):
        with pytest.raises(DomainOperationError) as rejected:
            await invoke(toolset, "read_file", {"path": path})
        assert rejected.value.code == "workspace_path_protected"

    policy = WorkspaceAccessPolicy(
        allowlisted_files=[".env"],
        allowlisted_directories=[".ssh/public"],
    )
    allowed_workspace = RootedWorkspace(tmp_path, access_policy=policy)
    allowed_toolset = WorkspaceToolset(allowed_workspace)
    env_read = completed(
        await invoke(allowed_toolset, "read_file", {"path": ".env"})
    ).result.to_json_object()
    assert env_read["content"] == "TOKEN=secret\n"
    public_read = completed(
        await invoke(
            allowed_toolset,
            "read_file",
            {"path": ".ssh/public/allowed.txt"},
        )
    ).result.to_json_object()
    assert public_read["content"] == "allowed-value\n"
    with pytest.raises(DomainOperationError) as still_protected:
        await invoke(
            allowed_toolset,
            "read_file",
            {"path": ".ssh/private/blocked.txt"},
        )
    assert still_protected.value.code == "workspace_path_protected"

    hidden_search = completed(
        await invoke(
            toolset,
            "search_files",
            {"query": "secret", "path": ".", "max_results": 10},
        )
    ).result.to_json_object()
    assert hidden_search["matches"] == []
    assert hidden_search["protected_entries_omitted"] is True
    allowed_search = completed(
        await invoke(
            allowed_toolset,
            "search_files",
            {"query": "secret", "path": ".", "max_results": 10},
        )
    ).result.to_json_object()
    assert allowed_search["matches"] == [
        {
            "path": ".env",
            "line": 1,
            "column": 7,
            "text": "TOKEN=secret",
        }
    ]


@pytest.mark.security
def test_descriptor_reads_reject_symlink_swaps_and_cycles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "target.txt"
    target.write_text("contained\n", encoding="utf-8")
    internal = tmp_path / "internal.txt"
    internal.symlink_to(target)
    broken = tmp_path / "broken"
    broken.symlink_to(tmp_path / "missing")
    cycle = tmp_path / "cycle"
    cycle.symlink_to(cycle)
    outside = tmp_path.parent / f"{tmp_path.name}-swap-secret"
    outside.write_text("outside-secret\n", encoding="utf-8")
    workspace = RootedWorkspace(tmp_path)

    canonical = workspace.read_text(
        "internal.txt",
        start_line=1,
        end_line=None,
        result_byte_limit=1024,
    )
    assert canonical.path == "target.txt"
    for path, code in (
        ("broken", "workspace_file_not_found"),
        ("cycle", "workspace_path_invalid"),
    ):
        with pytest.raises(DomainOperationError) as rejected:
            workspace.read_text(
                path,
                start_line=1,
                end_line=None,
                result_byte_limit=1024,
            )
        assert rejected.value.code == code

    original_resolve = workspace.resolve_path

    def swap_after_resolution(
        value: str,
        *,
        must_exist: bool = True,
        for_write: bool = False,
    ) -> Path:
        resolved = original_resolve(
            value,
            must_exist=must_exist,
            for_write=for_write,
        )
        if value == "target.txt":
            target.unlink()
            target.symlink_to(outside)
        return resolved

    monkeypatch.setattr(workspace, "resolve_path", swap_after_resolution)
    with pytest.raises(DomainOperationError) as swapped:
        workspace.read_text(
            "target.txt",
            start_line=1,
            end_line=None,
            result_byte_limit=1024,
        )
    assert swapped.value.code == "workspace_path_invalid"
    assert outside.read_text(encoding="utf-8") == "outside-secret\n"


async def test_complete_line_continuation_is_lossless_and_ranges_are_precise(
    tmp_path: Path,
) -> None:
    content = "first\n🙂 second\nthird\n"
    (tmp_path / "lines.txt").write_text(content, encoding="utf-8")
    (tmp_path / "empty.txt").write_text("", encoding="utf-8")
    (tmp_path / "huge-line.txt").write_text("x" * 1000, encoding="utf-8")
    workspace = RootedWorkspace(tmp_path)

    chunks: list[str] = []
    next_line: int | None = 1
    while next_line is not None:
        read = workspace.read_text(
            "lines.txt",
            start_line=next_line,
            end_line=None,
            result_byte_limit=205,
        )
        chunks.append(read.content)
        assert read.content.endswith("\n")
        next_line = read.next_start_line
    assert "".join(chunks) == content

    selected = workspace.read_text(
        "lines.txt",
        start_line=2,
        end_line=2,
        result_byte_limit=1024,
    )
    assert selected.content == "🙂 second\n"
    assert selected.end_line == 2
    assert selected.truncated is True
    assert selected.next_start_line == 3

    empty = workspace.read_text(
        "empty.txt",
        start_line=7,
        end_line=None,
        result_byte_limit=1024,
    )
    assert empty.model_dump(mode="json") == {
        "path": "empty.txt",
        "content": "",
        "sha256": hashlib.sha256(b"").hexdigest(),
        "start_line": 1,
        "end_line": 0,
        "total_lines": 0,
        "truncated": False,
        "next_start_line": None,
    }
    with pytest.raises(DomainOperationError) as out_of_range:
        workspace.read_text(
            "lines.txt",
            start_line=4,
            end_line=None,
            result_byte_limit=1024,
        )
    assert out_of_range.value.code == "workspace_line_out_of_range"
    with pytest.raises(DomainOperationError) as too_large:
        workspace.read_text(
            "huge-line.txt",
            start_line=1,
            end_line=None,
            result_byte_limit=300,
        )
    assert too_large.value.code == "workspace_line_too_large"


def test_new_result_models_are_closed_immutable_and_json_round_trip() -> None:
    listed = ListFilesResult(
        path=".",
        entries=(WorkspaceEntry(path="main.py", type="file"),),
        truncated=False,
        protected_entries_omitted=True,
    )
    searched = SearchFilesResult(
        query="needle",
        matches=(SearchMatch(path="main.py", line=1, column=1, text="needle"),),
        truncated=False,
        protected_entries_omitted=True,
    )
    read = ReadResult(
        path="main.py",
        content="needle\n",
        sha256=hashlib.sha256(b"needle\n").hexdigest(),
        start_line=1,
        end_line=1,
        total_lines=1,
        truncated=False,
    )
    for model in (listed, searched, read):
        assert type(model).model_validate_json(model.model_dump_json()) == model
        with pytest.raises(ValidationError):
            type(model).model_validate({**model.model_dump(), "unknown": float("nan")})
    with pytest.raises(ValidationError):
        listed.entries[0].model_copy(update={"type": "socket"})


class _NamedEntry:
    def __init__(self, name: str) -> None:
        self.name = name


def test_listing_scan_ceiling_precedes_sorting(tmp_path: Path) -> None:
    @contextmanager
    def scanner(_: int) -> Generator[Iterable[_NamedEntry], None, None]:
        yield (_NamedEntry(str(index)) for index in range(4))

    workspace = RootedWorkspace(
        tmp_path,
        max_scanned_entries=3,
        directory_scanner=scanner,
    )
    with pytest.raises(DomainOperationError) as exceeded:
        workspace.list_entries(".", max_depth=1, max_entries=1)
    assert exceeded.value.code == "workspace_scan_limit"


def test_dense_newline_file_does_not_materialize_per_line_objects(tmp_path: Path) -> None:
    content = b"\n" * 1_000_000
    (tmp_path / "dense.txt").write_bytes(content)
    workspace = RootedWorkspace(tmp_path)
    read = workspace.read_text(
        "dense.txt",
        start_line=1,
        end_line=None,
        result_byte_limit=300,
    )
    assert read.total_lines == 1_000_000
    assert read.truncated is True
    assert read.next_start_line is not None
    assert read.content


class _ScriptedSearchRunner:
    def __init__(
        self,
        result: ProcessResult | DomainOperationError,
    ) -> None:
        self.result = result
        self.calls: list[tuple[tuple[str, ...], Mapping[str, str] | None]] = []

    async def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        max_output_bytes: int,
        environment: Mapping[str, str] | None = None,
        on_chunk: Callable[[ProcessChunk], Awaitable[None]] | None = None,
        retain_output: bool = True,
    ) -> ProcessResult:
        del cwd, timeout_seconds, max_output_bytes, on_chunk, retain_output
        self.calls.append((tuple(argv), environment))
        if isinstance(self.result, DomainOperationError):
            raise self.result
        return self.result

    async def close(self) -> None:
        return None


def _search_process_result(
    *events: JsonObject | str,
    exit_code: int = 0,
    timed_out: bool = False,
    truncated: bool = False,
) -> ProcessResult:
    text = "".join(
        event if isinstance(event, str) else f"{json.dumps(event, ensure_ascii=False)}\n"
        for event in events
    )
    return ProcessResult(
        chunks=(ProcessChunk(channel=ToolOutputChannel.STDOUT, text=text),),
        exit_code=exit_code,
        timed_out=timed_out,
        output_truncated=truncated,
    )


def _match_event(
    *,
    path: str = "unicode.txt",
    line: int = 1,
    text: str = "🙂🙂needle\n",
    byte_offset: int = 8,
) -> JsonObject:
    return {
        "type": "match",
        "data": {
            "path": {"text": path},
            "lines": {"text": text},
            "line_number": line,
            "submatches": [{"start": byte_offset, "end": byte_offset + 6}],
        },
    }


async def test_search_invocation_is_isolated_and_unicode_columns_are_characters(
    tmp_path: Path,
) -> None:
    (tmp_path / "unicode.txt").write_text("🙂🙂needle\n", encoding="utf-8")
    runner = _ScriptedSearchRunner(_search_process_result(_match_event()))
    workspace = RootedWorkspace(
        tmp_path,
        search_runner=cast("BoundedProcessRunner", runner),
    )
    _, matches, truncated, protected = await workspace.search(
        query="needle",
        path=".",
        globs=("*.txt",),
        regex=False,
        case_sensitive=True,
        max_results=5,
        timeout_seconds=10,
        output_byte_limit=100_000,
    )
    assert matches == (SearchMatch(path="unicode.txt", line=1, column=3, text="🙂🙂needle"),)
    assert truncated is False
    assert protected is True
    argv, environment = runner.calls[0]
    assert Path(argv[0]).is_absolute()
    assert "--no-config" in argv
    assert "--no-follow" in argv
    assert "--max-filesize" in argv
    assert argv.index("*.txt") < argv.index("!**/.git/**")
    assert environment == {"LANG": "C", "LC_ALL": "C"}


@pytest.mark.parametrize(
    ("result", "expected_code"),
    [
        (_search_process_result("{not-json}\n"), "search_protocol_error"),
        (_search_process_result({"type": "unknown", "data": {}}), "search_protocol_error"),
        (_search_process_result(_match_event(path="../escape")), "search_protocol_error"),
        (_search_process_result(_match_event(line=-1)), "search_protocol_error"),
        (_search_process_result(_match_event(byte_offset=1)), "search_protocol_error"),
        (_search_process_result(exit_code=2), "search_failed"),
        (_search_process_result(timed_out=True), "search_timeout"),
    ],
)
async def test_search_fails_closed_for_protocol_and_process_errors(
    tmp_path: Path,
    result: ProcessResult,
    expected_code: str,
) -> None:
    (tmp_path / "unicode.txt").write_text("🙂🙂needle\n", encoding="utf-8")
    workspace = RootedWorkspace(
        tmp_path,
        search_runner=cast("BoundedProcessRunner", _ScriptedSearchRunner(result)),
    )
    with pytest.raises(DomainOperationError) as rejected:
        await workspace.search(
            query="needle",
            path=".",
            globs=(),
            regex=False,
            case_sensitive=True,
            max_results=1,
            timeout_seconds=10,
            output_byte_limit=100_000,
        )
    assert rejected.value.code == expected_code


async def test_search_truncation_requires_an_extra_match_or_output_truncation(
    tmp_path: Path,
) -> None:
    (tmp_path / "unicode.txt").write_text("🙂🙂needle\n", encoding="utf-8")
    one_runner = _ScriptedSearchRunner(_search_process_result(_match_event()))
    one_workspace = RootedWorkspace(
        tmp_path,
        search_runner=cast("BoundedProcessRunner", one_runner),
    )
    assert (
        await one_workspace.search(
            query="needle",
            path=".",
            globs=(),
            regex=False,
            case_sensitive=True,
            max_results=1,
            timeout_seconds=10,
            output_byte_limit=100_000,
        )
    )[2] is False

    two_runner = _ScriptedSearchRunner(_search_process_result(_match_event(), _match_event(line=2)))
    two_workspace = RootedWorkspace(
        tmp_path,
        search_runner=cast("BoundedProcessRunner", two_runner),
    )
    _, matches, truncated, _ = await two_workspace.search(
        query="needle",
        path=".",
        globs=(),
        regex=False,
        case_sensitive=True,
        max_results=1,
        timeout_seconds=10,
        output_byte_limit=100_000,
    )
    assert len(matches) == 1
    assert truncated is True

    tail_runner = _ScriptedSearchRunner(_search_process_result('{"type":"mat', truncated=True))
    tail_workspace = RootedWorkspace(
        tmp_path,
        search_runner=cast("BoundedProcessRunner", tail_runner),
    )
    assert (
        await tail_workspace.search(
            query="needle",
            path=".",
            globs=(),
            regex=False,
            case_sensitive=True,
            max_results=1,
            timeout_seconds=10,
            output_byte_limit=100_000,
        )
    )[2] is True


async def test_search_maps_startup_failure_and_invalid_regex(
    tmp_path: Path,
) -> None:
    (tmp_path / "file.txt").write_text("text\n", encoding="utf-8")
    unavailable_runner = _ScriptedSearchRunner(
        DomainOperationError(
            code="command_start_failed",
            message="opaque startup failure",
        )
    )
    unavailable_workspace = RootedWorkspace(
        tmp_path,
        search_runner=cast("BoundedProcessRunner", unavailable_runner),
    )
    with pytest.raises(DomainOperationError) as unavailable:
        await unavailable_workspace.search(
            query="text",
            path=".",
            globs=(),
            regex=False,
            case_sensitive=True,
            max_results=1,
            timeout_seconds=10,
            output_byte_limit=100_000,
        )
    assert unavailable.value.code == "search_unavailable"

    workspace = RootedWorkspace(tmp_path)
    with pytest.raises(DomainOperationError) as invalid_regex:
        await workspace.search(
            query="[",
            path=".",
            globs=(),
            regex=True,
            case_sensitive=True,
            max_results=1,
            timeout_seconds=10,
            output_byte_limit=100_000,
        )
    assert invalid_regex.value.code == "search_failed"


def test_workspace_and_toolset_reject_invalid_configuration(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="positive"):
        RootedWorkspace(tmp_path, max_file_bytes=-1)
    with pytest.raises(ValueError, match="positive"):
        RootedWorkspace(tmp_path, max_scanned_entries=-1)
    with pytest.raises(ValueError, match="positive"):
        RootedWorkspace(tmp_path, max_search_output_bytes=-1)
    with pytest.raises(ValueError, match="ripgrep_path"):
        RootedWorkspace(tmp_path, ripgrep_path="/definitely/missing/rg")
    workspace = RootedWorkspace(tmp_path)
    with pytest.raises(ValueError, match="positive"):
        WorkspaceToolset(workspace, search_timeout_seconds=-1)
    with pytest.raises(ValueError, match="positive"):
        WorkspaceToolset(workspace, default_command_timeout_seconds=-1)
    with pytest.raises(ValueError, match="positive"):
        WorkspaceToolset(workspace, max_edit_bytes=-1)
