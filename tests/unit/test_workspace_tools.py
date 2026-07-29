import hashlib
import json
import os
import shutil
from collections.abc import Sequence
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from agent_core.domain import DomainOperationError, FrozenJsonObject, JsonObject
from agent_core.tools import (
    ToolExecutionCompleted,
    ToolExecutionContext,
    ToolExecutionEvent,
)
from sandbox_runtime import EditFileArguments, RootedWorkspace, WorkspaceToolset

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
    prepared = toolset.registry().prepare(name, FrozenJsonObject(arguments))
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
    (rooted_workspace.root / "many.txt").write_text("🙂" * 1000, encoding="utf-8")

    read = await invoke(
        toolset,
        "read_file",
        {"path": "many.txt"},
        context=execution_context(result_limit=300),
    )
    read_json = completed(read).result.to_json_object()
    assert read_json["truncated"] is True
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
                content_byte_limit=100,
            )
        assert error.value.code == code


def test_command_tool_is_not_registered_without_explicit_request(
    rooted_workspace: RootedWorkspace,
) -> None:
    toolset = WorkspaceToolset(rooted_workspace)
    assert "run_command" not in {tool.name for tool in toolset.registry().definitions}
    with pytest.raises(ValueError, match="requires a sandbox"):
        toolset.registry(include_command=True)


def test_environment_does_not_affect_root_containment(
    rooted_workspace: RootedWorkspace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HOME", os.fspath(rooted_workspace.root.parent))
    with pytest.raises(DomainOperationError):
        rooted_workspace.resolve_path("~/secret")
