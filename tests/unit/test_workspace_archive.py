from __future__ import annotations

import hashlib
import os
import tarfile
from typing import TYPE_CHECKING

import pytest

from agent_core.domain.errors import DomainOperationError
from artifact_store import WorkspaceArchiver

if TYPE_CHECKING:
    from pathlib import Path


def test_workspace_archive_is_deterministic_private_and_policy_filtered(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "src").mkdir()
    (root / "src" / "main.py").write_text("print('ok')\n")
    (root / "README.md").write_text("hello\n")
    (root / "link.py").symlink_to("src/main.py")
    (root / ".git").mkdir()
    (root / ".git" / "config").write_text("private")
    (root / ".env").write_text("TOKEN=private")
    first = tmp_path / "first.tar.gz"
    second = tmp_path / "second.tar.gz"
    archiver = WorkspaceArchiver()

    first_hash, first_size = archiver.create(root, first)
    second_hash, second_size = archiver.create(root, second)

    assert first.read_bytes() == second.read_bytes()
    assert first_hash == second_hash == hashlib.sha256(first.read_bytes()).hexdigest()
    assert first_size == second_size == first.stat().st_size
    with tarfile.open(first, mode="r:gz") as archive:
        names = archive.getnames()
        assert names == ["README.md", "link.py", "src", "src/main.py"]
        assert archive.getmember("link.py").linkname == "src/main.py"
        source = archive.extractfile("src/main.py")
        assert source is not None and source.read() == b"print('ok')\n"


def test_archive_never_removes_a_preexisting_destination(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    destination = tmp_path / "existing.tar.gz"
    destination.write_bytes(b"owned-by-caller")

    with pytest.raises(FileExistsError):
        WorkspaceArchiver().create(root, destination)

    assert destination.read_bytes() == b"owned-by-caller"


@pytest.mark.parametrize(
    ("archiver", "code"),
    [
        (WorkspaceArchiver(max_entries=1), "checkpoint_entry_limit"),
        (WorkspaceArchiver(max_file_bytes=1), "checkpoint_file_limit"),
        (WorkspaceArchiver(max_expanded_bytes=3), "checkpoint_expanded_limit"),
        (WorkspaceArchiver(max_compressed_bytes=1), "checkpoint_archive_limit"),
    ],
)
def test_archive_enforces_independent_resource_limits(
    tmp_path: Path,
    archiver: WorkspaceArchiver,
    code: str,
) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "a.txt").write_bytes(b"ab")
    (root / "b.txt").write_bytes(b"cd")
    destination = tmp_path / "bounded.tar.gz"

    with pytest.raises(DomainOperationError) as captured:
        archiver.create(root, destination)

    assert captured.value.code == code
    assert not destination.exists()


def test_archive_rejects_unsupported_entries_and_invalid_configuration(tmp_path: Path) -> None:
    root = tmp_path / "workspace"
    root.mkdir()
    fifo = root / "pipe"
    os.mkfifo(fifo)

    with pytest.raises(DomainOperationError) as captured:
        WorkspaceArchiver().create(root, tmp_path / "fifo.tar.gz")
    assert captured.value.code == "checkpoint_entry_unsupported"

    with pytest.raises(ValueError, match="max_entries"):
        WorkspaceArchiver(max_entries=0)
    with pytest.raises(ValueError, match="root"):
        WorkspaceArchiver().create(root / "missing", tmp_path / "missing.tar.gz")
