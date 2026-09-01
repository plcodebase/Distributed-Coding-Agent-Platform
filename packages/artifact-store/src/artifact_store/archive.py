"""Deterministic, bounded workspace checkpoint archive creation."""

from __future__ import annotations

import gzip
import hashlib
import os
import stat
import tarfile
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from agent_core.domain.errors import DomainOperationError
from agent_core.workspace_access import WorkspaceAccessPolicy

_COPY_CHUNK_BYTES = 1024 * 1024


class _BoundedWriter:
    def __init__(self, target: BinaryIO, limit: int) -> None:
        self._target = target
        self._limit = limit
        self.written = 0

    def write(self, data: bytes) -> int:
        if self.written + len(data) > self._limit:
            raise DomainOperationError(
                code="checkpoint_archive_limit",
                message="the workspace checkpoint exceeds its compressed byte limit",
            )
        written = self._target.write(data)
        self.written += written
        return written

    def flush(self) -> None:
        self._target.flush()


class _BoundedReader:
    def __init__(self, source: BinaryIO, expected_size: int) -> None:
        self._source = source
        self._remaining = expected_size

    def read(self, amount: int = -1) -> bytes:
        if self._remaining == 0:
            return b""
        requested = self._remaining if amount < 0 else min(amount, self._remaining)
        chunk = self._source.read(min(requested, _COPY_CHUNK_BYTES))
        if not chunk:
            raise DomainOperationError(
                code="checkpoint_source_changed",
                message="a workspace file changed while its checkpoint was created",
                retryable=True,
            )
        self._remaining -= len(chunk)
        return chunk


class WorkspaceArchiver:
    """Create a private gzip tar while enforcing entry and byte ceilings."""

    def __init__(
        self,
        *,
        access_policy: WorkspaceAccessPolicy | None = None,
        max_entries: int = 20_000,
        max_file_bytes: int = 64 * 1024 * 1024,
        max_expanded_bytes: int = 2 * 1024 * 1024 * 1024,
        max_compressed_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        for name, value in (
            ("max_entries", max_entries),
            ("max_file_bytes", max_file_bytes),
            ("max_expanded_bytes", max_expanded_bytes),
            ("max_compressed_bytes", max_compressed_bytes),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        self._policy = access_policy or WorkspaceAccessPolicy()
        self._max_entries = max_entries
        self._max_file_bytes = max_file_bytes
        self._max_expanded_bytes = max_expanded_bytes
        self._max_compressed_bytes = max_compressed_bytes

    def create(self, root: Path, destination: Path) -> tuple[str, int]:
        """Write a deterministic archive and return its checksum and byte size."""

        try:
            resolved_root = root.resolve(strict=True)
        except OSError:
            raise ValueError("workspace archive root must be an existing directory") from None
        if not resolved_root.is_dir():
            raise ValueError("workspace archive root must be a directory")
        entries = self._entries(resolved_root)
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        digest = hashlib.sha256()
        created_destination = False
        try:
            with destination.open("xb") as raw:
                created_destination = True
                bounded = _BoundedWriter(raw, self._max_compressed_bytes)
                with (
                    gzip.GzipFile(filename="", mode="wb", fileobj=bounded, mtime=0) as compressed,
                    tarfile.open(fileobj=compressed, mode="w") as archive,
                ):
                    for relative, metadata in entries:
                        self._append(archive, resolved_root, relative, metadata)
                raw.flush()
                os.fsync(raw.fileno())
            with destination.open("rb") as source:
                while chunk := source.read(_COPY_CHUNK_BYTES):
                    digest.update(chunk)
            return digest.hexdigest(), destination.stat().st_size
        except BaseException:
            if created_destination:
                with suppress(OSError):
                    destination.unlink(missing_ok=True)
            raise

    def _entries(self, root: Path) -> tuple[tuple[PurePosixPath, os.stat_result], ...]:
        entries: list[tuple[PurePosixPath, os.stat_result]] = []
        expanded = 0
        pending = [PurePosixPath(".")]
        while pending:
            parent = pending.pop()
            directory = root.joinpath(*(() if parent == PurePosixPath(".") else parent.parts))
            try:
                children = sorted(os.scandir(directory), key=lambda item: item.name)
            except OSError:
                raise DomainOperationError(
                    code="checkpoint_source_changed",
                    message="the workspace changed while its checkpoint was created",
                    retryable=True,
                ) from None
            directories: list[PurePosixPath] = []
            for child in children:
                relative = (
                    PurePosixPath(child.name)
                    if parent == PurePosixPath(".")
                    else parent / child.name
                )
                if self._policy.is_protected(relative):
                    continue
                try:
                    metadata = child.stat(follow_symlinks=False)
                except OSError:
                    raise DomainOperationError(
                        code="checkpoint_source_changed",
                        message="the workspace changed while its checkpoint was created",
                        retryable=True,
                    ) from None
                if not (
                    stat.S_ISREG(metadata.st_mode)
                    or stat.S_ISDIR(metadata.st_mode)
                    or stat.S_ISLNK(metadata.st_mode)
                ):
                    raise DomainOperationError(
                        code="checkpoint_entry_unsupported",
                        message="the workspace contains an unsupported entry type",
                    )
                if stat.S_ISREG(metadata.st_mode):
                    if metadata.st_size > self._max_file_bytes:
                        raise DomainOperationError(
                            code="checkpoint_file_limit",
                            message="a workspace file exceeds the checkpoint byte limit",
                        )
                    expanded += metadata.st_size
                    if expanded > self._max_expanded_bytes:
                        raise DomainOperationError(
                            code="checkpoint_expanded_limit",
                            message="the workspace exceeds the checkpoint byte limit",
                        )
                entries.append((relative, metadata))
                if len(entries) > self._max_entries:
                    raise DomainOperationError(
                        code="checkpoint_entry_limit",
                        message="the workspace exceeds the checkpoint entry limit",
                    )
                if stat.S_ISDIR(metadata.st_mode):
                    directories.append(relative)
            pending.extend(reversed(directories))
        return tuple(sorted(entries, key=lambda item: item[0].as_posix()))

    @staticmethod
    def _append(
        archive: tarfile.TarFile,
        root: Path,
        relative: PurePosixPath,
        observed: os.stat_result,
    ) -> None:
        source = root.joinpath(*relative.parts)
        current = source.lstat()
        if (current.st_dev, current.st_ino, stat.S_IFMT(current.st_mode)) != (
            observed.st_dev,
            observed.st_ino,
            stat.S_IFMT(observed.st_mode),
        ):
            raise DomainOperationError(
                code="checkpoint_source_changed",
                message="the workspace changed while its checkpoint was created",
                retryable=True,
            )
        info = tarfile.TarInfo(relative.as_posix())
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        info.mtime = 0
        info.mode = stat.S_IMODE(current.st_mode)
        if stat.S_ISDIR(current.st_mode):
            info.type = tarfile.DIRTYPE
            archive.addfile(info)
            return
        if stat.S_ISLNK(current.st_mode):
            info.type = tarfile.SYMTYPE
            info.linkname = source.readlink().as_posix()
            archive.addfile(info)
            return
        info.type = tarfile.REGTYPE
        info.size = current.st_size
        descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino, opened.st_size) != (
                current.st_dev,
                current.st_ino,
                current.st_size,
            ):
                raise DomainOperationError(
                    code="checkpoint_source_changed",
                    message="the workspace changed while its checkpoint was created",
                    retryable=True,
                )
            with os.fdopen(descriptor, "rb", closefd=False) as source_file:
                archive.addfile(info, _BoundedReader(source_file, current.st_size))
        finally:
            os.close(descriptor)


__all__ = ["WorkspaceArchiver"]
