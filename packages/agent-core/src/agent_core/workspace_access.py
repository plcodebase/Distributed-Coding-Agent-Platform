"""Shared access policy for paths exposed to coding tools or commands."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import pairwise
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Annotated

from pydantic import StringConstraints, field_validator

from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError

if TYPE_CHECKING:
    from collections.abc import Iterable

_MAX_PATH_BYTES = 4096
_SENSITIVE_DIRECTORY_COMPONENTS = frozenset({".ssh", ".aws", ".azure", ".gnupg"})
_SENSITIVE_FILENAMES = frozenset(
    {
        ".npmrc",
        ".pypirc",
        ".netrc",
        "id_rsa",
        "id_dsa",
        "id_ecdsa",
        "id_ed25519",
        "application_default_credentials.json",
    }
)
_SAFE_ENV_SUFFIXES = (".example", ".sample", ".template")


class WorkspaceFileReference(DomainModel):
    """One canonical file path explicitly supplied for model context."""

    path: Annotated[str, StringConstraints(min_length=1, max_length=_MAX_PATH_BYTES)]

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        try:
            path = normalize_workspace_path(value)
        except DomainOperationError as exc:
            raise ValueError("referenced workspace paths must be canonical relative paths") from exc
        if path == PurePosixPath("."):
            raise ValueError("referenced workspace paths must identify a file")
        if _is_repository_metadata(path):
            raise ValueError("repository metadata cannot be referenced")
        return path.as_posix()


def _path_error(
    code: str,
    message: str,
    *,
    path: str,
) -> DomainOperationError:
    return DomainOperationError(code=code, message=message, details={"path": path})


def normalize_workspace_path(value: str) -> PurePosixPath:
    """Validate and normalize one untrusted relative workspace path."""

    if not value or "\x00" in value or len(value.encode("utf-8")) > _MAX_PATH_BYTES:
        raise _path_error(
            "workspace_path_invalid",
            "workspace paths must be non-empty bounded text",
            path=value,
        )
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise _path_error(
            "workspace_path_invalid",
            "workspace paths must remain relative",
            path=value,
        )
    normalized = PurePosixPath(*path.parts)
    return normalized if normalized.parts else PurePosixPath(".")


def _casefold_parts(path: PurePosixPath) -> tuple[str, ...]:
    return tuple(part.casefold() for part in path.parts if part != ".")


def _is_repository_metadata(path: PurePosixPath) -> bool:
    return ".git" in _casefold_parts(path)


def _is_sensitive_path(path: PurePosixPath) -> bool:
    parts = _casefold_parts(path)
    if any(part in _SENSITIVE_DIRECTORY_COMPONENTS for part in parts):
        return True
    if any(left == ".config" and right == "gcloud" for left, right in pairwise(parts)):
        return True
    if not parts:
        return False
    filename = parts[-1]
    if filename in _SENSITIVE_FILENAMES:
        return True
    if filename == ".env":
        return True
    return filename.startswith(".env.") and not filename.endswith(_SAFE_ENV_SUFFIXES)


@dataclass(frozen=True, slots=True)
class WorkspaceAccessPolicy:
    """Conservative protected-path policy with explicit composition-time exceptions."""

    allowlisted_files: frozenset[PurePosixPath] = frozenset()
    allowlisted_directories: frozenset[PurePosixPath] = frozenset()

    def __init__(
        self,
        *,
        allowlisted_files: Iterable[str] = (),
        allowlisted_directories: Iterable[str] = (),
    ) -> None:
        files = frozenset(self._validated_allowlist(allowlisted_files))
        directories = frozenset(self._validated_allowlist(allowlisted_directories))
        object.__setattr__(self, "allowlisted_files", files)
        object.__setattr__(self, "allowlisted_directories", directories)

    @staticmethod
    def _validated_allowlist(values: Iterable[str]) -> tuple[PurePosixPath, ...]:
        normalized: list[PurePosixPath] = []
        for value in values:
            path = normalize_workspace_path(value)
            if _is_repository_metadata(path):
                raise ValueError("repository metadata can never be allowlisted")
            normalized.append(PurePosixPath(*_casefold_parts(path)))
        return tuple(normalized)

    def is_repository_metadata(self, path: str | PurePosixPath) -> bool:
        """Return whether any component addresses `.git`, ignoring case."""

        normalized = normalize_workspace_path(path) if isinstance(path, str) else path
        return _is_repository_metadata(normalized)

    def is_protected(self, path: str | PurePosixPath) -> bool:
        """Return whether a path must be hidden or rejected."""

        normalized = normalize_workspace_path(path) if isinstance(path, str) else path
        if _is_repository_metadata(normalized):
            return True
        if not _is_sensitive_path(normalized):
            return False
        folded = PurePosixPath(*_casefold_parts(normalized))
        if folded in self.allowlisted_files:
            return False
        return not any(
            folded == directory or folded.is_relative_to(directory)
            for directory in self.allowlisted_directories
        )

    def require_accessible(self, path: PurePosixPath, *, display_path: str) -> None:
        """Reject metadata and protected paths before any filesystem operation."""

        if _is_repository_metadata(path):
            raise _path_error(
                "workspace_path_invalid",
                "workspace paths may not address repository metadata",
                path=display_path,
            )
        if self.is_protected(path):
            raise _path_error(
                "workspace_path_protected",
                "the requested workspace path is protected by the access policy",
                path=display_path,
            )

    def ripgrep_exclusion_globs(self) -> tuple[str, ...]:
        """Return final ripgrep globs implementing defense-in-depth filtering."""

        allowed = self.allowlisted_files | self.allowlisted_directories
        allowed_parts = tuple(_casefold_parts(path) for path in allowed)
        exclusions: list[str] = []
        for component in sorted(_SENSITIVE_DIRECTORY_COMPONENTS):
            if any(component in parts for parts in allowed_parts):
                continue
            exclusions.extend(
                (
                    f"!{component}",
                    f"!**/{component}",
                    f"!{component}/**",
                    f"!**/{component}/**",
                )
            )
        if not any(
            any(left == ".config" and right == "gcloud" for left, right in pairwise(parts))
            for parts in allowed_parts
        ):
            exclusions.extend(
                (
                    "!.config/gcloud",
                    "!**/.config/gcloud",
                    "!.config/gcloud/**",
                    "!**/.config/gcloud/**",
                )
            )
        if not any(parts and parts[-1] == ".env" for parts in allowed_parts):
            exclusions.extend(("!.env", "!**/.env"))
        for filename in sorted(_SENSITIVE_FILENAMES):
            if any(parts and parts[-1] == filename for parts in allowed_parts):
                continue
            exclusions.extend((f"!{filename}", f"!**/{filename}"))
        exclusions.extend(("!.git", "!**/.git", "!.git/**", "!**/.git/**"))
        return tuple(exclusions)


__all__ = ["WorkspaceAccessPolicy", "WorkspaceFileReference", "normalize_workspace_path"]
