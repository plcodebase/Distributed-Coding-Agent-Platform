"""Fail-closed mutual-TLS configuration for node-agent servers and clients."""

from __future__ import annotations

import os
import ssl
import stat
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, field_validator, model_validator

from agent_core.domain.base import DomainModel


class NodeAgentTlsSettings(DomainModel):
    environment: Literal["test", "production"] = "production"
    base_url: Annotated[str, StringConstraints(min_length=1, max_length=2048)] = (
        "https://127.0.0.1:9443"
    )
    ca_file: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    certificate_file: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    private_key_file: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    connect_timeout_seconds: float = Field(default=5, gt=0, le=60)
    operation_timeout_seconds: float = Field(default=3700, gt=0, le=7200)

    @field_validator("ca_file", "certificate_file", "private_key_file")
    @classmethod
    def require_absolute_file(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute() or "\x00" in value:
            raise ValueError("TLS paths must be absolute canonical file paths")
        return os.fspath(path)

    @model_validator(mode="after")
    def require_secure_url(self) -> Self:
        if self.environment == "production" and not self.base_url.startswith("https://"):
            raise ValueError("production node-agent clients require HTTPS")
        return self


def create_client_ssl_context(settings: NodeAgentTlsSettings) -> ssl.SSLContext:
    _require_tls_files(settings)
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=settings.ca_file)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    context.load_cert_chain(settings.certificate_file, settings.private_key_file)
    return context


def create_server_ssl_context(settings: NodeAgentTlsSettings) -> ssl.SSLContext:
    _require_tls_files(settings)
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cafile=settings.ca_file)
    context.load_cert_chain(settings.certificate_file, settings.private_key_file)
    return context


def _require_tls_files(settings: NodeAgentTlsSettings) -> None:
    for value in (
        settings.ca_file,
        settings.certificate_file,
        settings.private_key_file,
    ):
        path = Path(value)
        try:
            mode = path.stat().st_mode
        except OSError as error:
            raise ValueError("TLS files must exist before node-agent startup") from error
        if not stat.S_ISREG(mode):
            raise ValueError("TLS paths must reference regular files")
    key_stat = Path(settings.private_key_file).stat()
    key_mode = stat.S_IMODE(key_stat.st_mode)
    if settings.environment == "production":
        if key_mode & 0o037:
            raise ValueError("production TLS private keys have unsafe group/world permissions")
        if key_mode & 0o040 and key_stat.st_gid != os.getegid():
            raise ValueError("group-readable TLS keys must belong to the process group")
        if key_stat.st_uid not in {0, os.geteuid()}:
            raise ValueError("production TLS private keys must belong to root or the process user")


__all__ = [
    "NodeAgentTlsSettings",
    "create_client_ssl_context",
    "create_server_ssl_context",
]
