"""Authentication abstraction independent of any identity provider."""

from __future__ import annotations

import hmac
import re
import uuid  # noqa: TC003 - Pydantic resolves tenant IDs at runtime
from typing import Annotated, Protocol

from pydantic import StringConstraints

from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError

MAX_STATIC_CREDENTIALS = 1000
MAX_BEARER_TOKEN_LENGTH = 4096
MAX_AUTHORIZATION_LENGTH = len("Bearer ") + MAX_BEARER_TOKEN_LENGTH
_BEARER_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9._~+/=-]+$")


class Principal(DomainModel):
    """Authenticated tenant and audit subject."""

    tenant_id: uuid.UUID
    subject: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ]


class Authenticator(Protocol):
    """Resolve a bearer credential without exposing provider-specific request objects."""

    async def authenticate(self, authorization: str | None) -> Principal:
        """Return the principal or raise a structured authentication failure."""


class StaticTokenAuthenticator:
    """Bounded local authenticator for development and deterministic tests."""

    def __init__(self, credentials: dict[str, Principal]) -> None:
        if not isinstance(credentials, dict):
            raise TypeError("credentials must be a dictionary")
        if not 1 <= len(credentials) <= MAX_STATIC_CREDENTIALS:
            raise ValueError("credentials must contain between 1 and 1000 entries")
        encoded_credentials: list[tuple[bytes, Principal]] = []
        for token, principal in credentials.items():
            if (
                not isinstance(token, str)
                or not _BEARER_TOKEN_PATTERN.fullmatch(token)
                or len(token.encode("ascii")) > MAX_BEARER_TOKEN_LENGTH
            ):
                raise ValueError("credential tokens must be header-safe and bounded")
            if not isinstance(principal, Principal):
                raise TypeError("credential principals must be validated Principal values")
            encoded_credentials.append((token.encode("ascii"), principal))
        self._credentials = tuple(encoded_credentials)

    async def authenticate(self, authorization: str | None) -> Principal:
        token = _bearer_token(authorization)
        matched: Principal | None = None
        for candidate, principal in self._credentials:
            if hmac.compare_digest(token, candidate):
                matched = principal
        if matched is None:
            raise _authentication_error()
        return matched


def _bearer_token(authorization: str | None) -> bytes:
    if not isinstance(authorization, str) or len(authorization) > MAX_AUTHORIZATION_LENGTH:
        raise _authentication_error()
    scheme, separator, token = authorization.partition(" ")
    if (
        separator != " "
        or scheme.casefold() != "bearer"
        or not _BEARER_TOKEN_PATTERN.fullmatch(token)
    ):
        raise _authentication_error()
    return token.encode("ascii")


def _authentication_error() -> DomainOperationError:
    return DomainOperationError(
        code="authentication_required",
        message="a valid bearer credential is required",
    )


__all__ = ["Authenticator", "Principal", "StaticTokenAuthenticator"]
