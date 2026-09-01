"""Authentication abstraction independent of any identity provider."""

from __future__ import annotations

import hmac
import json
import math
import re
import time
import uuid
from typing import TYPE_CHECKING, Annotated, Any, Protocol, Self

import httpx
import jwt
from pydantic import StringConstraints

from agent_core.domain.base import DomainModel
from agent_core.domain.errors import DomainOperationError

MAX_STATIC_CREDENTIALS = 1000
MAX_BEARER_TOKEN_LENGTH = 4096
MAX_AUTHORIZATION_LENGTH = len("Bearer ") + MAX_BEARER_TOKEN_LENGTH
_BEARER_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9._~+/=-]+$")
_ALLOWED_OIDC_ALGORITHMS = frozenset({"RS256", "RS384", "RS512", "ES256", "ES384"})
_MAX_JWKS_BYTES = 1024 * 1024
_MAX_JWKS_KEYS = 100

if TYPE_CHECKING:
    from collections.abc import Callable

_MAX_ISSUER_CHARS = 2048
_MAX_AUDIENCE_CHARS = 1024
_MAX_KEY_ID_CHARS = 255
_HTTP_OK = 200


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


class OidcAuthenticator:
    """Asynchronous issuer/audience/JWKS validator with a bounded key cache."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        jwks_url: str,
        tenant_claim: str = "tenant_id",
        algorithms: tuple[str, ...] = ("RS256",),
        cache_seconds: float = 300,
        clock_skew_seconds: float = 30,
        client: httpx.AsyncClient | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not issuer or len(issuer) > _MAX_ISSUER_CHARS:
            raise ValueError("OIDC issuer must be nonempty bounded text")
        if not audience or len(audience) > _MAX_AUDIENCE_CHARS:
            raise ValueError("OIDC audience must be nonempty bounded text")
        if not jwks_url.startswith("https://") or len(jwks_url) > _MAX_ISSUER_CHARS:
            raise ValueError("OIDC JWKS URL must use HTTPS and be bounded")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}", tenant_claim):
            raise ValueError("OIDC tenant claim name is invalid")
        if not algorithms or any(item not in _ALLOWED_OIDC_ALGORITHMS for item in algorithms):
            raise ValueError("OIDC algorithms must use an approved asymmetric algorithm")
        if len(set(algorithms)) != len(algorithms):
            raise ValueError("OIDC algorithms must be unique")
        for name, value, maximum in (
            ("cache_seconds", cache_seconds, 3600),
            ("clock_skew_seconds", clock_skew_seconds, 300),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not 0 <= value <= maximum
            ):
                raise ValueError(f"{name} must be finite and in [0, {maximum}]")
        self._issuer = issuer.rstrip("/")
        self._audience = audience
        self._jwks_url = jwks_url
        self._tenant_claim = tenant_claim
        self._algorithms = algorithms
        self._cache_seconds = float(cache_seconds)
        self._clock_skew_seconds = float(clock_skew_seconds)
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(10, connect=5),
            follow_redirects=False,
            trust_env=False,
        )
        self._owns_client = client is None
        self._monotonic = monotonic
        self._keys: dict[tuple[str, str], Any] = {}
        self._expires_at = 0.0
        self._closed = False

    async def authenticate(self, authorization: str | None) -> Principal:
        token = _bearer_token(authorization).decode("ascii")
        try:
            header = jwt.get_unverified_header(token)
            key_id = header.get("kid")
            algorithm = header.get("alg")
            if (
                not isinstance(key_id, str)
                or not 1 <= len(key_id) <= _MAX_KEY_ID_CHARS
                or algorithm not in self._algorithms
            ):
                raise _authentication_error()
            key = await self._key(key_id, algorithm)
            claims = jwt.decode(
                token,
                key=key,
                algorithms=[algorithm],
                audience=self._audience,
                issuer=self._issuer,
                leeway=self._clock_skew_seconds,
                options={"require": ["exp", "iat", "iss", "aud", "sub", self._tenant_claim]},
            )
            subject = claims.get("sub")
            tenant = claims.get(self._tenant_claim)
            if not isinstance(subject, str) or not isinstance(tenant, str):
                raise _authentication_error()
            return Principal(tenant_id=uuid.UUID(tenant), subject=subject)
        except DomainOperationError:
            raise
        except (jwt.PyJWTError, ValueError, TypeError, UnicodeError):
            raise _authentication_error() from None

    async def aclose(self) -> None:
        if self._closed:
            return
        if self._owns_client:
            await self._client.aclose()
        self._keys.clear()
        self._closed = True

    async def __aenter__(self) -> Self:
        if self._closed:
            raise RuntimeError("OIDC authenticator is closed")
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def _key(self, key_id: str, algorithm: str) -> Any:
        now = self._monotonic()
        key = self._keys.get((key_id, algorithm)) if now < self._expires_at else None
        if key is not None:
            return key
        await self._refresh()
        key = self._keys.get((key_id, algorithm))
        if key is None:
            raise _authentication_error()
        return key

    async def _refresh(self) -> None:
        if self._closed:
            raise _authentication_error()
        try:
            async with self._client.stream("GET", self._jwks_url) as response:
                if response.status_code != _HTTP_OK:
                    raise _authentication_error()
                content = bytearray()
                async for chunk in response.aiter_bytes():
                    content.extend(chunk)
                    if len(content) > _MAX_JWKS_BYTES:
                        raise _authentication_error()
            value = json.loads(content)
            if not isinstance(value, dict) or set(value) != {"keys"}:
                raise _authentication_error()
            keys = value["keys"]
            if not isinstance(keys, list) or not 1 <= len(keys) <= _MAX_JWKS_KEYS:
                raise _authentication_error()
            normalized: dict[tuple[str, str], Any] = {}
            for item in keys:
                if not isinstance(item, dict):
                    raise _authentication_error()
                key_id = item.get("kid")
                algorithm = item.get("alg")
                if (
                    not isinstance(key_id, str)
                    or not isinstance(algorithm, str)
                    or algorithm not in self._algorithms
                    or (key_id, algorithm) in normalized
                ):
                    raise _authentication_error()
                normalized[(key_id, algorithm)] = jwt.PyJWK.from_dict(item, algorithm).key
        except DomainOperationError:
            raise
        except (httpx.HTTPError, jwt.PyJWTError, ValueError, TypeError, UnicodeError):
            raise _authentication_error() from None
        self._keys = normalized
        self._expires_at = self._monotonic() + self._cache_seconds


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


__all__ = ["Authenticator", "OidcAuthenticator", "Principal", "StaticTokenAuthenticator"]
