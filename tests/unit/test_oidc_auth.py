from __future__ import annotations

import base64
import time
import uuid

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import ValidationError

from agent_api.auth import OidcAuthenticator
from agent_api.event_factory import EventGatewaySettings
from agent_api.factory import AgentApiSettings
from agent_core.domain.errors import DomainOperationError

TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


def _base64url(value: int) -> str:
    encoded = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(encoded).rstrip(b"=").decode("ascii")


@pytest.mark.asyncio
async def test_oidc_authenticator_validates_signature_claims_and_caches_jwks() -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    numbers = private_key.public_key().public_numbers()
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        assert request.url == httpx.URL("https://issuer.example/keys")
        return httpx.Response(
            200,
            json={
                "keys": [
                    {
                        "kty": "RSA",
                        "use": "sig",
                        "kid": "key-1",
                        "alg": "RS256",
                        "n": _base64url(numbers.n),
                        "e": _base64url(numbers.e),
                    }
                ]
            },
        )

    now = int(time.time())
    token = jwt.encode(
        {
            "iss": "https://issuer.example",
            "aud": "agent-platform",
            "sub": "user-1",
            "tenant_id": str(TENANT_ID),
            "iat": now,
            "exp": now + 300,
        },
        private_key,
        algorithm="RS256",
        headers={"kid": "key-1"},
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    authenticator = OidcAuthenticator(
        issuer="https://issuer.example",
        audience="agent-platform",
        jwks_url="https://issuer.example/keys",
        client=client,
    )
    try:
        first = await authenticator.authenticate(f"Bearer {token}")
        second = await authenticator.authenticate(f"Bearer {token}")
        assert first == second
        assert first.tenant_id == TENANT_ID and first.subject == "user-1"
        assert requests == 1

        wrong_audience = jwt.encode(
            {
                "iss": "https://issuer.example",
                "aud": "another-service",
                "sub": "user-1",
                "tenant_id": str(TENANT_ID),
                "iat": now,
                "exp": now + 300,
            },
            private_key,
            algorithm="RS256",
            headers={"kid": "key-1"},
        )
        with pytest.raises(DomainOperationError) as error:
            await authenticator.authenticate(f"Bearer {wrong_audience}")
        assert error.value.code == "authentication_required"
    finally:
        await authenticator.aclose()
        await client.aclose()


def test_production_api_settings_require_oidc() -> None:
    with pytest.raises(ValidationError, match="requires OIDC"):
        AgentApiSettings(environment="production", api_credentials_json='{"token": {}}')

    settings = AgentApiSettings(
        environment="production",
        auth_mode="oidc",
        oidc_issuer="https://issuer.example",
        oidc_audience="agent-platform",
        oidc_jwks_url="https://issuer.example/keys",
    )
    assert settings.auth_mode == "oidc"

    with pytest.raises(ValidationError, match="requires OIDC"):
        EventGatewaySettings(
            environment="production",
            event_credentials_json='{"token": {}}',
        )
    event_settings = EventGatewaySettings(
        environment="production",
        auth_mode="oidc",
        oidc_issuer="https://issuer.example",
        oidc_audience="agent-platform",
        oidc_jwks_url="https://issuer.example/keys",
    )
    assert event_settings.auth_mode == "oidc"
