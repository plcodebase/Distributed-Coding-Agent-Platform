from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import TYPE_CHECKING, Any

import pytest
from botocore.exceptions import ClientError
from pydantic import ValidationError

from agent_core.domain.errors import DomainOperationError
from artifact_store import S3ObjectStore, S3ObjectStoreSettings

if TYPE_CHECKING:
    from pathlib import Path

NOW = datetime(2026, 8, 20, tzinfo=UTC)
OBJECT_KEY = "tenants/one/workspaces/two/source.tar.gz"


class _Body(BytesIO):
    closed_by_store = False

    def close(self) -> None:
        self.closed_by_store = True
        super().close()


class _Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.content = b"artifact-content"
        self.content_type = "application/gzip"
        self.metadata: dict[str, str] = {}
        self.head_missing = False
        self.put_conflict = False
        self.closed = 0

    def generate_presigned_url(self, client_method: str, **kwargs: Any) -> str:
        self.calls.append((client_method, kwargs))
        return "https://objects.invalid/upload"

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("head", kwargs))
        if self.head_missing:
            raise ClientError(
                {"ResponseMetadata": {"HTTPStatusCode": 404}, "Error": {"Code": "NotFound"}},
                "HeadObject",
            )
        return {
            "ContentLength": len(self.content),
            "ContentType": self.content_type,
            "ETag": '"head-etag"',
            "Metadata": self.metadata,
        }

    def head_bucket(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("head_bucket", kwargs))
        return {}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("get", kwargs))
        return {
            "ContentLength": len(self.content),
            "ContentType": "application/gzip",
            "ETag": '"get-etag"',
            "Body": _Body(self.content),
        }

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        if self.put_conflict:
            raise ClientError(
                {
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                    "Error": {"Code": "PreconditionFailed"},
                },
                "PutObject",
            )
        body = kwargs["Body"]
        kwargs = dict(kwargs)
        kwargs["Body"] = body.read()
        self.calls.append(("put", kwargs))
        return {"ETag": '"put-etag"'}

    def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("delete", kwargs))
        return {}

    def close(self) -> None:
        self.closed += 1


def _store(client: _Client) -> S3ObjectStore:
    return S3ObjectStore(client, bucket="agent-platform", clock=lambda: NOW)


@pytest.mark.asyncio
async def test_readiness_checks_bucket_and_closed_state() -> None:
    client = _Client()
    store = _store(client)

    assert await store.ready() is True
    assert client.calls == [("head_bucket", {"Bucket": "agent-platform"})]
    await store.aclose()
    assert await store.ready() is False


def test_settings_require_encrypted_https_storage_in_production() -> None:
    with pytest.raises(ValidationError, match="requires encryption"):
        S3ObjectStoreSettings(environment="production", endpoint="https://objects.invalid")
    with pytest.raises(ValidationError, match="requires https"):
        S3ObjectStoreSettings(
            environment="production",
            endpoint="http://objects.invalid",
            encryption="AES256",
        )
    with pytest.raises(ValidationError, match="requires kms_key_id"):
        S3ObjectStoreSettings(encryption="aws:kms")

    settings = S3ObjectStoreSettings(
        environment="production",
        endpoint="https://objects.invalid/",
        encryption="aws:kms",
        kms_key_id="secret-key-id",
    )
    assert settings.endpoint == "https://objects.invalid"
    assert "secret-key-id" not in repr(settings)


@pytest.mark.asyncio
async def test_presigned_upload_binds_type_size_and_expiry() -> None:
    client = _Client()
    upload = await _store(client).create_upload(
        object_key=OBJECT_KEY,
        content_type="application/gzip",
        max_bytes=1234,
        expires_at=NOW + timedelta(minutes=5),
    )

    assert upload.headers["x-amz-meta-max-bytes"] == "1234"
    method, call = client.calls[0]
    assert method == "put_object"
    assert call["ExpiresIn"] == 300
    assert call["Params"]["Metadata"] == {"max-bytes": "1234"}
    assert call["Params"]["IfNoneMatch"] == "*"
    assert upload.headers["if-none-match"] == "*"


@pytest.mark.asyncio
async def test_presigned_download_is_short_lived_and_object_scoped() -> None:
    client = _Client()
    download = await _store(client).create_download(
        object_key=OBJECT_KEY,
        expires_at=NOW + timedelta(minutes=5),
    )

    assert download.url == "https://objects.invalid/upload"
    method, call = client.calls[0]
    assert method == "get_object"
    assert call["ExpiresIn"] == 300
    assert call["Params"] == {"Bucket": "agent-platform", "Key": OBJECT_KEY}


@pytest.mark.asyncio
async def test_head_normalizes_metadata_and_missing_object() -> None:
    client = _Client()
    store = _store(client)
    found = await store.head(OBJECT_KEY)
    assert found is not None
    assert found.etag == "head-etag"
    assert found.size_bytes == len(client.content)

    client.head_missing = True
    assert await store.head(OBJECT_KEY) is None


@pytest.mark.asyncio
async def test_download_streams_and_verifies_checksum(tmp_path: Path) -> None:
    client = _Client()
    destination = tmp_path / "download.tar.gz"
    digest = hashlib.sha256(client.content).hexdigest()

    stored = await _store(client).download_to_path(
        OBJECT_KEY,
        destination,
        max_bytes=1024,
        expected_sha256=digest,
    )

    assert destination.read_bytes() == client.content
    assert stored.sha256 == digest
    assert stored.etag == "get-etag"


@pytest.mark.asyncio
async def test_download_removes_partial_file_on_integrity_failure(tmp_path: Path) -> None:
    destination = tmp_path / "download.tar.gz"

    with pytest.raises(DomainOperationError) as failure:
        await _store(_Client()).download_to_path(
            OBJECT_KEY,
            destination,
            max_bytes=1024,
            expected_sha256="0" * 64,
        )

    assert failure.value.code == "artifact_integrity_error"
    assert not destination.exists()


@pytest.mark.asyncio
async def test_download_rejects_declared_oversize_before_writing(tmp_path: Path) -> None:
    destination = tmp_path / "download.tar.gz"

    with pytest.raises(DomainOperationError) as failure:
        await _store(_Client()).download_to_path(
            OBJECT_KEY,
            destination,
            max_bytes=1,
            expected_sha256="0" * 64,
        )

    assert failure.value.code == "artifact_size_limit"
    assert not destination.exists()


@pytest.mark.asyncio
async def test_upload_hashes_file_and_sends_encryption_metadata(tmp_path: Path) -> None:
    client = _Client()
    source = tmp_path / "patch.diff"
    source.write_bytes(b"patch")
    store = S3ObjectStore(
        client,
        bucket="agent-platform",
        encryption="aws:kms",
        kms_key_id="kms-key",
    )

    stored = await store.upload_from_path(
        "tenants/one/final.patch",
        source,
        content_type="text/plain",
        max_bytes=1024,
    )

    assert stored.sha256 == hashlib.sha256(b"patch").hexdigest()
    _, call = client.calls[-1]
    assert call["Body"] == b"patch"
    assert call["Metadata"] == {"sha256": stored.sha256}
    assert call["IfNoneMatch"] == "*"
    assert call["ServerSideEncryption"] == "aws:kms"
    assert call["SSEKMSKeyId"] == "kms-key"


@pytest.mark.asyncio
async def test_upload_reuses_only_identical_immutable_object(tmp_path: Path) -> None:
    client = _Client()
    client.content = b"patch"
    client.content_type = "text/plain"
    client.metadata = {"sha256": hashlib.sha256(client.content).hexdigest()}
    client.put_conflict = True
    source = tmp_path / "patch.diff"
    source.write_bytes(client.content)

    stored = await _store(client).upload_from_path(
        "tenants/one/final.patch",
        source,
        content_type="text/plain",
        max_bytes=1024,
    )

    assert stored.sha256 == client.metadata["sha256"]
    client.metadata = {"sha256": "0" * 64}
    with pytest.raises(DomainOperationError) as conflict:
        await _store(client).upload_from_path(
            "tenants/one/final.patch",
            source,
            content_type="text/plain",
            max_bytes=1024,
        )
    assert conflict.value.code == "artifact_immutable_conflict"


@pytest.mark.asyncio
async def test_close_is_idempotent_and_blocks_future_operations() -> None:
    client = _Client()
    store = _store(client)

    await store.aclose()
    await store.aclose()

    assert client.closed == 1
    with pytest.raises(DomainOperationError) as failure:
        await store.head(OBJECT_KEY)
    assert failure.value.code == "artifact_store_closed"
