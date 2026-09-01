"""Bounded S3-compatible implementation of the core object-store contract."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import stat
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, Never, Protocol, Self

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_core.artifacts import (
    MediaType,
    ObjectKey,
    ObjectStat,
    PresignedDownload,
    PresignedUpload,
    StoredObject,
)
from agent_core.domain.errors import DomainOperationError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from agent_core.domain.base import AwareTimestamp
    from agent_core.domain.models import Sha256Hex

_CHUNK_BYTES = 1024 * 1024
_MAX_BUCKET_CHARS = 63
_MAX_UPLOAD_EXPIRY_SECONDS = 3600
_NOT_FOUND_STATUS = 404
_PRECONDITION_FAILED_STATUS = 412


class _StreamingBody(Protocol):
    def read(self, amount: int) -> bytes: ...

    def close(self) -> None: ...


class _S3Client(Protocol):
    def generate_presigned_url(
        self,
        client_method: str,
        **kwargs: Any,
    ) -> str: ...

    def head_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def head_bucket(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def delete_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def close(self) -> None: ...


class S3ObjectStoreSettings(BaseSettings):
    """Closed storage settings with explicit production encryption policy."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_PLATFORM_S3_",
        case_sensitive=False,
        extra="ignore",
    )

    environment: Literal["development", "test", "production"] = "development"
    endpoint: str = "http://127.0.0.1:9000"
    region: str = Field(default="us-east-1", min_length=1, max_length=100)
    bucket: str = Field(
        default="agent-platform",
        pattern=r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$",
    )
    access_key: SecretStr = SecretStr("local-minio")
    secret_key: SecretStr = SecretStr("local-minio-change-me")
    addressing_style: Literal["path", "virtual"] = "path"
    encryption: Literal["none", "AES256", "aws:kms"] = "none"
    kms_key_id: SecretStr | None = None
    connect_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    read_timeout_seconds: float = Field(default=30.0, gt=0, le=300)

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("S3 endpoint must use http or https")
        return value.rstrip("/")

    @model_validator(mode="after")
    def validate_policy(self) -> Self:
        if self.encryption == "aws:kms" and self.kms_key_id is None:
            raise ValueError("aws:kms encryption requires kms_key_id")
        if self.encryption != "aws:kms" and self.kms_key_id is not None:
            raise ValueError("kms_key_id is valid only with aws:kms encryption")
        if self.environment == "production":
            if self.encryption == "none":
                raise ValueError("production object storage requires encryption")
            if not self.endpoint.startswith("https://"):
                raise ValueError("production object storage requires https")
        return self


class S3ObjectStore:
    """Checksum-verifying adapter that never buffers a complete object in memory."""

    def __init__(
        self,
        client: _S3Client,
        *,
        bucket: str,
        encryption: Literal["none", "AES256", "aws:kms"] = "none",
        kms_key_id: str | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not bucket or len(bucket) > _MAX_BUCKET_CHARS:
            raise ValueError("bucket must be nonempty and at most 63 characters")
        if encryption == "aws:kms" and not kms_key_id:
            raise ValueError("aws:kms encryption requires a key ID")
        self._client = client
        self._bucket = bucket
        self._encryption = encryption
        self._kms_key_id = kms_key_id
        self._clock = clock or (lambda: datetime.now(UTC))
        self._closed = False
        self._close_lock = asyncio.Lock()

    async def ready(self) -> bool:
        if self._closed:
            return False
        try:
            await _await_thread(self._client.head_bucket, Bucket=self._bucket)
        except (ClientError, OSError):
            return False
        return True

    async def create_upload(
        self,
        *,
        object_key: ObjectKey,
        content_type: MediaType,
        max_bytes: int,
        expires_at: AwareTimestamp,
    ) -> PresignedUpload:
        self._require_open()
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        lifetime = int((expires_at - self._clock()).total_seconds())
        if lifetime < 1 or lifetime > _MAX_UPLOAD_EXPIRY_SECONDS:
            raise DomainOperationError(
                code="artifact_upload_expiry_invalid",
                message="artifact upload expiry must be within the next hour",
            )
        headers: dict[str, str] = {
            "content-type": content_type,
            "if-none-match": "*",
            "x-amz-meta-max-bytes": str(max_bytes),
        }
        params: dict[str, Any] = {
            "Bucket": self._bucket,
            "Key": object_key,
            "ContentType": content_type,
            "Metadata": {"max-bytes": str(max_bytes)},
            "IfNoneMatch": "*",
        }
        params.update(self._encryption_arguments())
        url = await _await_thread(
            self._client.generate_presigned_url,
            "put_object",
            Params=params,
            ExpiresIn=lifetime,
            HttpMethod="PUT",
        )
        return PresignedUpload(
            object_key=object_key,
            url=url,
            content_type=content_type,
            headers=headers,
            expires_at=expires_at,
        )

    async def head(self, object_key: ObjectKey) -> ObjectStat | None:
        self._require_open()
        try:
            response = await _await_thread(
                self._client.head_object,
                Bucket=self._bucket,
                Key=object_key,
            )
        except ClientError as exc:
            status_code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status_code == _NOT_FOUND_STATUS:
                return None
            raise _opaque_storage_error("artifact_head_failed") from None
        return _object_stat(object_key, response)

    async def create_download(
        self,
        *,
        object_key: ObjectKey,
        expires_at: AwareTimestamp,
    ) -> PresignedDownload:
        self._require_open()
        lifetime = int((expires_at - self._clock()).total_seconds())
        if lifetime < 1 or lifetime > _MAX_UPLOAD_EXPIRY_SECONDS:
            raise DomainOperationError(
                code="artifact_download_expiry_invalid",
                message="artifact download expiry must be within the next hour",
            )
        url = await _await_thread(
            self._client.generate_presigned_url,
            "get_object",
            Params={"Bucket": self._bucket, "Key": object_key},
            ExpiresIn=lifetime,
            HttpMethod="GET",
        )
        return PresignedDownload(url=url, expires_at=expires_at)

    async def download_to_path(
        self,
        object_key: ObjectKey,
        destination: Path,
        *,
        max_bytes: int,
        expected_sha256: Sha256Hex,
    ) -> StoredObject:
        self._require_open()
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        return await _await_thread(
            self._download_sync,
            object_key,
            destination,
            max_bytes,
            expected_sha256,
        )

    async def upload_from_path(
        self,
        object_key: ObjectKey,
        source: Path,
        *,
        content_type: MediaType,
        max_bytes: int,
    ) -> StoredObject:
        self._require_open()
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        return await _await_thread(
            self._upload_sync,
            object_key,
            source,
            content_type,
            max_bytes,
        )

    async def delete(self, object_key: ObjectKey) -> None:
        self._require_open()
        try:
            await _await_thread(
                self._client.delete_object,
                Bucket=self._bucket,
                Key=object_key,
            )
        except ClientError:
            raise _opaque_storage_error("artifact_delete_failed") from None

    async def aclose(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            await _await_thread(self._client.close)
            self._closed = True

    async def __aenter__(self) -> S3ObjectStore:
        self._require_open()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    def _download_sync(
        self,
        object_key: ObjectKey,
        destination: Path,
        max_bytes: int,
        expected_sha256: Sha256Hex,
    ) -> StoredObject:
        body: _StreamingBody | None = None
        descriptor: int | None = None
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=object_key)
            object_stat = _object_stat(object_key, response)
            if object_stat.size_bytes > max_bytes:
                _raise_size_limit()
            raw_body = response.get("Body")
            if raw_body is None:
                raise _opaque_storage_error("artifact_protocol_error")
            body = raw_body
            descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            digest = hashlib.sha256()
            observed = 0
            while chunk := body.read(_CHUNK_BYTES):
                observed += len(chunk)
                if observed > max_bytes:
                    _raise_size_limit()
                digest.update(chunk)
                _write_all(descriptor, chunk)
            os.fsync(descriptor)
            actual_sha256 = digest.hexdigest()
            if observed != object_stat.size_bytes or actual_sha256 != expected_sha256:
                _raise_integrity_error()
            return StoredObject(
                object_key=object_key,
                sha256=actual_sha256,
                size_bytes=observed,
                content_type=object_stat.content_type,
                etag=object_stat.etag,
            )
        except DomainOperationError:
            _unlink_exact(destination)
            raise
        except (ClientError, OSError, TypeError, ValueError):
            _unlink_exact(destination)
            raise _opaque_storage_error("artifact_download_failed") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if body is not None:
                body.close()

    def _upload_sync(
        self,
        object_key: ObjectKey,
        source: Path,
        content_type: MediaType,
        max_bytes: int,
    ) -> StoredObject:
        descriptor: int | None = None
        file_object: Any = None
        try:
            descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            source_stat = os.fstat(descriptor)
            if not stat.S_ISREG(source_stat.st_mode) or source_stat.st_size > max_bytes:
                _raise_size_limit(message="artifact source is not a bounded regular file")
            digest = hashlib.sha256()
            observed = 0
            while chunk := os.read(descriptor, _CHUNK_BYTES):
                observed += len(chunk)
                if observed > max_bytes:
                    _raise_size_limit()
                digest.update(chunk)
            sha256 = digest.hexdigest()
            os.lseek(descriptor, 0, os.SEEK_SET)
            file_object = os.fdopen(descriptor, "rb", closefd=False)
            arguments: dict[str, Any] = {
                "Bucket": self._bucket,
                "Key": object_key,
                "Body": file_object,
                "ContentLength": observed,
                "ContentType": content_type,
                "Metadata": {"sha256": sha256},
                "IfNoneMatch": "*",
            }
            arguments.update(self._encryption_arguments())
            try:
                response = self._client.put_object(**arguments)
            except ClientError as error:
                status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if status != _PRECONDITION_FAILED_STATUS:
                    raise
                existing = self._client.head_object(Bucket=self._bucket, Key=object_key)
                metadata = existing.get("Metadata")
                if (
                    existing.get("ContentLength") != observed
                    or existing.get("ContentType") != content_type
                    or not isinstance(metadata, dict)
                    or metadata.get("sha256") != sha256
                ):
                    raise DomainOperationError(
                        code="artifact_immutable_conflict",
                        message="the immutable artifact key already contains different content",
                    ) from None
                return StoredObject(
                    object_key=object_key,
                    sha256=sha256,
                    size_bytes=observed,
                    content_type=content_type,
                    etag=_normalize_etag(existing.get("ETag")),
                )
            return StoredObject(
                object_key=object_key,
                sha256=sha256,
                size_bytes=observed,
                content_type=content_type,
                etag=_normalize_etag(response.get("ETag")),
            )
        except DomainOperationError:
            raise
        except (ClientError, OSError, TypeError, ValueError):
            raise _opaque_storage_error("artifact_upload_failed") from None
        finally:
            if file_object is not None:
                file_object.close()
            if descriptor is not None:
                os.close(descriptor)

    def _encryption_arguments(self) -> dict[str, str]:
        if self._encryption == "none":
            return {}
        arguments: dict[str, str] = {"ServerSideEncryption": self._encryption}
        if self._encryption == "aws:kms" and self._kms_key_id is not None:
            arguments["SSEKMSKeyId"] = self._kms_key_id
        return arguments

    def _require_open(self) -> None:
        if self._closed:
            raise DomainOperationError(
                code="artifact_store_closed",
                message="artifact store is closed",
            )


def create_s3_object_store(settings: S3ObjectStoreSettings) -> S3ObjectStore:
    """Create one explicitly owned boto client for an S3-compatible endpoint."""

    client = boto3.client(
        "s3",
        endpoint_url=settings.endpoint,
        region_name=settings.region,
        aws_access_key_id=settings.access_key.get_secret_value(),
        aws_secret_access_key=settings.secret_key.get_secret_value(),
        config=Config(
            signature_version="s3v4",
            connect_timeout=settings.connect_timeout_seconds,
            read_timeout=settings.read_timeout_seconds,
            retries={"max_attempts": 0, "mode": "standard"},
            s3={"addressing_style": settings.addressing_style},
        ),
    )
    return S3ObjectStore(
        client,
        bucket=settings.bucket,
        encryption=settings.encryption,
        kms_key_id=(settings.kms_key_id.get_secret_value() if settings.kms_key_id else None),
    )


def _object_stat(object_key: ObjectKey, response: Mapping[str, Any]) -> ObjectStat:
    size = response.get("ContentLength")
    content_type = response.get("ContentType")
    if not isinstance(size, int) or size < 0 or not isinstance(content_type, str):
        raise _opaque_storage_error("artifact_protocol_error")
    return ObjectStat(
        object_key=object_key,
        size_bytes=size,
        content_type=content_type,
        etag=_normalize_etag(response.get("ETag")),
    )


def _normalize_etag(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise _opaque_storage_error("artifact_protocol_error")
    normalized = value.strip('"')
    if not normalized:
        raise _opaque_storage_error("artifact_protocol_error")
    return normalized


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("short object-store write")
        view = view[written:]


def _unlink_exact(path: Path) -> None:
    with contextlib.suppress(OSError):
        path.unlink(missing_ok=True)


def _raise_size_limit(
    message: str = "artifact exceeds its configured byte limit",
) -> Never:
    raise DomainOperationError(
        code="artifact_size_limit",
        message=message,
    )


def _raise_integrity_error() -> Never:
    raise DomainOperationError(
        code="artifact_integrity_error",
        message="artifact size or checksum did not match",
    )


def _opaque_storage_error(code: str) -> DomainOperationError:
    return DomainOperationError(
        code=code,
        message="artifact storage operation failed",
        retryable=True,
    )


async def _await_thread[ResultT](
    function: Callable[..., ResultT],
    *args: Any,
    **kwargs: Any,
) -> ResultT:
    """Do not abandon a storage thread while it still owns files or a response body."""

    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


__all__ = ["S3ObjectStore", "S3ObjectStoreSettings", "create_s3_object_store"]
