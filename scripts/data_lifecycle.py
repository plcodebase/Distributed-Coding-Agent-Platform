"""Privileged, bounded data-lifecycle administration entrypoint.

This command is intentionally separate from the public API. It accepts no model input,
performs one bounded operation per invocation, and requires exact tenant confirmation
before a destructive tenant transition.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from agent_core.audit import AuditEntry
from agent_core.domain.errors import DomainOperationError
from agent_core.lifecycle import AuditExportStatus, LegalHold
from agent_core.lifecycle_service import (
    AuditExportService,
    LifecycleServiceConfig,
    ObjectDeletionWorker,
    TenantDeletionService,
)
from artifact_store import S3ObjectStoreSettings, create_s3_object_store
from platform_persistence import (
    Database,
    DatabaseSettings,
    PostgresAuditSink,
    PostgresLifecycleRepository,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import TextIO

    from agent_core.artifacts import ObjectStore
    from agent_core.domain.base import JsonObject

MAX_ACTOR_BYTES = 255
MAX_REASON_CHARS = 2_000
MAX_REASON_BYTES = 8_000
PLATFORM_AUDIT_TENANT_ID = uuid.UUID(int=0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="data-lifecycle",
        description="Run one bounded privileged lifecycle operation.",
    )
    parser.add_argument(
        "--temporary-parent",
        type=Path,
        help="Existing private parent for bounded audit-export temporary files.",
    )
    parser.add_argument("--audit-batch-size", type=int, default=1_000)
    parser.add_argument("--audit-max-records", type=int, default=10_000_000)
    parser.add_argument("--audit-max-bytes", type=int, default=1024 * 1024 * 1024)
    parser.add_argument("--deletion-batch-size", type=int, default=100)
    parser.add_argument("--deletion-lease-seconds", type=int, default=60)
    parser.add_argument("--deletion-cooling-off-seconds", type=int, default=7 * 24 * 60 * 60)
    commands = parser.add_subparsers(dest="command", required=True)

    status = commands.add_parser("status", help="Read tenant lifecycle state.")
    _tenant(status)

    export = commands.add_parser("export-audit", help="Create and verify one audit export.")
    _tenant(export)
    _actor(export)
    export.add_argument("--export-id", type=_uuid, required=True)

    verify = commands.add_parser("verify-audit", help="Download and verify audit evidence.")
    _tenant(verify)
    _actor(verify)
    verify.add_argument("--export-id", type=_uuid, required=True)

    hold = commands.add_parser("place-hold", help="Place a tenant-wide legal hold.")
    _tenant(hold)
    _actor(hold)
    hold.add_argument("--hold-id", type=_uuid, required=True)
    hold.add_argument("--reason", type=_reason, required=True)
    hold.add_argument("--expires-at", type=_timestamp)

    release = commands.add_parser("release-hold", help="Release one legal hold.")
    _tenant(release)
    _actor(release)
    release.add_argument("--hold-id", type=_uuid, required=True)

    request = commands.add_parser(
        "request-deletion",
        help="Export, verify, and request tenant deletion with a cooling-off period.",
    )
    _tenant(request)
    _actor(request)
    _tenant_confirmation(request)
    request.add_argument("--request-id", type=_uuid, required=True)
    request.add_argument("--export-id", type=_uuid, required=True)

    prepare = commands.add_parser(
        "prepare-deletion",
        help="After cooling-off, block and enqueue one tenant deletion batch.",
    )
    _tenant(prepare)
    _actor(prepare)
    _tenant_confirmation(prepare)

    finalize = commands.add_parser(
        "finalize-deletion",
        help="Finalize a tenant only after all objects and application rows are gone.",
    )
    _tenant(finalize)
    _actor(finalize)
    _tenant_confirmation(finalize)

    retention = commands.add_parser(
        "retention-scan",
        help="Enqueue one bounded batch of expired retention-safe artifacts.",
    )
    _actor(retention)
    retention.add_argument("--confirm", choices=("retention",), required=True)

    cleanup = commands.add_parser(
        "cleanup-objects",
        help="Delete one bounded leased object batch and persist every outcome.",
    )
    _actor(cleanup)
    cleanup.add_argument("--worker-id", type=_actor_value, required=True)
    cleanup.add_argument("--confirm", choices=("cleanup",), required=True)
    return parser


async def run(arguments: argparse.Namespace) -> JsonObject:
    config = LifecycleServiceConfig(
        audit_batch_size=arguments.audit_batch_size,
        audit_max_records=arguments.audit_max_records,
        audit_max_bytes=arguments.audit_max_bytes,
        deletion_batch_size=arguments.deletion_batch_size,
        deletion_lease_seconds=arguments.deletion_lease_seconds,
        deletion_cooling_off_seconds=arguments.deletion_cooling_off_seconds,
    )
    database = Database(DatabaseSettings())
    object_store = create_s3_object_store(S3ObjectStoreSettings())
    repository = PostgresLifecycleRepository(database.sessions)
    audit_sink = PostgresAuditSink(database.sessions)
    exports = AuditExportService(
        repository,
        object_store,
        config=config,
        temporary_parent=arguments.temporary_parent,
    )
    deletion = TenantDeletionService(repository, exports, config=config)
    try:
        return await _execute(
            arguments,
            repository=repository,
            audit_sink=audit_sink,
            exports=exports,
            deletion=deletion,
            object_store=object_store,
            config=config,
        )
    finally:
        try:
            await object_store.aclose()
        finally:
            await database.aclose()


async def _execute(  # noqa: PLR0911, PLR0912 - closed dispatch is locally auditable
    arguments: argparse.Namespace,
    *,
    repository: PostgresLifecycleRepository,
    audit_sink: PostgresAuditSink,
    exports: AuditExportService,
    deletion: TenantDeletionService,
    object_store: ObjectStore,
    config: LifecycleServiceConfig,
) -> JsonObject:
    command = str(arguments.command)
    occurred_at = datetime.now(UTC)
    if command == "status":
        lifecycle = await repository.get_tenant_lifecycle(arguments.tenant_id)
        return {
            "command": command,
            "lifecycle": lifecycle.model_dump(mode="json") if lifecycle is not None else None,
        }
    if command == "export-audit":
        await _audit(
            audit_sink,
            tenant_id=arguments.tenant_id,
            actor=arguments.actor,
            action="audit.export_requested",
            request_id=arguments.export_id,
            details={"export_id": str(arguments.export_id)},
            occurred_at=occurred_at,
        )
        export = await exports.export(
            arguments.tenant_id,
            export_id=arguments.export_id,
            requested_by=arguments.actor,
        )
        await exports.verify(export)
        return {"command": command, "audit_export": export.model_dump(mode="json")}
    if command == "verify-audit":
        verified_export = await repository.get_audit_export(
            arguments.tenant_id, arguments.export_id
        )
        if verified_export is None:
            raise DomainOperationError(
                code="audit_export_not_found",
                message="audit export was not found",
            )
        await _audit(
            audit_sink,
            tenant_id=arguments.tenant_id,
            actor=arguments.actor,
            action="audit.export_verification_requested",
            request_id=arguments.export_id,
            details={"export_id": str(arguments.export_id)},
            occurred_at=occurred_at,
        )
        await exports.verify(verified_export)
        if (
            verified_export.status is not AuditExportStatus.COMPLETED
            or verified_export.object is None
        ):
            raise DomainOperationError(
                code="audit_export_invalid",
                message="audit export evidence is incomplete",
            )
        return {
            "command": command,
            "verified": True,
            "audit_export": verified_export.model_dump(mode="json"),
        }
    if command == "place-hold":
        existing_hold = await repository.get_legal_hold(arguments.tenant_id, arguments.hold_id)
        if existing_hold is not None:
            if (
                existing_hold.reason != arguments.reason
                or existing_hold.placed_by != arguments.actor
                or existing_hold.expires_at != arguments.expires_at
            ):
                raise DomainOperationError(
                    code="legal_hold_conflict",
                    message="legal hold identity is already in use",
                )
            hold = existing_hold
        else:
            hold = LegalHold(
                id=arguments.hold_id,
                tenant_id=arguments.tenant_id,
                reason=arguments.reason,
                placed_by=arguments.actor,
                placed_at=occurred_at,
                expires_at=arguments.expires_at,
            )
        await _audit(
            audit_sink,
            tenant_id=arguments.tenant_id,
            actor=arguments.actor,
            action="legal_hold.placed",
            request_id=arguments.hold_id,
            details={"hold_id": str(arguments.hold_id)},
            occurred_at=occurred_at,
        )
        persisted = await repository.place_legal_hold(hold)
        return {"command": command, "legal_hold": persisted.model_dump(mode="json")}
    if command == "release-hold":
        await _audit(
            audit_sink,
            tenant_id=arguments.tenant_id,
            actor=arguments.actor,
            action="legal_hold.released",
            request_id=arguments.hold_id,
            details={"hold_id": str(arguments.hold_id)},
            occurred_at=occurred_at,
        )
        released = await repository.release_legal_hold(
            arguments.tenant_id,
            arguments.hold_id,
            released_by=arguments.actor,
            released_at=occurred_at,
        )
        return {"command": command, "legal_hold": released.model_dump(mode="json")}
    if command == "request-deletion":
        _confirm_tenant(arguments)
        await _audit(
            audit_sink,
            tenant_id=arguments.tenant_id,
            actor=arguments.actor,
            action="tenant.deletion_intent",
            request_id=arguments.request_id,
            details={
                "request_id": str(arguments.request_id),
                "export_id": str(arguments.export_id),
            },
            occurred_at=occurred_at,
        )
        lifecycle, export = await deletion.request(
            arguments.tenant_id,
            request_id=arguments.request_id,
            export_id=arguments.export_id,
            requested_by=arguments.actor,
        )
        return {
            "command": command,
            "lifecycle": lifecycle.model_dump(mode="json"),
            "audit_export": export.model_dump(mode="json"),
        }
    if command == "prepare-deletion":
        _confirm_tenant(arguments)
        await _audit(
            audit_sink,
            tenant_id=arguments.tenant_id,
            actor=arguments.actor,
            action="tenant.deletion_started",
            request_id=uuid.uuid4(),
            details={},
            occurred_at=occurred_at,
        )
        lifecycle, enqueued = await deletion.prepare(arguments.tenant_id)
        return {
            "command": command,
            "lifecycle": lifecycle.model_dump(mode="json"),
            "enqueued": enqueued,
        }
    if command == "finalize-deletion":
        _confirm_tenant(arguments)
        await _audit(
            audit_sink,
            tenant_id=arguments.tenant_id,
            actor=arguments.actor,
            action="tenant.deletion_finalized",
            request_id=uuid.uuid4(),
            details={},
            occurred_at=occurred_at,
        )
        lifecycle = await deletion.finalize(arguments.tenant_id)
        return {"command": command, "lifecycle": lifecycle.model_dump(mode="json")}
    if command == "retention-scan":
        await _audit(
            audit_sink,
            tenant_id=PLATFORM_AUDIT_TENANT_ID,
            actor=arguments.actor,
            action="retention.scan_requested",
            request_id=uuid.uuid4(),
            details={"batch_size": config.deletion_batch_size},
            occurred_at=occurred_at,
        )
        enqueued = await repository.enqueue_expired_artifacts(
            occurred_at=occurred_at,
            limit=config.deletion_batch_size,
        )
        return {"command": command, "enqueued": enqueued}
    if command == "cleanup-objects":
        await _audit(
            audit_sink,
            tenant_id=PLATFORM_AUDIT_TENANT_ID,
            actor=arguments.actor,
            action="object_cleanup.batch_requested",
            request_id=uuid.uuid4(),
            details={
                "batch_size": config.deletion_batch_size,
                "worker_id": arguments.worker_id,
            },
            occurred_at=occurred_at,
        )
        result = await ObjectDeletionWorker(
            repository,
            object_store,
            worker_id=arguments.worker_id,
            config=config,
        ).run_batch()
        return {"command": command, "result": result.model_dump(mode="json")}
    raise ValueError("unsupported lifecycle command")


async def _audit(
    sink: PostgresAuditSink,
    *,
    tenant_id: uuid.UUID,
    actor: str,
    action: str,
    request_id: uuid.UUID,
    details: JsonObject,
    occurred_at: datetime,
) -> None:
    await sink.append(
        AuditEntry(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            subject=actor,
            method="POST" if action != "tenant.deletion_finalized" else "DELETE",
            resource=f"/admin/tenants/{tenant_id}/lifecycle",
            action=action,
            request_id=str(request_id),
            details=details,
            occurred_at=occurred_at,
        )
    )


def _tenant(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--tenant-id", type=_uuid, required=True)


def _actor(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--actor", type=_actor_value, required=True)


def _tenant_confirmation(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--confirm-tenant-id", type=_uuid, required=True)


def _confirm_tenant(arguments: argparse.Namespace) -> None:
    if arguments.confirm_tenant_id != arguments.tenant_id:
        raise DomainOperationError(
            code="tenant_confirmation_mismatch",
            message="the exact tenant confirmation did not match",
        )


def _uuid(value: str) -> uuid.UUID:
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a canonical UUID") from error
    if str(parsed) != value:
        raise argparse.ArgumentTypeError("value must be a canonical lowercase UUID")
    return parsed


def _timestamp(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timestamp must be ISO 8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(UTC)


def _actor_value(value: str) -> str:
    candidate = value.strip()
    if not candidate or len(candidate.encode("utf-8")) > MAX_ACTOR_BYTES or "\x00" in candidate:
        raise argparse.ArgumentTypeError("actor must be between 1 and 255 UTF-8 bytes")
    return candidate


def _reason(value: str) -> str:
    candidate = value.strip()
    if (
        not candidate
        or len(candidate) > MAX_REASON_CHARS
        or len(candidate.encode("utf-8")) > MAX_REASON_BYTES
        or "\x00" in candidate
    ):
        raise argparse.ArgumentTypeError(
            "reason must be 1-2000 characters and at most 8000 UTF-8 bytes"
        )
    return candidate


def _write_json(stream: TextIO, value: object) -> None:
    stream.write(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        arguments = build_parser().parse_args(argv)
        result = asyncio.run(run(arguments))
    except DomainOperationError as error:
        _write_json(sys.stderr, {"error": error.as_dict()})
        return 2
    except (ValueError, OSError) as error:
        del error
        _write_json(
            sys.stderr,
            {
                "error": {
                    "code": "lifecycle_configuration_invalid",
                    "message": "lifecycle configuration is invalid",
                    "retryable": False,
                    "details": {},
                }
            },
        )
        return 2
    except Exception:
        _write_json(
            sys.stderr,
            {
                "error": {
                    "code": "lifecycle_operation_failed",
                    "message": "the lifecycle operation could not be completed",
                    "retryable": True,
                    "details": {},
                }
            },
        )
        return 1
    _write_json(sys.stdout, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
