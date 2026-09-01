"""Provider-neutral immutable administrative audit contracts."""

from __future__ import annotations

import json
import uuid  # noqa: TC003 - Pydantic resolves identifiers at runtime
from typing import Annotated, Literal, Protocol, Self

from pydantic import Field, StringConstraints, model_validator

from agent_core.domain.base import AwareTimestamp, DomainModel, FrozenJsonObject

MAX_AUDIT_ENTRY_BYTES = 1024 * 1024
MAX_AUDIT_DETAILS_BYTES = 1_000_000


class AuditEntry(DomainModel):
    """One append-only authenticated administrative action intent."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    subject: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=255),
    ]
    method: Literal["POST", "PUT", "PATCH", "DELETE"]
    resource: Annotated[str, StringConstraints(min_length=1, max_length=4096)]
    action: Annotated[
        str,
        StringConstraints(min_length=1, max_length=255, pattern=r"^[a-z][a-z0-9_.:-]*$"),
    ]
    request_id: Annotated[str, StringConstraints(min_length=1, max_length=255)]
    details: FrozenJsonObject = Field(default_factory=lambda: FrozenJsonObject({}))
    occurred_at: AwareTimestamp

    @model_validator(mode="after")
    def validate_serialized_size(self) -> Self:
        details_bytes = len(
            json.dumps(
                self.details.to_json_object(),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        )
        if details_bytes > MAX_AUDIT_DETAILS_BYTES:
            raise ValueError("serialized audit details exceed the 1000000-byte limit")
        if len(self.model_dump_json().encode("utf-8")) > MAX_AUDIT_ENTRY_BYTES:
            raise ValueError("serialized audit entry exceeds the 1 MiB limit")
        return self


class AuditSink(Protocol):
    """Append-only durable audit boundary used before administrative mutations."""

    async def append(self, entry: AuditEntry) -> AuditEntry: ...


__all__ = [
    "MAX_AUDIT_DETAILS_BYTES",
    "MAX_AUDIT_ENTRY_BYTES",
    "AuditEntry",
    "AuditSink",
]
