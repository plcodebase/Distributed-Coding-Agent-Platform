"""Best-effort log redaction with explicit known-secret support."""

import re
from collections.abc import Mapping, Sequence
from typing import Any

from structlog.typing import EventDict, WrappedLogger

_REDACTED = "[REDACTED]"
_MIN_SECRET_LENGTH = 4
_SENSITIVE_KEY = re.compile(
    r"(?:api[_-]?key|authorization|credential|password|secret|token)",
    re.IGNORECASE,
)
_BUILTIN_PATTERNS = (
    re.compile(r"\bsk-ant-[A-Za-z0-9_-]{8,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+\b", re.IGNORECASE),
    re.compile(r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\b"),
    re.compile(r"(?P<prefix>://[^:/@\s]+:)[^@\s]+(?P<suffix>@)"),
)


class Redactor:
    """Redact nested log values without mutating the caller's object."""

    def __init__(self, known_secrets: Sequence[str] = ()) -> None:
        self._known_secrets = tuple(
            sorted(
                {secret for secret in known_secrets if len(secret) >= _MIN_SECRET_LENGTH},
                key=len,
                reverse=True,
            )
        )

    def redact_text(self, value: str) -> str:
        redacted = value
        for secret in self._known_secrets:
            redacted = redacted.replace(secret, _REDACTED)
        for pattern in _BUILTIN_PATTERNS:
            if "prefix" in pattern.groupindex:
                redacted = pattern.sub(
                    lambda match: f"{match.group('prefix')}{_REDACTED}{match.group('suffix')}",
                    redacted,
                )
            else:
                redacted = pattern.sub(_REDACTED, redacted)
        return redacted

    def redact(self, value: Any, *, key: str | None = None) -> Any:
        result: Any
        if key is not None and _SENSITIVE_KEY.search(key):
            result = _REDACTED
        elif isinstance(value, str):
            result = self.redact_text(value)
        elif isinstance(value, bytes):
            result = "[BINARY CONTENT OMITTED]"
        elif isinstance(value, Mapping):
            result = {
                str(item_key): self.redact(item_value, key=str(item_key))
                for item_key, item_value in value.items()
            }
        elif isinstance(value, tuple):
            result = tuple(self.redact(item) for item in value)
        elif isinstance(value, list):
            result = [self.redact(item) for item in value]
        else:
            result = value
        return result

    def structlog_processor(
        self,
        _logger: WrappedLogger,
        _method_name: str,
        event_dict: EventDict,
    ) -> EventDict:
        redacted = self.redact(event_dict)
        if not isinstance(redacted, dict):
            raise TypeError("structlog event must remain a dictionary")
        return redacted
