"""Best-effort log redaction with explicit known-secret support."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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

    def stream(self) -> StreamingRedactor:
        """Return a fragment-safe incremental redactor for one text stream.

        The stream retains the only suffix that could still become a configured
        secret or one of the built-in token shapes after the next fragment.
        Callers must call ``finish`` before discarding the stream.
        """

        return StreamingRedactor(
            self,
            known_secrets=self._known_secrets,
            literal_overlap=max(
                (len(secret) - 1 for secret in self._known_secrets),
                default=0,
            ),
        )

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


class StreamingRedactor:
    """Incrementally redact text without exposing secrets split across chunks."""

    def __init__(
        self,
        redactor: Redactor,
        *,
        known_secrets: tuple[str, ...],
        literal_overlap: int,
    ) -> None:
        self._redactor = redactor
        self._known_secrets = known_secrets
        self._pending: list[str] = []
        self._finished = False
        self._literal_overlap = literal_overlap

    def feed(self, value: str) -> str:
        """Consume one raw fragment and return only the safe redacted prefix."""

        if self._finished:
            raise RuntimeError("redaction stream is already finished")
        if not value:
            return ""
        self._pending.append(value)
        if not any(character.isspace() for character in value):
            return ""
        pending = "".join(self._pending)
        flush_at = min(
            max(0, len(pending) - self._literal_overlap),
            _last_two_tokens_start(pending),
        )
        flush_at = _avoid_redaction_match_split(
            pending,
            flush_at,
            known_secrets=self._known_secrets,
        )
        if flush_at == 0:
            return ""
        stable, suffix = pending[:flush_at], pending[flush_at:]
        self._pending = [suffix] if suffix else []
        return self._redactor.redact_text(stable)

    def finish(self) -> str:
        """Redact and release the final retained suffix exactly once."""

        if self._finished:
            raise RuntimeError("redaction stream is already finished")
        self._finished = True
        stable, self._pending = "".join(self._pending), []
        return self._redactor.redact_text(stable)


def _last_two_tokens_start(value: str) -> int:
    """Keep two non-whitespace tokens so every built-in regex stays atomic."""

    cursor = len(value) - 1
    while cursor >= 0 and value[cursor].isspace():
        cursor -= 1
    while cursor >= 0 and not value[cursor].isspace():
        cursor -= 1
    while cursor >= 0 and value[cursor].isspace():
        cursor -= 1
    if cursor < 0:
        return 0
    while cursor >= 0 and not value[cursor].isspace():
        cursor -= 1
    return cursor + 1


def _avoid_redaction_match_split(
    value: str,
    boundary: int,
    *,
    known_secrets: tuple[str, ...],
) -> int:
    """Move a proposed flush boundary before every complete crossing match."""

    while boundary:
        previous = boundary
        for secret in known_secrets:
            start = max(0, boundary - len(secret) + 1)
            position = value.find(secret, start)
            while position != -1 and position < boundary:
                if position + len(secret) > boundary:
                    boundary = position
                    break
                position = value.find(secret, position + 1)
        for pattern in _BUILTIN_PATTERNS:
            for regex_match in pattern.finditer(value):
                if regex_match.start() < boundary < regex_match.end():
                    boundary = regex_match.start()
                    break
        if boundary == previous:
            return boundary
    return 0
