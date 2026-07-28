"""Shared validation primitives for durable domain values."""

from collections.abc import Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Annotated, cast

from pydantic import AfterValidator, BaseModel, ConfigDict, JsonValue, PlainSerializer


def _freeze_json_value(value: JsonValue) -> JsonValue:
    if isinstance(value, dict):
        frozen = MappingProxyType({key: _freeze_json_value(item) for key, item in value.items()})
        return cast("JsonValue", frozen)
    if isinstance(value, list):
        return cast("JsonValue", tuple(_freeze_json_value(item) for item in value))
    return value


def freeze_json_object(value: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Recursively freeze a validated JSON object against post-hash mutation."""

    return cast("dict[str, JsonValue]", _freeze_json_value(value))


def _thaw_json_value(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json_value(item) for item in value]
    return cast("JsonValue", value)


def thaw_json_object(value: dict[str, JsonValue]) -> dict[str, JsonValue]:
    """Return a plain JSON object for hashing and serialization."""

    return cast("dict[str, JsonValue]", _thaw_json_value(value))


type JsonObject = Annotated[
    dict[str, JsonValue],
    AfterValidator(freeze_json_object),
    PlainSerializer(thaw_json_object, return_type=dict[str, JsonValue]),
]


def normalize_timestamp(value: datetime) -> datetime:
    """Require an aware timestamp and normalize it to UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(UTC)


type AwareTimestamp = Annotated[datetime, AfterValidator(normalize_timestamp)]


class DomainModel(BaseModel):
    """Immutable, closed-schema base for values crossing core boundaries."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        validate_default=True,
    )
