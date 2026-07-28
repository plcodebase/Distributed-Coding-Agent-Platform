"""Shared validation primitives for durable domain values."""

from __future__ import annotations

import math
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Annotated, Any, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    GetCoreSchemaHandler,
    JsonValue,
    TypeAdapter,
)
from pydantic_core import CoreSchema, core_schema

type JsonObject = dict[str, JsonValue]
type FrozenJsonValue = (
    bool | int | float | str | tuple["FrozenJsonValue", ...] | FrozenJsonObject | None
)


def _freeze_json_value(value: JsonValue) -> FrozenJsonValue:
    if isinstance(value, dict):
        return FrozenJsonObject._from_validated(value)
    if isinstance(value, list):
        return tuple(_freeze_json_value(item) for item in value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("JSON numbers must be finite")
    return value


def _thaw_json_value(value: FrozenJsonValue) -> JsonValue:
    if isinstance(value, FrozenJsonObject):
        return value.to_json_object()
    if isinstance(value, tuple):
        return [_thaw_json_value(item) for item in value]
    return value


class FrozenJsonObject(Mapping[str, FrozenJsonValue]):
    """A recursively immutable JSON object used inside trusted domain models."""

    __slots__ = ("_values",)
    _values: Mapping[str, FrozenJsonValue]

    def __setattr__(self, name: str, value: object) -> None:
        if name != "_values" or hasattr(self, "_values"):
            raise AttributeError(f"{type(self).__name__} is immutable")
        object.__setattr__(self, name, value)

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __init__(self, value: JsonObject | FrozenJsonObject) -> None:
        if isinstance(value, FrozenJsonObject):
            self._values = value._values
            return
        validated = _JSON_OBJECT_ADAPTER.validate_python(value)
        self._values = MappingProxyType(
            {key: _freeze_json_value(item) for key, item in validated.items()}
        )

    @classmethod
    def _from_validated(cls, value: JsonObject) -> FrozenJsonObject:
        instance = cls.__new__(cls)
        instance._values = MappingProxyType(
            {key: _freeze_json_value(item) for key, item in value.items()}
        )
        return instance

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: object,
        handler: GetCoreSchemaHandler,
    ) -> CoreSchema:
        """Validate wire dictionaries and serialize back to ordinary JSON containers."""

        del source_type
        json_object_schema = handler.generate_schema(JsonObject)
        return core_schema.json_or_python_schema(
            json_schema=core_schema.no_info_after_validator_function(
                cls._from_validated,
                json_object_schema,
            ),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(cls),
                    core_schema.no_info_after_validator_function(
                        cls._from_validated,
                        json_object_schema,
                    ),
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls.to_json_object,
                return_schema=json_object_schema,
            ),
        )

    def __getitem__(self, key: str) -> FrozenJsonValue:
        return self._values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.to_json_object()!r})"

    def to_json_object(self) -> JsonObject:
        """Return a defensive, mutable JSON representation for a wire boundary."""

        return {key: _thaw_json_value(value) for key, value in self._values.items()}


def normalize_timestamp(value: datetime) -> datetime:
    """Require an aware timestamp and normalize it to UTC."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return value.astimezone(UTC)


type AwareTimestamp = Annotated[datetime, AfterValidator(normalize_timestamp)]


class DomainModel(BaseModel):
    """Immutable, closed-schema base for values crossing core boundaries."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        validate_default=True,
    )

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        """Create a copy by rebuilding and revalidating the complete model.

        ``model_construct`` remains available to trusted persistence adapters only. It
        must never be used with untrusted or unvalidated persisted data.
        """

        del deep  # Domain values are immutable and validation rebuilds nested values.
        values = self.model_dump(mode="python", round_trip=True)
        if update is not None:
            values.update(update)
        return type(self).model_validate(values)


_JSON_OBJECT_ADAPTER: TypeAdapter[JsonObject] = TypeAdapter(JsonObject)
