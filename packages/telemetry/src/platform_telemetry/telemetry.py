"""OpenTelemetry lifecycle, context propagation, and safe span helpers."""

from __future__ import annotations

import asyncio
import math
import re
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import structlog
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from platform_telemetry.metrics import MetricsSettings, PlatformMetrics

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, MutableMapping

    from opentelemetry.context import Context

_NAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")
_CONTEXT_VALUE_MAX_BYTES = 256
_MAX_ENDPOINT_BYTES = 2_048
_MAX_OTLP_HEADERS = 32
_MAX_FLUSH_MILLIS = 300_000


class ErrorCategory(StrEnum):
    """Stable low-cardinality error categories shared by traces and metrics."""

    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    CAPACITY = "capacity"
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    CONFLICT = "conflict"
    DEPENDENCY = "dependency"
    PROVIDER = "provider"
    SANDBOX = "sandbox"
    PERSISTENCE = "persistence"
    INTERNAL = "internal"


def _bounded_name(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or _NAME.fullmatch(value) is None:
        raise ValueError(f"{field_name} has an invalid format")
    return value


def _bounded_optional(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a nonempty string")
    if len(value.encode("utf-8")) > _CONTEXT_VALUE_MAX_BYTES:
        raise ValueError(f"{field_name} exceeds {_CONTEXT_VALUE_MAX_BYTES} bytes")
    return value


@dataclass(frozen=True, slots=True)
class TelemetryContext:
    """Correlated platform identifiers; values are trace attributes, never metric labels."""

    tenant_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    turn_id: str | None = None
    model_call_id: str | None = None
    tool_call_id: str | None = None

    def __post_init__(self) -> None:
        values = {
            "tenant_id": self.tenant_id,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "turn_id": self.turn_id,
            "model_call_id": self.model_call_id,
            "tool_call_id": self.tool_call_id,
        }
        for name, value in values.items():
            _bounded_optional(value, field_name=name)

    def as_attributes(self) -> dict[str, str]:
        values = {
            "agent.tenant.id": self.tenant_id,
            "agent.session.id": self.session_id,
            "agent.run.id": self.run_id,
            "agent.turn.id": self.turn_id,
            "agent.model_call.id": self.model_call_id,
            "agent.tool_call.id": self.tool_call_id,
        }
        return {key: value for key, value in values.items() if value is not None}


_CURRENT_CONTEXT: ContextVar[TelemetryContext | None] = ContextVar(
    "agent_platform_telemetry_context",
    default=None,
)


@dataclass(frozen=True, slots=True)
class TelemetrySettings:
    """Validated telemetry composition settings."""

    service_name: str
    service_version: str = "0.1.0"
    environment: str = "development"
    otlp_http_endpoint: str | None = None
    otlp_headers: Mapping[str, str] = field(default_factory=dict, repr=False)
    metrics: MetricsSettings = field(default_factory=MetricsSettings)

    def __post_init__(self) -> None:
        _bounded_name(self.service_name, field_name="service_name")
        _bounded_name(self.service_version, field_name="service_version")
        _bounded_name(self.environment, field_name="environment")
        endpoint = self.otlp_http_endpoint
        if endpoint is not None:
            parsed = urlsplit(endpoint)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError(
                    "otlp_http_endpoint must use http or https without credentials or query data"
                )
            if len(endpoint.encode("utf-8")) > _MAX_ENDPOINT_BYTES:
                raise ValueError("otlp_http_endpoint exceeds 2048 bytes")
        if len(self.otlp_headers) > _MAX_OTLP_HEADERS:
            raise ValueError("otlp_headers may contain at most 32 entries")
        for key, value in self.otlp_headers.items():
            _bounded_name(key, field_name="otlp header name")
            _bounded_optional(value, field_name="otlp header value")
        object.__setattr__(self, "otlp_headers", MappingProxyType(dict(self.otlp_headers)))


class PlatformTelemetry:
    """Explicitly owned trace provider and Prometheus registry."""

    def __init__(
        self,
        settings: TelemetrySettings,
        *,
        span_exporter: SpanExporter | None = None,
    ) -> None:
        self.settings = settings
        resource = Resource.create(
            {
                "service.name": settings.service_name,
                "service.version": settings.service_version,
                "deployment.environment.name": settings.environment,
            }
        )
        self._provider = TracerProvider(resource=resource)
        if span_exporter is not None:
            self._provider.add_span_processor(SimpleSpanProcessor(span_exporter))
        if settings.otlp_http_endpoint is not None:
            exporter = OTLPSpanExporter(
                endpoint=settings.otlp_http_endpoint,
                headers=dict(settings.otlp_headers),
            )
            self._provider.add_span_processor(BatchSpanProcessor(exporter))
        self.tracer: Tracer = self._provider.get_tracer(
            "agent-platform",
            settings.service_version,
        )
        self.metrics = PlatformMetrics(settings.metrics)
        self._closed = False

    @property
    def current(self) -> TelemetryContext:
        return _CURRENT_CONTEXT.get() or TelemetryContext()

    @contextmanager
    def bind(self, context: TelemetryContext) -> Iterator[None]:
        token: Token[TelemetryContext | None] = _CURRENT_CONTEXT.set(context)
        try:
            with structlog.contextvars.bound_contextvars(**context.as_attributes()):
                yield
        finally:
            _CURRENT_CONTEXT.reset(token)

    @contextmanager
    def span(
        self,
        name: str,
        *,
        context: TelemetryContext | None = None,
        attributes: Mapping[str, str | bool | int | float] | None = None,
        parent: Context | None = None,
        kind: SpanKind = SpanKind.INTERNAL,
    ) -> Iterator[Span]:
        _bounded_name(name, field_name="span name")
        safe_attributes = self._safe_attributes(attributes or {})
        active_context = context or self.current
        safe_attributes.update(active_context.as_attributes())
        with (
            self.bind(active_context),
            self.tracer.start_as_current_span(
                name,
                context=parent,
                kind=kind,
                attributes=safe_attributes,
                record_exception=False,
                set_status_on_exception=False,
            ) as active_span,
        ):
            try:
                yield active_span
            except asyncio.CancelledError:
                active_span.set_attribute("error.type", ErrorCategory.CANCELLED.value)
                active_span.set_status(Status(StatusCode.ERROR))
                raise
            except Exception:
                active_span.set_attribute("error.type", ErrorCategory.INTERNAL.value)
                active_span.set_status(Status(StatusCode.ERROR))
                raise

    def record_error(
        self,
        span: Span,
        *,
        category: ErrorCategory,
        component: str,
        retryable: bool,
    ) -> None:
        component_label = _bounded_name(component, field_name="component")
        span.set_attribute("error.type", category.value)
        span.set_attribute("agent.error.retryable", retryable)
        span.set_status(Status(StatusCode.ERROR))
        self.metrics.errors.labels(
            category=category.value,
            component=self.metrics.component(component_label),
        ).inc()

    @staticmethod
    def annotate(span: Span, context: TelemetryContext) -> None:
        """Attach validated correlation identifiers to an already-active span."""

        for key, value in context.as_attributes().items():
            span.set_attribute(key, value)

    @staticmethod
    def inject(carrier: MutableMapping[str, str], context: Context | None = None) -> None:
        inject(carrier, context=context)

    @staticmethod
    def extract(carrier: Mapping[str, str]) -> Context:
        return TraceContextTextMapPropagator().extract(carrier=carrier)

    def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._provider.shutdown()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        if type(timeout_millis) is not int or not 1 <= timeout_millis <= _MAX_FLUSH_MILLIS:
            raise ValueError("timeout_millis must be an integer in [1, 300000]")
        return self._provider.force_flush(timeout_millis=timeout_millis)

    @staticmethod
    def _safe_attributes(
        values: Mapping[str, str | bool | int | float],
    ) -> dict[str, str | bool | int | float]:
        result: dict[str, str | bool | int | float] = {}
        for key, value in values.items():
            _bounded_name(key, field_name="attribute name")
            if isinstance(value, str):
                bounded = _bounded_optional(value, field_name=key)
                if bounded is not None:
                    result[key] = bounded
            elif isinstance(value, bool | int):
                result[key] = value
            elif isinstance(value, float):
                if not math.isfinite(value):
                    raise ValueError("span attributes must be finite")
                result[key] = value
            else:
                raise TypeError("span attributes must be scalar values")
        return result


def current_telemetry_context() -> TelemetryContext:
    return _CURRENT_CONTEXT.get() or TelemetryContext()


__all__ = [
    "ErrorCategory",
    "PlatformTelemetry",
    "TelemetryContext",
    "TelemetrySettings",
    "current_telemetry_context",
]
