from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from platform_telemetry import (
    ErrorCategory,
    MetricsSettings,
    PlatformMetrics,
    PlatformTelemetry,
    TelemetryContext,
    TelemetrySettings,
    current_telemetry_context,
)


def test_telemetry_context_is_bounded_and_restored() -> None:
    telemetry = PlatformTelemetry(TelemetrySettings(service_name="agent-api"))
    context = TelemetryContext(
        tenant_id="tenant-a",
        session_id="session-a",
        run_id="run-a",
        turn_id="1",
        model_call_id="model-a",
        tool_call_id="tool-a",
    )

    assert current_telemetry_context() == TelemetryContext()
    with telemetry.bind(context):
        assert telemetry.current == context
        assert current_telemetry_context() == context
    assert current_telemetry_context() == TelemetryContext()

    with pytest.raises(ValueError, match="tenant_id"):
        TelemetryContext(tenant_id="x" * 257)
    with pytest.raises(FrozenInstanceError):
        context.run_id = "different"  # type: ignore[misc]
    telemetry.shutdown()


def test_spans_propagate_w3c_context_without_payload_content() -> None:
    telemetry = PlatformTelemetry(TelemetrySettings(service_name="agent-worker"))
    carrier: dict[str, str] = {}

    with telemetry.span(
        "agent.run",
        context=TelemetryContext(run_id="run-a"),
        attributes={"agent.run.attempt": 2},
    ) as span:
        telemetry.inject(carrier)
        assert span.get_span_context().trace_id != 0

    extracted = telemetry.extract(carrier)
    with telemetry.span("agent.child", parent=extracted) as child:
        assert child.get_span_context().trace_id != 0
        assert child.get_span_context().trace_id == int(carrier["traceparent"].split("-")[1], 16)
    assert "run-a" not in repr(carrier)
    telemetry.shutdown()


def test_invalid_settings_and_attributes_fail_closed() -> None:
    with pytest.raises(ValueError, match="service_name"):
        TelemetrySettings(service_name="bad service")
    with pytest.raises(ValueError, match="http or https"):
        TelemetrySettings(service_name="api", otlp_http_endpoint="file:///tmp/traces")
    with pytest.raises(ValueError, match="without credentials"):
        TelemetrySettings(
            service_name="api",
            otlp_http_endpoint="https://user:secret@collector.example/v1/traces",
        )
    with pytest.raises(ValueError, match="at most 32"):
        TelemetrySettings(
            service_name="api",
            otlp_headers={f"header-{index}": "value" for index in range(33)},
        )

    telemetry = PlatformTelemetry(TelemetrySettings(service_name="api"))
    with (
        pytest.raises(ValueError, match="finite"),
        telemetry.span("agent.run", attributes={"duration": float("nan")}),
    ):
        pass
    with pytest.raises(ValueError, match="timeout_millis"):
        telemetry.force_flush(0)
    telemetry.shutdown()
    telemetry.shutdown()


def test_otlp_headers_are_defensively_copied_and_repr_safe() -> None:
    headers = {"authorization": "secret-collector-token"}
    settings = TelemetrySettings(service_name="api", otlp_headers=headers)

    headers["authorization"] = "changed"

    assert settings.otlp_headers["authorization"] == "secret-collector-token"
    assert "secret-collector-token" not in repr(settings)
    with pytest.raises(TypeError):
        settings.otlp_headers["authorization"] = "mutated"  # type: ignore[index]


def test_prometheus_metrics_have_bounded_labels_and_opaque_tenants() -> None:
    metrics = PlatformMetrics(
        MetricsSettings(max_tenant_labels=1, max_route_labels=1, max_tool_labels=1)
    )
    route = metrics.route("coding-primary")
    metrics.api_requests.labels(route=route, method="POST", status="202").inc()
    metrics.api_duration.labels(route=route, method="POST").observe(0.25)
    metrics.record_cost(tenant_id="customer-secret-id", route="coding-primary", usd=0.42)
    metrics.record_cost(tenant_id="another-tenant", route="fallback-route", usd=0.01)
    metrics.tool_calls.labels(tool=metrics.tool("read_file"), outcome="success").inc()
    metrics.tool_calls.labels(tool=metrics.tool("edit_file"), outcome="error").inc()
    metrics.observe_queue(depth={"interactive": 2, "background": 1}, oldest_seconds=3.5)
    metrics.observe_worker(active=2, total=4)
    metrics.set_circuit(route="coding-primary", state="open")
    payload = metrics.render().decode("utf-8")

    assert "agent_platform_api_requests_total" in payload
    assert "customer-secret-id" not in payload
    assert 'tenant="tenant_' in payload
    assert 'tenant="overflow"' in payload
    assert 'route="overflow"' in payload
    assert 'tool="overflow"' in payload
    assert 'priority="evaluation"' in payload
    assert "agent_platform_worker_utilization_ratio 0.5" in payload
    assert 'state="open"} 1.0' in payload
    assert metrics.method("NONSTANDARD-METHOD") == "OTHER"


def test_metric_observations_validate_values() -> None:
    with pytest.raises(ValueError, match="max_tenant_labels"):
        MetricsSettings(max_tenant_labels=0)
    metrics = PlatformMetrics()
    with pytest.raises(ValueError, match="queue depth"):
        metrics.observe_queue(depth={"interactive": -1}, oldest_seconds=0)
    with pytest.raises(ValueError, match="between zero and total"):
        metrics.observe_worker(active=2, total=1)
    with pytest.raises(ValueError, match="finite and nonnegative"):
        metrics.record_cost(tenant_id="tenant", route="route", usd=float("inf"))
    with pytest.raises(ValueError, match="closed, open, or half_open"):
        metrics.set_circuit(route="route", state="unknown")


def test_record_error_is_structured_and_content_free() -> None:
    telemetry = PlatformTelemetry(TelemetrySettings(service_name="agent-worker"))
    with telemetry.span("agent.run") as span:
        telemetry.record_error(
            span,
            category=ErrorCategory.TIMEOUT,
            component="worker",
            retryable=True,
        )
    payload = telemetry.metrics.render().decode("utf-8")
    assert 'category="timeout",component="worker"' in payload
    telemetry.shutdown()


def test_unexpected_exception_content_is_not_recorded_by_span_helper() -> None:
    exporter = InMemorySpanExporter()
    telemetry = PlatformTelemetry(
        TelemetrySettings(service_name="agent-worker"),
        span_exporter=exporter,
    )
    known_value = "exception-secret-must-not-escape"

    with pytest.raises(RuntimeError, match=known_value), telemetry.span("worker.run"):
        raise RuntimeError(known_value)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert known_value not in repr(spans[0].attributes)
    assert known_value not in repr(spans[0].events)
    telemetry.shutdown()
