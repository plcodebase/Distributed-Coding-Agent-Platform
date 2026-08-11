import json
from io import StringIO

import pytest
from pydantic import ValidationError

from agent_core.settings import PlatformSettings
from platform_telemetry import (
    LoggingSettings,
    PlatformTelemetry,
    Redactor,
    TelemetryContext,
    TelemetrySettings,
    configure_logging,
)


def test_settings_read_prefixed_environment_and_hide_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AGENT_PLATFORM_ENVIRONMENT", "test")
    monkeypatch.setenv("AGENT_PLATFORM_MAX_TURNS", "12")
    monkeypatch.setenv("AGENT_PLATFORM_GATEWAY_API_KEY", "sk-known-secret-value")

    settings = PlatformSettings()

    assert settings.environment == "test"
    assert settings.max_turns == 12
    assert settings.gateway_api_key.get_secret_value() == "sk-known-secret-value"
    assert "sk-known-secret-value" not in repr(settings)
    assert "sk-known-secret-value" in settings.redaction_values()


def test_settings_validate_cross_field_invariants() -> None:
    with pytest.raises(ValidationError, match="may not exceed"):
        PlatformSettings(
            command_timeout_seconds=121,
            max_command_timeout_seconds=120,
        )

    with pytest.raises(ValidationError, match="heartbeat interval"):
        PlatformSettings(
            heartbeat_interval_seconds=20,
            lease_duration_seconds=20,
        )

    with pytest.raises(ValidationError, match="http or https"):
        PlatformSettings(gateway_url="ftp://gateway")


def test_redactor_handles_known_patterned_nested_and_url_secrets() -> None:
    redactor = Redactor(("exact-secret",))
    value = {
        "message": (
            "exact-secret sk-abcdefghijk postgresql://user:password@example.invalid/database"
        ),
        "authorization": "Bearer abc.def.ghi",
        "nested": [{"token": "anything"}, b"source"],
    }

    redacted = redactor.redact(value)

    assert redacted["authorization"] == "[REDACTED]"
    assert redacted["nested"] == [
        {"token": "[REDACTED]"},
        "[BINARY CONTENT OMITTED]",
    ]
    assert "exact-secret" not in redacted["message"]
    assert "password" not in redacted["message"]
    assert "sk-abcdefghijk" not in redacted["message"]


def test_structured_logger_emits_redacted_json() -> None:
    output = StringIO()
    logger = configure_logging(
        LoggingSettings(service_name="unit-test"),
        redactor=Redactor(("exact-secret",)),
        stream=output,
    )

    logger.info("model_request", api_key="exact-secret", safe="value")

    event = json.loads(output.getvalue())
    assert event["event"] == "model_request"
    assert event["api_key"] == "[REDACTED]"
    assert event["service"] == "unit-test"
    assert event["safe"] == "value"


def test_structured_logger_includes_active_trace_and_platform_context() -> None:
    output = StringIO()
    logger = configure_logging(LoggingSettings(service_name="unit-test"), stream=output)
    telemetry = PlatformTelemetry(TelemetrySettings(service_name="unit-test"))

    with telemetry.span(
        "worker.run",
        context=TelemetryContext(tenant_id="tenant-a", run_id="run-a"),
    ):
        logger.info("run_started")

    event = json.loads(output.getvalue())
    assert len(event["trace.id"]) == 32
    assert len(event["span.id"]) == 16
    assert event["agent.tenant.id"] == "tenant-a"
    assert event["agent.run.id"] == "run-a"
    telemetry.shutdown()


def test_logging_validates_level_and_supports_console_output() -> None:
    with pytest.raises(TypeError, match="unknown log level"):
        configure_logging(LoggingSettings(service_name="unit-test", level="invalid"))

    output = StringIO()
    logger = configure_logging(
        LoggingSettings(service_name="unit-test", json=False),
        stream=output,
    )
    logger.info("human_readable")

    assert "human_readable" in output.getvalue()


def test_redactor_handles_tuples_and_scalar_values() -> None:
    redactor = Redactor()

    assert redactor.redact(("safe", 3)) == ("safe", 3)
    assert redactor.redact(["safe", None]) == ["safe", None]
    assert redactor.redact(42) == 42


def test_structlog_processor_requires_dictionary_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redactor = Redactor()
    monkeypatch.setattr(redactor, "redact", lambda _value: "not-a-dictionary")

    with pytest.raises(TypeError, match="remain a dictionary"):
        redactor.structlog_processor(None, "info", {})
