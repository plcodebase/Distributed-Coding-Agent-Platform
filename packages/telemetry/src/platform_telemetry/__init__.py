"""Explicit telemetry primitives shared by platform composition roots."""

from platform_telemetry.logging import LoggingSettings, configure_logging
from platform_telemetry.metrics import MetricsSettings, PlatformMetrics
from platform_telemetry.operations import OperationsServer, OperationsServerSettings
from platform_telemetry.redaction import Redactor
from platform_telemetry.telemetry import (
    ErrorCategory,
    PlatformTelemetry,
    TelemetryContext,
    TelemetrySettings,
    current_telemetry_context,
)

__all__ = [
    "ErrorCategory",
    "LoggingSettings",
    "MetricsSettings",
    "OperationsServer",
    "OperationsServerSettings",
    "PlatformMetrics",
    "PlatformTelemetry",
    "Redactor",
    "TelemetryContext",
    "TelemetrySettings",
    "configure_logging",
    "current_telemetry_context",
]
