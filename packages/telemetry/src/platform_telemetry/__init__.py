"""Telemetry primitives that do not depend on a concrete exporter."""

from platform_telemetry.logging import LoggingSettings, configure_logging
from platform_telemetry.redaction import Redactor

__all__ = ["LoggingSettings", "Redactor", "configure_logging"]
