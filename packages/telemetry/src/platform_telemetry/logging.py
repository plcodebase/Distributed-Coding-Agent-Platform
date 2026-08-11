"""Structured JSON logging configured explicitly by each application."""

import logging
import sys
from dataclasses import dataclass
from typing import TextIO, cast

import structlog
from opentelemetry import trace
from structlog.typing import EventDict, FilteringBoundLogger

from platform_telemetry.redaction import Redactor


def _add_trace_context(
    logger: object,
    method_name: str,
    event_dict: EventDict,
) -> EventDict:
    del logger, method_name
    context = trace.get_current_span().get_span_context()
    if context.is_valid:
        event_dict["trace.id"] = f"{context.trace_id:032x}"
        event_dict["span.id"] = f"{context.span_id:016x}"
    return event_dict


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    service_name: str
    level: str = "INFO"
    json: bool = True


def configure_logging(
    settings: LoggingSettings,
    *,
    redactor: Redactor | None = None,
    stream: TextIO | None = None,
) -> FilteringBoundLogger:
    """Configure stdlib/structlog once at an application composition root."""

    output = stream or sys.stdout
    level = getattr(logging, settings.level.upper(), None)
    if not isinstance(level, int):
        raise TypeError(f"unknown log level: {settings.level}")

    logging.basicConfig(
        format="%(message)s",
        level=level,
        stream=output,
        force=True,
    )
    active_redactor = redactor or Redactor()

    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        _add_trace_context,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
        active_redactor.structlog_processor,
    ]
    if settings.json:
        processors.append(structlog.processors.JSONRenderer(sort_keys=True))
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=False))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(settings.service_name).bind(service=settings.service_name)
    return cast("FilteringBoundLogger", logger)
