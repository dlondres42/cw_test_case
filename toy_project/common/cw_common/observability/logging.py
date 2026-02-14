"""
Structured JSON logging with OpenTelemetry trace context injection.

Provides a single `setup_logging()` call that configures the root logger
with JSON output and automatic trace/span ID injection into every log line.

Usage::

    from cw_common.observability.logging import setup_logging, get_logger

    setup_logging()                       # call once at process startup
    logger = get_logger("my-service")     # get a named logger
    logger.info("hello")                  # {"timestamp": ..., "level": "INFO", ...}
"""

import logging

from opentelemetry.instrumentation.logging import LoggingInstrumentor
from pythonjsonlogger.json import JsonFormatter


_FORMAT_STRING = "%(timestamp)s %(level)s %(name)s %(message)s"
_setup_done = False


class JsonTraceFormatter(JsonFormatter):
    """JSON formatter that adds standard fields to every log record."""

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = record.created
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        log_record["message"] = record.getMessage()


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure the root logger with structured JSON output and trace context.

    Safe to call multiple times — subsequent calls are no-ops.

    Args:
        level: The root log level (default ``logging.INFO``).
    """
    global _setup_done
    if _setup_done:
        return
    _setup_done = True

    # Instrument stdlib logging so OTel injects trace/span IDs
    LoggingInstrumentor().instrument(set_logging_format=False)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonTraceFormatter(_FORMAT_STRING))

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger (thin wrapper for discoverability)."""
    return logging.getLogger(name)
