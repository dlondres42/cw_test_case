"""
Structured JSON logging with OpenTelemetry trace context injection.

Provides a single `setup_logging()` call that configures the root logger
with JSON output and automatic trace/span ID injection into every log line.

Includes a ``WebhookAlertHandler`` that sends CRITICAL-level log records
(tagged with ``alert=True``) to a configurable webhook URL.

Usage::

    from cw_common.observability.logging import setup_logging, get_logger

    setup_logging()                       # call once at process startup
    logger = get_logger("my-service")     # get a named logger
    logger.info("hello")                  # {"timestamp": ..., "level": "INFO", ...}

    # Alert logging (picked up by WebhookAlertHandler if ALERT_WEBHOOK_URL is set)
    logger.critical("denied spike", extra={"alert": True, "anomaly_details": {...}})
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError

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


class WebhookAlertHandler(logging.Handler):
    """
    Logging handler that POSTs CRITICAL alert records to a webhook URL.

    Only fires for records that satisfy BOTH conditions:
      1. Level is ``CRITICAL``
      2. Record has ``alert=True`` in its extra data

    WARNING-level alerts are intentionally NOT sent to the webhook —
    they are visible in Grafana via Loki log queries only.

    The POST is done in a background thread to avoid blocking the logger.

    Args:
        webhook_url: The URL to POST alert payloads to.
        timeout: HTTP request timeout in seconds.
    """

    def __init__(self, webhook_url: str, timeout: int = 5):
        super().__init__(level=logging.WARNING)
        self.webhook_url = webhook_url
        self.timeout = timeout

    def emit(self, record: logging.LogRecord) -> None:
        # Only fire webhook for CRITICAL + alert=True
        if record.levelno < logging.CRITICAL:
            return
        if not getattr(record, "alert", False):
            return

        try:
            payload = self._build_payload(record)
            # Fire-and-forget in a daemon thread
            thread = threading.Thread(
                target=self._send, args=(payload,), daemon=True
            )
            thread.start()
        except Exception:
            self.handleError(record)

    def _build_payload(self, record: logging.LogRecord) -> dict:
        """Build a structured JSON payload from the log record."""
        return {
            "severity": record.levelname,
            "message": record.getMessage(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "logger": record.name,
            "service": getattr(record, "service", "unknown"),
            "anomaly_details": getattr(record, "anomaly_details", {}),
            "alert_statuses": getattr(record, "alert_statuses", []),
            "score": getattr(record, "score", None),
            "trace_id": getattr(record, "otelTraceID", ""),
            "span_id": getattr(record, "otelSpanID", ""),
        }

    def _send(self, payload: dict) -> None:
        """POST the payload to the webhook URL."""
        try:
            data = json.dumps(payload).encode("utf-8")
            req = Request(
                self.webhook_url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urlopen(req, timeout=self.timeout)
        except URLError as exc:
            # Log at debug to avoid recursive handler invocation
            logging.getLogger("webhook").debug(
                "Webhook POST failed: %s", exc
            )
        except Exception as exc:
            logging.getLogger("webhook").debug(
                "Webhook error: %s", exc
            )


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure the root logger with structured JSON output and trace context.

    Safe to call multiple times — subsequent calls are no-ops.

    If the ``ALERT_WEBHOOK_URL`` environment variable is set, a
    ``WebhookAlertHandler`` is attached to the root logger so that
    CRITICAL-level alert records are also POSTed to the webhook.

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

    # Attach webhook handler if configured
    webhook_url = os.environ.get("ALERT_WEBHOOK_URL")
    if webhook_url:
        webhook_handler = WebhookAlertHandler(webhook_url)
        root.addHandler(webhook_handler)
        logging.getLogger("observability").info(
            "WebhookAlertHandler attached (url=%s)", webhook_url
        )


def get_logger(name: str) -> logging.Logger:
    """Return a named logger (thin wrapper for discoverability)."""
    return logging.getLogger(name)
