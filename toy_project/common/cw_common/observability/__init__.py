"""
cw_common.observability — Centralised observability for CW services.

Submodules
----------
logging      Structured JSON logging with OTel trace-context injection.
metrics      Prometheus metric factories and helpers.
tracing      OpenTelemetry tracing (OTLP → Jaeger).
propagation  W3C TraceContext inject / extract for Kafka headers.
middleware   Reusable Starlette HTTP-metrics middleware.
testing      In-memory tracing exporter & metric-reset helpers for tests.

Quick start
-----------
::

    from cw_common.observability import init_observability, get_logger

    init_observability("my-service", "1.0.0")
    logger = get_logger("my-service")
"""

import logging as _logging
import os as _os

# ── logging ──────────────────────────────────────────────────────
from .logging import setup_logging, get_logger, JsonTraceFormatter

# ── metrics ──────────────────────────────────────────────────────
from .metrics import (
    create_counter,
    create_histogram,
    create_info,
    create_gauge,
    create_service_info,
    metrics_response,
)

# ── tracing ──────────────────────────────────────────────────────
from .tracing import init_tracing, shutdown_tracing

# ── propagation ──────────────────────────────────────────────────
from .propagation import (
    inject_trace_context,
    extract_trace_context,
    kafka_headers_to_dict,
    dict_to_kafka_headers,
)

# ── middleware ────────────────────────────────────────────────────
from .middleware import MetricsMiddleware

# ── testing ──────────────────────────────────────────────────────
from .testing import (
    setup_test_tracing,
    get_spans_by_name,
    find_span_links,
    reset_metrics,
)


# ── bootstrap ────────────────────────────────────────────────────

def init_observability(
    service_name: str,
    version: str,
    *,
    log_level: int = _logging.INFO,
    environment: str | None = None,
) -> None:
    """
    One-call bootstrap for logging, tracing, and service-info metrics.

    1. ``setup_logging(log_level)``
    2. ``init_tracing(service_name)`` — only when
       ``OTEL_EXPORTER_OTLP_ENDPOINT`` is set; failures are logged and
       swallowed so they never crash the service.
    3. ``create_service_info(service_name, version, environment)``

    Args:
        service_name: Identifier used in traces and the info metric.
        version: Semantic version of the service.
        log_level: Root log level (default ``INFO``).
        environment: Deployment env; defaults to ``$ENVIRONMENT`` or
            ``"development"``.
    """
    # 1 — structured JSON logging
    setup_logging(log_level)
    logger = get_logger(service_name)

    # 2 — distributed tracing (conditional)
    if _os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        try:
            init_tracing(service_name)
        except Exception as exc:
            logger.warning("Tracing init failed (non-fatal): %s", exc)
    else:
        logger.info("Tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set)")

    # 3 — Prometheus service-info metric
    create_service_info(
        service_name.replace("-", "_"),
        version,
        environment,
    )

    logger.info("Observability initialised for %s v%s", service_name, version)


__all__ = [
    # bootstrap
    "init_observability",
    # logging
    "setup_logging",
    "get_logger",
    "JsonTraceFormatter",
    # metrics
    "create_counter",
    "create_histogram",
    "create_info",
    "create_gauge",
    "create_service_info",
    "metrics_response",
    # tracing
    "init_tracing",
    "shutdown_tracing",
    # propagation
    "inject_trace_context",
    "extract_trace_context",
    "kafka_headers_to_dict",
    "dict_to_kafka_headers",
    # middleware
    "MetricsMiddleware",
    # testing
    "setup_test_tracing",
    "get_spans_by_name",
    "find_span_links",
    "reset_metrics",
]
