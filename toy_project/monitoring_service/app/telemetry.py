"""
Service-specific telemetry for monitoring-api.

Domain metrics and FastAPI instrumentation that sit on top of the
shared ``cw_common.observability`` module.
"""

import logging

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from cw_common.observability import (
    create_counter,
    create_histogram,
    MetricsMiddleware,
)

logger = logging.getLogger("telemetry")

# ── Metrics (Prometheus) ──────────────────────────────────────────

MESSAGES_CONSUMED = create_counter(
    "messages_consumed_total",
    "Total number of transaction records consumed from Kafka",
)

CONSUME_DURATION = create_histogram(
    "consume_batch_duration_seconds",
    "Time spent processing a consumed Kafka batch",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

HTTP_REQUESTS = create_counter(
    "http_requests_total",
    "Total HTTP requests by method and path",
    ["method", "path", "status"],
)

INSERT_DURATION = create_histogram(
    "db_insert_duration_seconds",
    "Time spent inserting records into SQLite",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
)


# ── Initialization ───────────────────────────────────────────────

def init(app):
    """Wire service-specific telemetry into the FastAPI app.

    * Adds the HTTP-metrics middleware.
    * Wires Kafka-consumer metric counters.
    * Instruments FastAPI with OpenTelemetry auto-instrumentation.
    """
    # HTTP request counting via shared middleware
    app.add_middleware(MetricsMiddleware, counter=HTTP_REQUESTS)

    # Wire metrics into the consumer module
    try:
        from app import consumer
        consumer.messages_consumed_counter = MESSAGES_CONSUMED
        consumer.consume_duration_histogram = CONSUME_DURATION
        consumer.insert_duration_histogram = INSERT_DURATION
    except ImportError:
        pass

    # FastAPI auto-instrumentation (creates spans for every route)
    try:
        FastAPIInstrumentor.instrument_app(app)
        logger.info("FastAPI instrumentation enabled")
    except Exception as e:
        logger.warning("FastAPI instrumentation failed: %s", e)

    logger.info("Service telemetry initialised")
