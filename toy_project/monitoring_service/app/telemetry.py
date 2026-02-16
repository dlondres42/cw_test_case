"""
Service-specific telemetry for monitoring-api.

Domain metrics and FastAPI instrumentation that sit on top of the
shared ``cw_common.observability`` module.
"""

import logging

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from cw_common.observability import (
    create_counter,
    create_gauge,
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

# ── Business Metrics (per-status) ────────────────────────────────

TRANSACTIONS_BY_STATUS = create_counter(
    "transactions_by_status_total",
    "Total transaction count by status",
    ["status"],
)

TRANSACTION_STATUS_RATE = create_gauge(
    "transaction_status_rate",
    "Current per-minute transaction rate by status",
    ["status"],
)

TRANSACTION_ANOMALY_SCORE = create_gauge(
    "transaction_anomaly_score",
    "Current anomaly score by status (lower = more anomalous)",
    ["status"],
)

TRANSACTION_ALERTS_TOTAL = create_counter(
    "transaction_alerts_total",
    "Total anomaly alerts fired by status and severity",
    ["status", "severity"],
)

OVERALL_ANOMALY_SCORE = create_gauge(
    "overall_anomaly_score",
    "Overall anomaly score from the Isolation Forest model",
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
        consumer.transactions_by_status_counter = TRANSACTIONS_BY_STATUS
        consumer.transaction_status_rate_gauge = TRANSACTION_STATUS_RATE
    except ImportError:
        pass

    # Wire anomaly detector + alert dispatcher into consumer
    try:
        from app import consumer as _consumer
        from anomaly_model.model import AnomalyDetector
        from app.alerting import AlertDispatcher

        _consumer.anomaly_detector = AnomalyDetector()
        _consumer.alert_dispatcher = AlertDispatcher()
        logger.info("Anomaly detector + alert dispatcher wired into consumer")
    except Exception as e:
        logger.warning("Anomaly detector init skipped: %s", e)

    # FastAPI auto-instrumentation (creates spans for every route)
    try:
        FastAPIInstrumentor.instrument_app(app)
        logger.info("FastAPI instrumentation enabled")
    except Exception as e:
        logger.warning("FastAPI instrumentation failed: %s", e)

    logger.info("Service telemetry initialised")
