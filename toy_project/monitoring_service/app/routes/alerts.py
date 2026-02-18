"""
Alert routes — anomaly analysis, status, and evaluation endpoints.

Provides the core alerting API:
  POST /alerts/analyze   — Run anomaly detection and return recommendations
  GET  /alerts/status    — Lightweight current anomaly status check
  GET  /alerts/rates     — Status rate time series for visualization
  POST /alerts/evaluate  — Evaluate a single transaction count's severity
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from app.database import (
    get_status_counts_at,
    get_history_window,
    get_status_rates,
    insert_transactions,
)
from app.detector import PolicyAnomalyDetector, ALERT_STATUSES, AnomalyDetail, AnomalyResult
from app.alerting import AlertDispatcher
from app.models.alerts import (
    AnomalyAlert,
    AlertResponse,
    AlertStatusResponse,
    StatusRateRecord,
    StatusRateResponse,
    TransactionEvaluateRequest,
    TransactionEvaluateResponse,
)

logger = logging.getLogger("alerts")

router = APIRouter(prefix="/alerts", tags=["Alerts"])

# Lazy-loaded detector — initialized on first use
_detector = None


def _get_detector() -> PolicyAnomalyDetector:
    """Lazy-load the policy-based anomaly detector."""
    global _detector
    if _detector is None:
        _detector = PolicyAnomalyDetector()
        logger.info("PolicyAnomalyDetector loaded")
    return _detector


def _severity_for_status(z_score: float, threshold: float = 2.5) -> str:
    """Map a z-score to a severity label."""
    if abs(z_score) > 4.0:
        return "CRITICAL"
    elif abs(z_score) > threshold:
        return "WARNING"
    return "NORMAL"


def _build_recommendation(severity: str, anomalous_statuses: list[str]) -> str:
    """Generate a human-readable recommendation string."""
    if severity == "CRITICAL":
        return (
            f"ALERT: Critical anomaly detected in {', '.join(anomalous_statuses)}. "
            "Immediate investigation recommended. Check payment gateway status "
            "and system health."
        )
    elif severity == "WARNING":
        return (
            f"WARNING: Elevated anomaly in {', '.join(anomalous_statuses)}. "
            "Monitor closely. Consider pre-emptive investigation if trend continues."
        )
    return "All transaction statuses within normal parameters."


@router.post("/analyze", response_model=AlertResponse)
def analyze_transactions(
    window_minutes: int = Query(default=60, ge=5, le=1440),
):
    """
    Run anomaly detection on recent transaction data.

    Fetches the last ``window_minutes`` of data from the DB, runs
    the policy-based Z-score detector, and returns per-status anomaly
    alerts with a severity grade and recommendation.
    """
    detector = _get_detector()

    # Fetch current counts and history
    current_counts = get_status_counts_at(minutes=1)
    history = get_history_window(minutes=window_minutes)

    if not current_counts and not history:
        return AlertResponse(
            timestamp=datetime.now(timezone.utc),
            max_z_score=0.0,
            overall_severity="NORMAL",
            alerts=[],
            recommendation="No transaction data available for analysis.",
            window_minutes=window_minutes,
            detection_method="policy_zscore",
        )

    # If we have history but no separate current counts, use the last history entry
    if not current_counts and history:
        current_counts = history[-1]

    # Run anomaly detection
    result = detector.detect(current_counts, history)

    # Update Prometheus metrics
    _update_metrics(result)

    # Build response
    alerts = []
    anomalous_statuses = []
    for detail in result.anomalies:
        sev = _severity_for_status(detail.z_score)
        if detail.is_anomalous:
            anomalous_statuses.append(detail.status)
        alerts.append(AnomalyAlert(
            status=detail.status,
            severity=sev,
            z_score=detail.z_score,
            current_value=detail.current_value,
            baseline_mean=detail.baseline_mean,
            baseline_std=detail.baseline_std,
            is_anomalous=detail.is_anomalous,
            message=detail.contribution,
        ))

    recommendation = _build_recommendation(result.severity, anomalous_statuses)

    return AlertResponse(
        timestamp=result.timestamp,
        max_z_score=result.max_z_score,
        overall_severity=result.severity,
        alerts=alerts,
        recommendation=recommendation,
        window_minutes=window_minutes,
        detection_method="policy_zscore",
    )


@router.get("/status", response_model=AlertStatusResponse)
def alert_status():
    """Lightweight current anomaly status — no full analysis, just z-scores."""
    detector = _get_detector()
    current_counts = get_status_counts_at(minutes=1)
    history = get_history_window(minutes=60)

    if not current_counts and not history:
        return AlertStatusResponse(
            timestamp=datetime.now(timezone.utc),
            overall_severity="NORMAL",
            max_z_score=0.0,
            statuses={},
        )

    if not current_counts and history:
        current_counts = history[-1]

    result = detector.detect(current_counts, history)

    statuses = {d.status: d.z_score for d in result.anomalies}

    return AlertStatusResponse(
        timestamp=result.timestamp,
        overall_severity=result.severity,
        max_z_score=result.max_z_score,
        statuses=statuses,
    )


@router.get("/rates", response_model=StatusRateResponse)
def status_rates(
    minutes: int = Query(default=60, ge=1, le=1440),
):
    """Get per-minute status rate time series for visualization."""
    rows = get_status_rates(minutes)
    data = [StatusRateRecord(**row) for row in rows]
    return StatusRateResponse(
        window_minutes=minutes,
        data=data,
        total_points=len(data),
    )


def _update_metrics(result):
    """Push anomaly detection results to Prometheus gauges/counters."""
    from app.scheduler import _update_anomaly_metrics

    _update_anomaly_metrics(result)


@router.post("/evaluate", response_model=TransactionEvaluateResponse)
def evaluate_transaction(body: TransactionEvaluateRequest):
    """
    Evaluate a single transaction status count against the rolling baseline.

    Accepts a ``{status, count, timestamp?}`` payload, evaluates severity,
    and if anomalous:
    - Dispatches an alert to the configured webhook
    - Inserts the record into the database

    Returns the Z-score severity assessment with dispatch/insertion status.
    """
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span(
        "POST /alerts/evaluate",
        kind=SpanKind.SERVER,
        attributes={
            "http.method": "POST",
            "http.route": "/alerts/evaluate",
            "transaction.status": body.status,
            "transaction.count": body.count,
        }
    ) as span:
        if body.status not in ALERT_STATUSES:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Status '{body.status}' is not monitored. "
                    f"Valid statuses: {', '.join(ALERT_STATUSES)}"
                ),
            )

        # Use provided timestamp or current time
        timestamp_str = body.timestamp
        if not timestamp_str:
            from datetime import datetime, timezone
            timestamp_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        span.set_attribute("transaction.timestamp", timestamp_str)

        detector = _get_detector()
        
        # Fetch history window with span
        with tracer.start_as_current_span(
            "fetch history window",
            kind=SpanKind.INTERNAL,
            attributes={"window.minutes": 60}
        ) as history_span:
            history = get_history_window(minutes=60)
            history_span.set_attribute("history.records", len(history))

        # Evaluate anomaly with span
        with tracer.start_as_current_span(
            "evaluate anomaly",
            kind=SpanKind.INTERNAL,
            attributes={
                "detector.type": "PolicyAnomalyDetector",
                "evaluation.status": body.status,
                "evaluation.count": body.count,
            }
        ) as eval_span:
            result = detector.evaluate_single(body.status, body.count, history)
            eval_span.set_attribute("anomaly.severity", result["severity"])
            eval_span.set_attribute("anomaly.z_score", result["z_score"])
            eval_span.set_attribute("anomaly.is_anomalous", result["is_anomalous"])

        # Update Prometheus metrics (so manual evaluations appear in Grafana)
        try:
            from app.telemetry import TRANSACTIONS_BY_STATUS, TRANSACTION_STATUS_RATE
            TRANSACTIONS_BY_STATUS.labels(status=body.status).inc(body.count)
            TRANSACTION_STATUS_RATE.labels(status=body.status).set(body.count)
        except Exception as e:
            logger.debug("Failed to update metrics: %s", e)

        # Track side-effects
        alert_dispatched = False
        db_inserted = False

        # If anomalous, dispatch alert and insert to DB
        if result["is_anomalous"]:
            # Create AnomalyDetail and AnomalyResult for dispatching
            detail = AnomalyDetail(
                status=body.status,
                current_value=body.count,
                baseline_mean=result["baseline_mean"],
                baseline_std=result["baseline_std"],
                z_score=result["z_score"],
                is_anomalous=True,
                contribution=result["message"],
            )
            anomaly_result = AnomalyResult(
                max_z_score=abs(result["z_score"]),
                severity=result["severity"],
                anomalies=[detail],
                timestamp=datetime.now(timezone.utc),
            )

            # Dispatch alert via webhook with span
            with tracer.start_as_current_span(
                "dispatch alert",
                kind=SpanKind.INTERNAL,
                attributes={
                    "alert.severity": result["severity"],
                    "alert.status": body.status,
                    "alert.z_score": result["z_score"],
                    "alert.count": body.count,
                }
            ) as dispatch_span:
                try:
                    dispatcher = AlertDispatcher()
                    dispatched = dispatcher.dispatch(anomaly_result)
                    alert_dispatched = len(dispatched) > 0
                    dispatch_span.set_attribute("alert.dispatched", alert_dispatched)
                    dispatch_span.set_attribute("alert.webhook_count", len(dispatched))
                    logger.info(
                        "Alert dispatched for %s: severity=%s, z_score=%.2f",
                        body.status,
                        result["severity"],
                        result["z_score"],
                    )
                except Exception as e:
                    dispatch_span.set_attribute("error", True)
                    logger.warning("Failed to dispatch alert: %s", e)

            # Insert to database with span
            with tracer.start_as_current_span(
                "insert anomalous transaction",
                kind=SpanKind.INTERNAL,
                attributes={
                    "db.operation": "INSERT",
                    "transaction.status": body.status,
                    "transaction.count": body.count,
                    "transaction.timestamp": timestamp_str,
                }
            ) as insert_span:
                try:
                    record = {
                        "timestamp": timestamp_str,
                        "status": body.status,
                        "count": body.count,
                    }
                    insert_transactions([record])
                    db_inserted = True
                    insert_span.set_attribute("db.inserted", True)
                    logger.info(
                        "Inserted anomalous transaction: %s at %s (count=%d)",
                        body.status,
                        timestamp_str,
                        body.count,
                    )
                except Exception as e:
                    insert_span.set_attribute("error", True)
                    logger.warning("Failed to insert transaction: %s", e)

        return TransactionEvaluateResponse(
            status=body.status,
            severity=result["severity"],
            z_score=result["z_score"],
            is_anomalous=result["is_anomalous"],
            baseline_mean=result["baseline_mean"],
            baseline_std=result["baseline_std"],
            message=result["message"],
            timestamp=timestamp_str,
            alert_dispatched=alert_dispatched,
            db_inserted=db_inserted,
        )
