"""
Alert routes — anomaly analysis and status endpoints.

Provides the core alerting API:
  POST /alerts/analyze  — Run anomaly detection and return recommendations
  GET  /alerts/status   — Lightweight current anomaly status check
  GET  /alerts/rates    — Status rate time series for visualization
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Query

from app.database import get_status_counts_at, get_history_window, get_status_rates
from app.models.alerts import (
    AnomalyAlert,
    AlertResponse,
    AlertStatusResponse,
    StatusRateRecord,
    StatusRateResponse,
)

logger = logging.getLogger("alerts")

router = APIRouter(prefix="/alerts", tags=["Alerts"])

# Lazy-loaded detector — initialized on first use
_detector = None


def _get_detector():
    """Lazy-load the anomaly detector (avoids import-time model loading)."""
    global _detector
    if _detector is None:
        try:
            from anomaly_model.model import AnomalyDetector
            _detector = AnomalyDetector()
            logger.info("AnomalyDetector loaded (model: %s)",
                        "ML" if _detector._model else "rule-only")
        except Exception as exc:
            logger.warning("Failed to load AnomalyDetector: %s", exc)
            # Create a minimal rule-only detector
            from anomaly_model.model import AnomalyDetector
            _detector = AnomalyDetector()
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
    the Isolation Forest model (or rule-based fallback), and returns
    per-status anomaly alerts with a severity grade and recommendation.
    """
    detector = _get_detector()

    # Fetch current counts and history
    current_counts = get_status_counts_at(minutes=1)
    history = get_history_window(minutes=window_minutes)

    if not current_counts and not history:
        return AlertResponse(
            timestamp=datetime.utcnow(),
            overall_score=0.0,
            overall_severity="NORMAL",
            alerts=[],
            recommendation="No transaction data available for analysis.",
            window_minutes=window_minutes,
            model_mode="rule" if detector._model is None else "ml",
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
            score=result.score,
            current_value=detail.current_value,
            baseline_mean=detail.baseline_mean,
            baseline_std=detail.baseline_std,
            z_score=detail.z_score,
            is_anomalous=detail.is_anomalous,
            message=detail.contribution,
        ))

    recommendation = _build_recommendation(result.severity, anomalous_statuses)

    return AlertResponse(
        timestamp=result.timestamp,
        overall_score=result.score,
        overall_severity=result.severity,
        alerts=alerts,
        recommendation=recommendation,
        window_minutes=window_minutes,
        model_mode="rule" if detector._model is None else "ml",
    )


@router.get("/status", response_model=AlertStatusResponse)
def alert_status():
    """Lightweight current anomaly status — no full analysis, just z-scores."""
    detector = _get_detector()
    current_counts = get_status_counts_at(minutes=1)
    history = get_history_window(minutes=60)

    if not current_counts and not history:
        return AlertStatusResponse(
            timestamp=datetime.utcnow(),
            overall_severity="NORMAL",
            overall_score=0.0,
            statuses={},
        )

    if not current_counts and history:
        current_counts = history[-1]

    result = detector.detect(current_counts, history)

    statuses = {d.status: d.z_score for d in result.anomalies}

    return AlertStatusResponse(
        timestamp=result.timestamp,
        overall_severity=result.severity,
        overall_score=result.score,
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
    try:
        from app.telemetry import (
            TRANSACTION_ANOMALY_SCORE,
            TRANSACTION_ALERTS_TOTAL,
            OVERALL_ANOMALY_SCORE,
        )

        OVERALL_ANOMALY_SCORE.set(result.score)

        for detail in result.anomalies:
            TRANSACTION_ANOMALY_SCORE.labels(status=detail.status).set(detail.z_score)
            if detail.is_anomalous:
                sev = _severity_for_status(detail.z_score)
                TRANSACTION_ALERTS_TOTAL.labels(
                    status=detail.status, severity=sev
                ).inc()
    except Exception as exc:
        logger.warning("Failed to update alert metrics: %s", exc)
