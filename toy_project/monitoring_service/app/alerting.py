"""
Alert dispatcher — bridges anomaly detection results to logging/metrics/notifications.

The dispatcher:
  1. Evaluates anomaly detection results
  2. Logs at WARNING (Grafana-visible via Loki) or CRITICAL (Grafana + webhook)
  3. Updates Prometheus alert counters
  4. Enforces a cooldown to prevent alert storms

Severity routing:
  - WARNING → Grafana only (Loki log query picks up level="WARNING" + alert="true")
  - CRITICAL → Grafana + webhook (WebhookAlertHandler in logging.py fires the POST)
"""

import logging
import time
from datetime import datetime, timezone

from opentelemetry import trace

logger = logging.getLogger("alerting")

# Default cooldown: don't re-alert same status+severity within this many seconds
DEFAULT_COOLDOWN_SECONDS = 300  # 5 minutes


class AlertDispatcher:
    """
    Dispatches anomaly detection results as structured log alerts.

    Args:
        cooldown_seconds: Minimum seconds between alerts for the same
            status+severity combination.
    """

    def __init__(self, cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS):
        self.cooldown_seconds = cooldown_seconds
        # Tracks last alert time per (status, severity) key
        self._last_alerted: dict[tuple[str, str], float] = {}

    def dispatch(self, result) -> list[dict]:
        """
        Process an AnomalyResult and fire alerts as needed.

        Args:
            result: An ``AnomalyResult`` from the anomaly detector.

        Returns:
            List of alert dicts that were actually dispatched (after cooldown filtering).
        """
        dispatched = []

        if result.severity == "NORMAL":
            return dispatched

        tracer = trace.get_tracer(__name__)

        for detail in result.anomalies:
            if not detail.is_anomalous:
                continue

            severity = self._severity_for_detail(detail)
            key = (detail.status, severity)

            # Cooldown check
            now = time.monotonic()
            last = self._last_alerted.get(key, 0)
            if (now - last) < self.cooldown_seconds:
                continue

            self._last_alerted[key] = now

            alert_info = {
                "status": detail.status,
                "severity": severity,
                "current_value": detail.current_value,
                "baseline_mean": detail.baseline_mean,
                "baseline_std": detail.baseline_std,
                "z_score": detail.z_score,
                "score": result.max_z_score,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            with tracer.start_as_current_span(
                "dispatch anomaly alert",
                attributes={
                    "alert.status": detail.status,
                    "alert.severity": severity,
                    "alert.current_value": detail.current_value,
                    "alert.z_score": detail.z_score,
                    "alert.score": result.max_z_score,
                },
            ):
                if severity == "CRITICAL":
                    logger.critical(
                        "ALERT: %s transactions anomaly — count=%d, z_score=%.2f "
                        "(baseline mean=%.2f, std=%.2f)",
                        detail.status,
                        detail.current_value,
                        detail.z_score,
                        detail.baseline_mean,
                        detail.baseline_std,
                        extra={
                            "alert": True,
                            "anomaly_details": alert_info,
                            "alert_statuses": [detail.status],
                            "score": result.max_z_score,
                            "service": "monitoring-api",
                        },
                    )
                elif severity == "WARNING":
                    logger.warning(
                        "ELEVATED: %s transactions above normal — count=%d, z_score=%.2f "
                        "(baseline mean=%.2f, std=%.2f)",
                        detail.status,
                        detail.current_value,
                        detail.z_score,
                        detail.baseline_mean,
                        detail.baseline_std,
                        extra={
                            "alert": True,
                            "anomaly_details": alert_info,
                            "alert_statuses": [detail.status],
                            "score": result.max_z_score,
                            "service": "monitoring-api",
                        },
                    )

            # Update Prometheus alert counter
            self._update_alert_counter(detail.status, severity)

            dispatched.append(alert_info)

        return dispatched

    def _severity_for_detail(self, detail) -> str:
        """Map a detail's z-score to severity."""
        if abs(detail.z_score) > 4.0:
            return "CRITICAL"
        elif abs(detail.z_score) > 2.5:
            return "WARNING"
        return "NORMAL"

    def _update_alert_counter(self, status: str, severity: str) -> None:
        """Increment the Prometheus alert counter."""
        try:
            from app.telemetry import TRANSACTION_ALERTS_TOTAL
            TRANSACTION_ALERTS_TOTAL.labels(status=status, severity=severity).inc()
        except Exception:
            pass  # Metrics not available (e.g., in tests)

    def reset_cooldowns(self) -> None:
        """Clear all cooldown timers (useful for testing)."""
        self._last_alerted.clear()
