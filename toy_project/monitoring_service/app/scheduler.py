"""
Background alert scheduler — periodic anomaly detection on DB data.

Replaces Grafana-based alerting with an in-service polling loop that
runs every ``ALERT_CHECK_INTERVAL_SECONDS`` seconds:
  1. Query the DB for current status counts and rolling history.
  2. Run the ``PolicyAnomalyDetector``.
  3. Dispatch alerts via ``AlertDispatcher`` when anomalies are found.
  4. Update Prometheus gauges so dashboards stay current.
"""

import asyncio
import logging
import os

from app.database import get_status_counts_at, get_history_window
from app.detector import PolicyAnomalyDetector
from app.alerting import AlertDispatcher

logger = logging.getLogger("scheduler")

ALERT_CHECK_INTERVAL_SECONDS = int(
    os.environ.get("ALERT_CHECK_INTERVAL_SECONDS", "30")
)


def _update_anomaly_metrics(result) -> None:
    """Push anomaly detection results to Prometheus gauges."""
    try:
        from app.telemetry import (
            TRANSACTION_ANOMALY_SCORE,
            TRANSACTION_ALERTS_TOTAL,
            OVERALL_ANOMALY_SCORE,
        )

        OVERALL_ANOMALY_SCORE.set(result.max_z_score)

        for detail in result.anomalies:
            TRANSACTION_ANOMALY_SCORE.labels(status=detail.status).set(
                detail.z_score
            )
    except Exception as exc:
        logger.warning("Failed to update anomaly metrics: %s", exc)


def run_alert_check(
    detector: PolicyAnomalyDetector,
    dispatcher: AlertDispatcher,
) -> None:
    """Execute a single alert-check cycle (synchronous).

    This is extracted so it can be called directly in tests without
    needing the async loop.
    """
    current_counts = get_status_counts_at(minutes=1)
    history = get_history_window(minutes=60)

    if not current_counts and not history:
        logger.debug("No data in DB — skipping alert check")
        return

    if not current_counts and history:
        current_counts = history[-1]

    result = detector.detect(current_counts, history)

    # Always update Prometheus gauges regardless of severity
    _update_anomaly_metrics(result)

    if result.severity != "NORMAL":
        dispatcher.dispatch(result)
        logger.info(
            "Alert check: severity=%s  max_z=%.2f",
            result.severity,
            result.max_z_score,
        )
    else:
        logger.debug(
            "Alert check: NORMAL  max_z=%.2f",
            result.max_z_score,
        )


async def alert_loop(
    detector: PolicyAnomalyDetector | None = None,
    dispatcher: AlertDispatcher | None = None,
    interval: int | None = None,
) -> None:
    """Async loop that runs ``run_alert_check`` every *interval* seconds.

    Designed to be launched via ``asyncio.create_task`` inside the FastAPI
    lifespan context.
    """
    detector = detector or PolicyAnomalyDetector()
    dispatcher = dispatcher or AlertDispatcher()
    interval = interval or ALERT_CHECK_INTERVAL_SECONDS

    logger.info(
        "Background alert scheduler started (interval=%ds)", interval
    )

    while True:
        try:
            # Run the synchronous DB + detection work in a thread so we
            # don't block the event loop.
            await asyncio.to_thread(run_alert_check, detector, dispatcher)
        except asyncio.CancelledError:
            logger.info("Alert scheduler cancelled — shutting down")
            raise
        except Exception:
            logger.exception("Alert scheduler iteration failed")

        await asyncio.sleep(interval)
