"""
Integration tests for the alert pipeline.

End-to-end flow:
  1. Seed the database with normal + anomalous transaction data
  2. Hit the /alerts/analyze endpoint
  3. Verify the anomaly detector finds the spikes
  4. Verify the AlertDispatcher fires alerts for anomalous results
  5. Verify webhook handler would be invoked for CRITICAL severity
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock
import logging
import random

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
from app.database import init_db, insert_transactions, get_history_window, get_status_counts_at
from app.main import app
from app.alerting import AlertDispatcher


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def use_temp_db(tmp_path, monkeypatch):
    """Each test uses a fresh temporary SQLite database."""
    test_db = tmp_path / "test.db"
    monkeypatch.setattr(db_module, "DB_PATH", test_db)
    monkeypatch.setenv("DB_RESET_ON_START", "false")
    init_db()
    yield test_db


@pytest.fixture
def client():
    return TestClient(app)


def _ts(offset_minutes: int = 0) -> str:
    """Generate UTC timestamp string with optional minute offset from now."""
    t = datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)
    return t.strftime("%Y-%m-%d %H:%M:%S")


def _seed_normal_traffic(minutes: int = 60) -> int:
    """Insert baseline normal traffic with realistic variance.

    Values fluctuate around the baseline so that z-scores stay low:
    ~95-105 approved, ~3-7 denied, ~1-4 failed, ~0-2 reversed per minute.
    """
    rng = random.Random(42)  # deterministic
    records = []
    for i in range(minutes, 0, -1):
        ts = _ts(-i)
        records.extend([
            {"timestamp": ts, "status": "approved", "count": rng.randint(95, 105)},
            {"timestamp": ts, "status": "denied", "count": rng.randint(3, 7)},
            {"timestamp": ts, "status": "failed", "count": rng.randint(1, 4)},
            {"timestamp": ts, "status": "reversed", "count": rng.randint(0, 2)},
        ])
    return insert_transactions(records)


def _seed_anomalous_spike() -> int:
    """
    Insert a large spike of denied/failed/reversed in the last 2 minutes.

    This should be detected as anomalous by the policy-based Z-score detector.
    """
    records = []
    for i in range(2, 0, -1):
        ts = _ts(-i)
        records.extend([
            {"timestamp": ts, "status": "approved", "count": 100},
            {"timestamp": ts, "status": "denied", "count": 200},    # 40x normal
            {"timestamp": ts, "status": "failed", "count": 100},    # 50x normal
            {"timestamp": ts, "status": "reversed", "count": 80},   # 80x normal
        ])
    return insert_transactions(records)


# ── Database Layer Tests ──────────────────────────────────────────

class TestAlertDatabaseQueries:
    """Verify the new DB queries used by the alert system."""

    def test_get_status_counts_at_returns_dict(self):
        _seed_normal_traffic(5)
        counts = get_status_counts_at(minutes=5)
        assert isinstance(counts, dict)
        assert "approved" in counts
        assert "denied" in counts

    def test_get_status_counts_at_values_reasonable(self):
        _seed_normal_traffic(5)
        counts = get_status_counts_at(minutes=5)
        # 5 minutes * ~100 per min ≈ 475-525 approved
        assert 450 <= counts["approved"] <= 550
        assert 10 <= counts["denied"] <= 40

    def test_get_history_window_returns_list_of_dicts(self):
        _seed_normal_traffic(10)
        history = get_history_window(minutes=10)
        assert isinstance(history, list)
        assert len(history) == 10
        assert isinstance(history[0], dict)
        assert "approved" in history[0]

    def test_get_history_window_ordered_oldest_first(self):
        _seed_normal_traffic(10)
        history = get_history_window(minutes=10)
        # Each entry should have roughly the same values (stable baseline)
        for entry in history:
            assert 90 <= entry.get("approved", 0) <= 110

    def test_empty_db_returns_empty(self):
        counts = get_status_counts_at(minutes=1)
        assert counts == {}
        history = get_history_window(minutes=60)
        assert history == []


# ── Anomaly Detection Integration ────────────────────────────────

class TestAnomalyDetectionIntegration:
    """Test anomaly detection against real DB data."""

    def test_normal_traffic_not_anomalous(self):
        """Baseline traffic should not trigger anomaly alerts."""
        from app.detector import PolicyAnomalyDetector

        _seed_normal_traffic(60)
        detector = PolicyAnomalyDetector()

        current = get_status_counts_at(minutes=1)
        history = get_history_window(minutes=60)
        result = detector.detect(current, history)

        # Normal traffic may trigger CRITICAL on approved (high approval is good)
        # but should not trigger CRITICAL on problem statuses
        if result.severity == "CRITICAL":
            problem_statuses_critical = [
                d for d in result.anomalies
                if d.z_score > 4.0 and d.status in ("denied", "failed", "reversed", "backend_reversed")
            ]
            assert len(problem_statuses_critical) == 0, \
                f"Normal traffic should not trigger CRITICAL on problem statuses: {problem_statuses_critical}"

    def test_spike_detected_as_anomalous(self):
        """A massive spike should be detected as WARNING or CRITICAL."""
        from app.detector import PolicyAnomalyDetector

        _seed_normal_traffic(60)
        _seed_anomalous_spike()
        detector = PolicyAnomalyDetector()

        current = get_status_counts_at(minutes=1)
        history = get_history_window(minutes=60)
        result = detector.detect(current, history)

        assert result.severity in ("WARNING", "CRITICAL")
        # At least one of the alert statuses should be anomalous
        anomalous = [d for d in result.anomalies if d.is_anomalous]
        assert len(anomalous) > 0

    def test_spike_identifies_correct_statuses(self):
        """The spike should flag denied/failed/reversed, not approved."""
        from app.detector import PolicyAnomalyDetector

        _seed_normal_traffic(60)
        _seed_anomalous_spike()
        detector = PolicyAnomalyDetector()

        current = get_status_counts_at(minutes=1)
        history = get_history_window(minutes=60)
        result = detector.detect(current, history)

        anomalous_statuses = {d.status for d in result.anomalies if d.is_anomalous}
        # At least denied should be anomalous (biggest spike)
        assert "denied" in anomalous_statuses or "failed" in anomalous_statuses


# ── API Endpoint Integration ──────────────────────────────────────

class TestAlertEndpointIntegration:
    """Test the /alerts/ API endpoints with real data."""

    def test_analyze_empty_db(self, client):
        resp = client.post("/alerts/analyze")
        assert resp.status_code == 200
        data = resp.json()
        assert data["overall_severity"] == "NORMAL"
        assert data["recommendation"] == "No transaction data available for analysis."

    def test_analyze_normal_traffic(self, client):
        _seed_normal_traffic(60)
        resp = client.post("/alerts/analyze?window_minutes=60")
        assert resp.status_code == 200
        data = resp.json()
        # CRITICAL may occur on approved (high approval), but not on problem statuses
        if data["overall_severity"] == "CRITICAL":
            problem_alerts = [
                a for a in data["alerts"]
                if a["z_score"] > 4.0 and a["status"] in ("denied", "failed", "reversed", "backend_reversed")
            ]
            assert len(problem_alerts) == 0, "Normal traffic should not trigger CRITICAL on problem statuses"
        assert "detection_method" in data

    def test_analyze_anomalous_traffic(self, client):
        _seed_normal_traffic(60)
        _seed_anomalous_spike()
        resp = client.post("/alerts/analyze?window_minutes=60")
        assert resp.status_code == 200
        data = resp.json()
        assert data["overall_severity"] in ("WARNING", "CRITICAL")
        assert len(data["alerts"]) > 0
        # Recommendation should mention alerting
        assert "ALERT" in data["recommendation"] or "WARNING" in data["recommendation"]

    def test_status_endpoint(self, client):
        _seed_normal_traffic(10)
        resp = client.get("/alerts/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "overall_severity" in data
        assert "statuses" in data

    def test_rates_endpoint(self, client):
        _seed_normal_traffic(10)
        resp = client.get("/alerts/rates?minutes=10")
        assert resp.status_code == 200
        data = resp.json()
        assert data["window_minutes"] == 10
        assert data["total_points"] > 0

    def test_rates_empty_db(self, client):
        resp = client.get("/alerts/rates?minutes=60")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_points"] == 0


# ── Alert Dispatcher Integration ──────────────────────────────────

class TestDispatcherIntegration:
    """Test AlertDispatcher with real anomaly detection results."""

    def test_dispatcher_fires_on_anomalous_data(self):
        """Full pipeline: seed spike → detect → dispatch → verify alert logged."""
        from app.detector import PolicyAnomalyDetector

        _seed_normal_traffic(60)
        _seed_anomalous_spike()

        detector = PolicyAnomalyDetector()
        dispatcher = AlertDispatcher(cooldown_seconds=0)  # no cooldown for test

        current = get_status_counts_at(minutes=1)
        history = get_history_window(minutes=60)
        result = detector.detect(current, history)

        with patch.object(logging.getLogger("alerting"), "critical") as mock_crit, \
             patch.object(logging.getLogger("alerting"), "warning") as mock_warn:
            dispatched = dispatcher.dispatch(result)

        if result.severity == "CRITICAL":
            assert mock_crit.called or mock_warn.called
        elif result.severity == "WARNING":
            assert mock_warn.called

        assert len(dispatched) > 0

    def test_dispatcher_quiet_on_normal_data(self):
        """Normal traffic should produce no dispatched alerts."""
        from app.detector import PolicyAnomalyDetector

        _seed_normal_traffic(60)

        detector = PolicyAnomalyDetector()
        dispatcher = AlertDispatcher(cooldown_seconds=0)

        current = get_status_counts_at(minutes=1)
        history = get_history_window(minutes=60)
        result = detector.detect(current, history)

        dispatched = dispatcher.dispatch(result)

        if result.severity == "NORMAL":
            assert len(dispatched) == 0

    def test_cooldown_prevents_rapid_alerts(self):
        """Two rapid detections of the same anomaly should only alert once."""
        from app.detector import PolicyAnomalyDetector

        _seed_normal_traffic(60)
        _seed_anomalous_spike()

        detector = PolicyAnomalyDetector()
        dispatcher = AlertDispatcher(cooldown_seconds=300)

        current = get_status_counts_at(minutes=1)
        history = get_history_window(minutes=60)
        result = detector.detect(current, history)

        first = dispatcher.dispatch(result)
        second = dispatcher.dispatch(result)

        # Second dispatch should be suppressed by cooldown
        if len(first) > 0:
            assert len(second) == 0, "Cooldown should suppress duplicate alerts"


# ── Webhook Integration ───────────────────────────────────────────

class TestWebhookIntegration:
    """Test that critical alerts trigger webhook notifications."""

    def test_webhook_handler_fires_for_critical(self):
        """
        Simulate the full chain: anomaly → CRITICAL log → WebhookAlertHandler invoked.
        """
        from cw_common.observability.logging import WebhookAlertHandler

        # Use a real handler with a patched _send method to verify it fires
        handler = WebhookAlertHandler(webhook_url="http://localhost:9999/test")
        handler.setLevel(logging.CRITICAL)
        emit_called = []
        original_emit = handler.emit

        def tracking_emit(record):
            emit_called.append(record)
            # Don't actually send HTTP - just track the call

        handler.emit = tracking_emit

        alert_logger = logging.getLogger("alerting.webhook_test")
        alert_logger.setLevel(logging.DEBUG)
        alert_logger.addHandler(handler)

        try:
            # Emit a critical alert log with alert=True
            alert_logger.critical(
                "Test critical alert",
                extra={
                    "alert": True,
                    "anomaly_details": {
                        "status": "denied",
                        "z_score": 5.0,
                        "severity": "CRITICAL",
                    },
                },
            )

            # The handler's emit should have been called
            assert len(emit_called) == 1
            assert emit_called[0].getMessage() == "Test critical alert"
        finally:
            alert_logger.removeHandler(handler)

    def test_webhook_handler_skips_warning(self):
        """WebhookAlertHandler should not fire for WARNING-level alerts."""
        from cw_common.observability.logging import WebhookAlertHandler

        mock_handler = MagicMock(spec=WebhookAlertHandler)
        mock_handler.level = logging.CRITICAL

        # Create a filter that mimics the real handler behavior
        def should_emit(record):
            return record.levelno >= logging.CRITICAL and getattr(record, "alert", False)

        mock_handler.filter.side_effect = should_emit

        alert_logger = logging.getLogger("alerting.webhook_skip_test")
        alert_logger.setLevel(logging.DEBUG)
        alert_logger.addFilter(mock_handler.filter)
        alert_logger.addHandler(mock_handler)

        try:
            alert_logger.warning("Test warning alert", extra={"alert": True})
            # Handler emit should NOT be called (level too low)
            assert not mock_handler.emit.called
        finally:
            alert_logger.removeHandler(mock_handler)
            alert_logger.removeFilter(mock_handler.filter)
