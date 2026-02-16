"""Tests for the AlertDispatcher."""

import logging
import time

import pytest

from app.alerting import AlertDispatcher


class MockAnomalyDetail:
    """Lightweight mock of AnomalyDetail."""

    def __init__(self, status, current_value, baseline_mean, baseline_std, z_score, is_anomalous, contribution=""):
        self.status = status
        self.current_value = current_value
        self.baseline_mean = baseline_mean
        self.baseline_std = baseline_std
        self.z_score = z_score
        self.is_anomalous = is_anomalous
        self.contribution = contribution


class MockAnomalyResult:
    """Lightweight mock of AnomalyResult."""

    def __init__(self, score, severity, anomalies):
        self.score = score
        self.severity = severity
        self.anomalies = anomalies


def _make_critical_result():
    return MockAnomalyResult(
        score=-0.2,
        severity="CRITICAL",
        anomalies=[
            MockAnomalyDetail("denied", 45, 5.0, 2.0, 8.5, True),
            MockAnomalyDetail("failed", 12, 1.0, 0.5, 5.2, True),
            MockAnomalyDetail("reversed", 1, 1.0, 0.5, 0.5, False),
            MockAnomalyDetail("backend_reversed", 0, 0.0, 0.5, 0.0, False),
        ],
    )


def _make_warning_result():
    return MockAnomalyResult(
        score=-0.08,
        severity="WARNING",
        anomalies=[
            MockAnomalyDetail("denied", 15, 5.0, 2.0, 3.0, True),
            MockAnomalyDetail("failed", 1, 1.0, 0.5, 0.5, False),
            MockAnomalyDetail("reversed", 1, 1.0, 0.5, 0.5, False),
            MockAnomalyDetail("backend_reversed", 0, 0.0, 0.5, 0.0, False),
        ],
    )


def _make_normal_result():
    return MockAnomalyResult(
        score=0.2,
        severity="NORMAL",
        anomalies=[
            MockAnomalyDetail("denied", 5, 5.0, 2.0, 0.3, False),
            MockAnomalyDetail("failed", 1, 1.0, 0.5, 0.2, False),
        ],
    )


class TestAlertDispatcher:
    def test_normal_result_no_alerts(self):
        """NORMAL results should not dispatch any alerts."""
        dispatcher = AlertDispatcher(cooldown_seconds=0)
        result = _make_normal_result()
        dispatched = dispatcher.dispatch(result)
        assert dispatched == []

    def test_critical_dispatches_alerts(self):
        """CRITICAL result should dispatch alerts for anomalous statuses."""
        dispatcher = AlertDispatcher(cooldown_seconds=0)
        result = _make_critical_result()
        dispatched = dispatcher.dispatch(result)
        assert len(dispatched) == 2  # denied + failed
        statuses = {d["status"] for d in dispatched}
        assert "denied" in statuses
        assert "failed" in statuses

    def test_warning_dispatches_alerts(self):
        """WARNING result should dispatch for anomalous statuses."""
        dispatcher = AlertDispatcher(cooldown_seconds=0)
        result = _make_warning_result()
        dispatched = dispatcher.dispatch(result)
        assert len(dispatched) == 1
        assert dispatched[0]["status"] == "denied"

    def test_alert_fields(self):
        """Dispatched alerts should contain required fields."""
        dispatcher = AlertDispatcher(cooldown_seconds=0)
        result = _make_critical_result()
        dispatched = dispatcher.dispatch(result)
        alert = dispatched[0]
        assert "status" in alert
        assert "severity" in alert
        assert "current_value" in alert
        assert "baseline_mean" in alert
        assert "z_score" in alert
        assert "score" in alert
        assert "timestamp" in alert

    def test_cooldown_prevents_duplicate(self):
        """Same status+severity should not re-alert within cooldown."""
        dispatcher = AlertDispatcher(cooldown_seconds=10)
        result = _make_critical_result()

        first = dispatcher.dispatch(result)
        assert len(first) == 2

        # Second dispatch within cooldown → no alerts
        second = dispatcher.dispatch(result)
        assert len(second) == 0

    def test_cooldown_expires(self):
        """After cooldown expires, alerts should fire again."""
        dispatcher = AlertDispatcher(cooldown_seconds=0)  # 0 = no cooldown
        result = _make_critical_result()

        first = dispatcher.dispatch(result)
        assert len(first) == 2

        second = dispatcher.dispatch(result)
        assert len(second) == 2

    def test_reset_cooldowns(self):
        """reset_cooldowns should allow re-alerting."""
        dispatcher = AlertDispatcher(cooldown_seconds=3600)
        result = _make_critical_result()

        first = dispatcher.dispatch(result)
        assert len(first) == 2

        dispatcher.reset_cooldowns()

        second = dispatcher.dispatch(result)
        assert len(second) == 2

    def test_severity_mapping(self):
        """z-score > 4 → CRITICAL, 2.5-4 → WARNING."""
        dispatcher = AlertDispatcher(cooldown_seconds=0)
        result = _make_critical_result()
        dispatched = dispatcher.dispatch(result)

        denied = next(d for d in dispatched if d["status"] == "denied")
        assert denied["severity"] == "CRITICAL"  # z=8.5

        failed = next(d for d in dispatched if d["status"] == "failed")
        assert failed["severity"] == "CRITICAL"  # z=5.2

    def test_critical_logs_at_critical_level(self, caplog):
        """CRITICAL alerts should log at CRITICAL level."""
        dispatcher = AlertDispatcher(cooldown_seconds=0)
        result = _make_critical_result()

        with caplog.at_level(logging.CRITICAL, logger="alerting"):
            dispatcher.dispatch(result)

        critical_logs = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical_logs) >= 1

    def test_warning_logs_at_warning_level(self, caplog):
        """WARNING alerts should log at WARNING level."""
        dispatcher = AlertDispatcher(cooldown_seconds=0)
        result = _make_warning_result()

        with caplog.at_level(logging.WARNING, logger="alerting"):
            dispatcher.dispatch(result)

        warning_logs = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning_logs) >= 1

    def test_non_anomalous_statuses_skipped(self):
        """Statuses with is_anomalous=False should not generate alerts."""
        dispatcher = AlertDispatcher(cooldown_seconds=0)
        result = _make_critical_result()
        dispatched = dispatcher.dispatch(result)
        dispatched_statuses = {d["status"] for d in dispatched}
        assert "reversed" not in dispatched_statuses
        assert "backend_reversed" not in dispatched_statuses
