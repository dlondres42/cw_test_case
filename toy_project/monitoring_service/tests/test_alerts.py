"""Tests for the /alerts endpoints."""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
from app.database import init_db, insert_transactions
from app.main import app


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


def _seed_normal_data(minutes: int = 120):
    """Seed the DB with normal-looking transaction data."""
    import random
    random.seed(42)
    base_ts = datetime(2025, 7, 12, 14, 0, 0)
    records = []
    for i in range(minutes):
        ts = (base_ts + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        records.extend([
            {"timestamp": ts, "status": "approved", "count": random.randint(100, 130)},
            {"timestamp": ts, "status": "denied", "count": random.randint(3, 8)},
            {"timestamp": ts, "status": "failed", "count": random.randint(0, 2)},
            {"timestamp": ts, "status": "reversed", "count": random.randint(0, 2)},
            {"timestamp": ts, "status": "backend_reversed", "count": 0},
            {"timestamp": ts, "status": "refunded", "count": random.randint(0, 1)},
        ])
    insert_transactions(records)


def _seed_anomalous_spike(minutes_offset: int = 120):
    """Add an anomalous spike at the end of the data."""
    base_ts = datetime(2025, 7, 12, 14, 0, 0)
    ts = (base_ts + timedelta(minutes=minutes_offset)).strftime("%Y-%m-%d %H:%M:%S")
    records = [
        {"timestamp": ts, "status": "approved", "count": 60},
        {"timestamp": ts, "status": "denied", "count": 50},   # ~10x normal
        {"timestamp": ts, "status": "failed", "count": 15},    # ~15x normal
        {"timestamp": ts, "status": "reversed", "count": 10},  # ~10x normal
        {"timestamp": ts, "status": "backend_reversed", "count": 5},
        {"timestamp": ts, "status": "refunded", "count": 1},
    ]
    insert_transactions(records)


# ── POST /alerts/analyze ──────────────────────────────────────────

class TestAnalyze:
    def test_analyze_no_data(self, client):
        """Empty DB should return NORMAL with no alerts."""
        response = client.post("/alerts/analyze")
        assert response.status_code == 200
        data = response.json()
        assert data["overall_severity"] == "NORMAL"
        assert data["alerts"] == []
        assert "No transaction data" in data["recommendation"]

    def test_analyze_normal_data(self, client):
        """Normal data should not return CRITICAL severity."""
        _seed_normal_data(120)
        response = client.post("/alerts/analyze?window_minutes=60")
        assert response.status_code == 200
        data = response.json()
        assert data["overall_severity"] in ("NORMAL", "WARNING")
        assert "model_mode" in data
        assert data["window_minutes"] == 60

    def test_analyze_returns_all_alert_fields(self, client):
        """Response should contain all required fields."""
        _seed_normal_data(120)
        response = client.post("/alerts/analyze")
        data = response.json()

        assert "timestamp" in data
        assert "overall_score" in data
        assert "overall_severity" in data
        assert "alerts" in data
        assert "recommendation" in data
        assert "window_minutes" in data
        assert "model_mode" in data

    def test_analyze_alert_detail_fields(self, client):
        """Each alert should have per-status detail fields."""
        _seed_normal_data(120)
        response = client.post("/alerts/analyze")
        data = response.json()

        if data["alerts"]:
            alert = data["alerts"][0]
            assert "status" in alert
            assert "severity" in alert
            assert "score" in alert
            assert "current_value" in alert
            assert "baseline_mean" in alert
            assert "baseline_std" in alert
            assert "z_score" in alert
            assert "is_anomalous" in alert

    def test_analyze_detects_anomaly(self, client):
        """Spike in denied/failed should be detected as anomalous."""
        _seed_normal_data(120)
        _seed_anomalous_spike(120)
        response = client.post("/alerts/analyze?window_minutes=120")
        data = response.json()

        assert data["overall_severity"] in ("WARNING", "CRITICAL")
        anomalous = [a for a in data["alerts"] if a["is_anomalous"]]
        assert len(anomalous) > 0
        anomalous_statuses = {a["status"] for a in anomalous}
        # At least denied or failed should be flagged
        assert anomalous_statuses & {"denied", "failed", "reversed"}

    def test_analyze_custom_window(self, client):
        """Custom window_minutes parameter should be respected."""
        _seed_normal_data(120)
        response = client.post("/alerts/analyze?window_minutes=30")
        assert response.status_code == 200
        assert response.json()["window_minutes"] == 30

    def test_analyze_invalid_window(self, client):
        """Window below minimum should return 422."""
        response = client.post("/alerts/analyze?window_minutes=1")
        assert response.status_code == 422


# ── GET /alerts/status ────────────────────────────────────────────

class TestAlertStatus:
    def test_status_no_data(self, client):
        response = client.get("/alerts/status")
        assert response.status_code == 200
        data = response.json()
        assert data["overall_severity"] == "NORMAL"

    def test_status_with_data(self, client):
        _seed_normal_data(120)
        response = client.get("/alerts/status")
        assert response.status_code == 200
        data = response.json()
        assert "overall_severity" in data
        assert "overall_score" in data
        assert "statuses" in data
        assert isinstance(data["statuses"], dict)


# ── GET /alerts/rates ─────────────────────────────────────────────

class TestAlertRates:
    def test_rates_no_data(self, client):
        response = client.get("/alerts/rates")
        assert response.status_code == 200
        data = response.json()
        assert data["data"] == []
        assert data["total_points"] == 0

    def test_rates_with_data(self, client):
        _seed_normal_data(120)
        response = client.get("/alerts/rates?minutes=60")
        assert response.status_code == 200
        data = response.json()
        assert data["window_minutes"] == 60
        assert data["total_points"] > 0
        # Each record should have timestamp, status, count
        if data["data"]:
            rec = data["data"][0]
            assert "timestamp" in rec
            assert "status" in rec
            assert "count" in rec

    def test_rates_contains_multiple_statuses(self, client):
        _seed_normal_data(120)
        response = client.get("/alerts/rates?minutes=120")
        data = response.json()
        statuses = {r["status"] for r in data["data"]}
        assert "approved" in statuses
        assert "denied" in statuses
