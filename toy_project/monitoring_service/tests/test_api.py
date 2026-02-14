from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import app.database as db_module
from app.database import init_db
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


def _now_ts(offset_minutes: int = 0) -> str:
    t = datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)
    return t.strftime("%Y-%m-%dT%H:%M:%S")


# ── Health ────────────────────────────────────────────────────────

class TestHealth:
    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_status_healthy(self, client):
        data = client.get("/health").json()
        assert data["status"] == "healthy"

    def test_health_db_connected(self, client):
        data = client.get("/health").json()
        assert data["db_connected"] is True

    def test_health_total_records_starts_at_zero(self, client):
        data = client.get("/health").json()
        assert data["total_records"] == 0


# ── Ingestion ─────────────────────────────────────────────────────

class TestIngestion:
    def test_ingest_single_record(self, client):
        payload = {
            "timestamp": "2025-07-12T14:00:00",
            "status": "approved",
            "count": 120,
        }
        response = client.post("/transactions", json=payload)
        assert response.status_code == 200
        assert response.json()["records_inserted"] == 1

    def test_ingest_batch(self, client):
        payload = {
            "records": [
                {"timestamp": "2025-07-12T14:00:00", "status": "approved", "count": 120},
                {"timestamp": "2025-07-12T14:00:00", "status": "denied", "count": 5},
                {"timestamp": "2025-07-12T14:00:00", "status": "failed", "count": 2},
            ]
        }
        response = client.post("/transactions/batch", json=payload)
        assert response.status_code == 200
        assert response.json()["records_inserted"] == 3

    def test_ingest_increments_total(self, client):
        payload = {
            "timestamp": "2025-07-12T14:00:00",
            "status": "approved",
            "count": 100,
        }
        client.post("/transactions", json=payload)
        client.post("/transactions", json=payload)
        health = client.get("/health").json()
        assert health["total_records"] == 2

    def test_ingest_rejects_negative_count(self, client):
        payload = {
            "timestamp": "2025-07-12T14:00:00",
            "status": "approved",
            "count": -1,
        }
        response = client.post("/transactions", json=payload)
        assert response.status_code == 422

    def test_ingest_large_batch(self, client):
        records = [
            {"timestamp": _now_ts(), "status": "approved", "count": i}
            for i in range(100)
        ]
        response = client.post("/transactions/batch", json={"records": records})
        assert response.status_code == 200
        assert response.json()["records_inserted"] == 100


# ── Summary ───────────────────────────────────────────────────────

class TestSummary:
    def test_summary_empty_db(self, client):
        response = client.get("/transactions/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["statuses"] == []
        assert data["total_records"] == 0

    def test_summary_returns_aggregation(self, client):
        ts1 = _now_ts()
        ts2 = _now_ts(-1)
        batch = {
            "records": [
                {"timestamp": ts1, "status": "approved", "count": 100},
                {"timestamp": ts1, "status": "denied", "count": 5},
                {"timestamp": ts2, "status": "approved", "count": 110},
                {"timestamp": ts2, "status": "denied", "count": 8},
            ]
        }
        client.post("/transactions/batch", json=batch)
        response = client.get("/transactions/summary?minutes=1440")
        data = response.json()
        assert data["window_minutes"] == 1440
        assert len(data["statuses"]) == 2

    def test_summary_correct_totals(self, client):
        ts = _now_ts()
        batch = {
            "records": [
                {"timestamp": ts, "status": "approved", "count": 100},
                {"timestamp": _now_ts(-1), "status": "approved", "count": 50},
            ]
        }
        client.post("/transactions/batch", json=batch)
        data = client.get("/transactions/summary?minutes=1440").json()
        approved = next(s for s in data["statuses"] if s["status"] == "approved")
        assert approved["total"] == 150
        assert approved["data_points"] == 2
        assert approved["max_count"] == 100
        assert approved["min_count"] == 50

    def test_summary_invalid_minutes(self, client):
        response = client.get("/transactions/summary?minutes=0")
        assert response.status_code == 422
        response = client.get("/transactions/summary?minutes=2000")
        assert response.status_code == 422


# ── Recent ────────────────────────────────────────────────────────

class TestRecent:
    def test_recent_empty_db(self, client):
        response = client.get("/transactions/recent")
        assert response.status_code == 200
        data = response.json()
        assert data["records"] == []
        assert data["count"] == 0

    def test_recent_returns_latest(self, client):
        for i in range(5):
            client.post("/transactions", json={
                "timestamp": f"2025-07-12T14:0{i}:00",
                "status": "approved",
                "count": (i + 1) * 10,
            })
        data = client.get("/transactions/recent?limit=3").json()
        assert data["count"] == 3
        assert data["records"][0]["count"] == 50

    def test_recent_respects_limit(self, client):
        for i in range(20):
            client.post("/transactions", json={
                "timestamp": "2025-07-12T14:00:00",
                "status": "approved",
                "count": i,
            })
        data = client.get("/transactions/recent?limit=5").json()
        assert data["count"] == 5

    def test_recent_invalid_limit(self, client):
        response = client.get("/transactions/recent?limit=0")
        assert response.status_code == 422
        response = client.get("/transactions/recent?limit=200")
        assert response.status_code == 422


# ── Database Reset ────────────────────────────────────────────────

class TestDatabaseReset:
    def test_db_reset_on_start(self, tmp_path, monkeypatch):
        test_db = tmp_path / "reset_test.db"
        monkeypatch.setattr(db_module, "DB_PATH", test_db)
        monkeypatch.setenv("DB_RESET_ON_START", "false")
        init_db()

        from app.database import insert_transactions, get_total_records
        insert_transactions([{
            "timestamp": "2025-07-12T14:00:00",
            "status": "approved",
            "count": 100,
        }])
        assert get_total_records() == 1

        monkeypatch.setenv("DB_RESET_ON_START", "true")
        init_db()
        assert get_total_records() == 0
