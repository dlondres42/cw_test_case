import os
import sqlite3
from pathlib import Path
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.trace import SpanKind

DB_PATH = Path(os.environ.get("DATABASE_PATH", str(Path(__file__).parent.parent / "transactions.db")))

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    status TEXT NOT NULL,
    count INTEGER NOT NULL,
    ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
"""


def _get_db_path() -> Path:
    return DB_PATH


def init_db():
    db_path = _get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    should_reset = os.environ.get("DB_RESET_ON_START", "false").lower() == "true"

    with sqlite3.connect(str(db_path)) as conn:
        if should_reset:
            conn.execute("DROP TABLE IF EXISTS transactions")
        conn.executescript(SCHEMA)


@contextmanager
def get_connection():
    conn = sqlite3.connect(str(_get_db_path()))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def insert_transactions(records: list[dict]):
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span(
        "db insert_transactions",
        kind=SpanKind.INTERNAL,
        attributes={
            "db.system": "sqlite",
            "db.operation": "INSERT",
            "db.records_count": len(records),
        }
    ):
        with get_connection() as conn:
            conn.executemany(
                "INSERT INTO transactions (timestamp, status, count) VALUES (:timestamp, :status, :count)",
                records,
            )
        return len(records)


def get_summary(minutes: int = 60) -> list[dict]:
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span(
        "db query get_summary",
        kind=SpanKind.INTERNAL,
        attributes={
            "db.system": "sqlite",
            "db.operation": "SELECT",
            "db.query.window_minutes": minutes,
        }
    ) as span:
        query = """
        SELECT
            status,
            SUM(count) AS total,
            ROUND(AVG(count), 2) AS avg_per_min,
            MAX(count) AS max_count,
            MIN(count) AS min_count,
            COUNT(*) AS data_points
        FROM transactions
        WHERE timestamp >= datetime(
            (SELECT MAX(timestamp) FROM transactions),
            ?
        )
        GROUP BY status
        ORDER BY total DESC
        """
        offset = f"-{minutes} minutes"
        with get_connection() as conn:
            rows = conn.execute(query, (offset,)).fetchall()
        
        result = [dict(row) for row in rows]
        span.set_attribute("db.result_count", len(result))
        return result


def get_total_records() -> int:
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM transactions").fetchone()
    return row["cnt"]


def get_recent_records(limit: int = 10) -> list[dict]:
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span(
        "db query get_recent_records",
        kind=SpanKind.INTERNAL,
        attributes={
            "db.system": "sqlite",
            "db.operation": "SELECT",
            "db.query.limit": limit,
        }
    ) as span:
        query = """
        SELECT timestamp, status, count, ingested_at
        FROM transactions
        ORDER BY ingested_at DESC, id DESC
        LIMIT ?
        """
        with get_connection() as conn:
            rows = conn.execute(query, (limit,)).fetchall()
        
        result = [dict(row) for row in rows]
        span.set_attribute("db.result_count", len(result))
        return result


def check_connection() -> bool:
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False
