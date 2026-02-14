from pydantic import BaseModel


class StatusSummary(BaseModel):
    status: str
    total: int
    avg_per_min: float
    max_count: int
    min_count: int
    data_points: int


class SummaryResponse(BaseModel):
    window_minutes: int
    statuses: list[StatusSummary]
    total_records: int


class HealthResponse(BaseModel):
    status: str
    db_connected: bool
    total_records: int


class RecentRecord(BaseModel):
    timestamp: str
    status: str
    count: int
    ingested_at: str


class RecentResponse(BaseModel):
    records: list[RecentRecord]
    count: int
