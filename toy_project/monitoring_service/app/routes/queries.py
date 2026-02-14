from fastapi import APIRouter, Query
from app.database import get_summary, get_recent_records
from app.models.queries import StatusSummary, SummaryResponse, RecentRecord, RecentResponse

router = APIRouter(tags=["Queries"])


@router.get("/transactions/summary", response_model=SummaryResponse)
def summary(minutes: int = Query(default=60, ge=1, le=1440)):
    rows = get_summary(minutes)
    statuses = [StatusSummary(**row) for row in rows]
    total = sum(s.data_points for s in statuses)
    return SummaryResponse(
        window_minutes=minutes,
        statuses=statuses,
        total_records=total,
    )


@router.get("/transactions/recent", response_model=RecentResponse)
def recent(limit: int = Query(default=10, ge=1, le=100)):
    rows = get_recent_records(limit)
    records = [RecentRecord(**row) for row in rows]
    return RecentResponse(records=records, count=len(records))
