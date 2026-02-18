from fastapi import APIRouter, Query
from app.database import get_summary, get_recent_records, get_status_counts_at
from app.models.queries import StatusSummary, SummaryResponse, RecentRecord, RecentResponse, StatusDistribution, StatusDistributionResponse

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


@router.get("/transactions/status-distribution", response_model=StatusDistributionResponse)
def status_distribution(minutes: int = Query(default=60, ge=1, le=1440)):
    """
    Get current status distribution from database for the last N minutes.
    Returns data in a format optimized for Grafana pie charts.
    """
    counts = get_status_counts_at(minutes=minutes)
    
    if not counts:
        return StatusDistributionResponse(
            window_minutes=minutes,
            statuses=[],
            total=0
        )
    
    statuses = [
        StatusDistribution(status=status, count=count)
        for status, count in counts.items()
    ]
    
    total = sum(s.count for s in statuses)
    
    return StatusDistributionResponse(
        window_minutes=minutes,
        statuses=statuses,
        total=total
    )
