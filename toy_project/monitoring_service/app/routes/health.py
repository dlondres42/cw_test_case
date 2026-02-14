from fastapi import APIRouter, Response
from app.database import check_connection, get_total_records
from app.models.queries import HealthResponse
from cw_common.observability import metrics_response

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
def health():
    db_ok = check_connection()
    total = get_total_records() if db_ok else 0
    return HealthResponse(
        status="healthy" if db_ok else "unhealthy",
        db_connected=db_ok,
        total_records=total,
    )


@router.get("/metrics")
def metrics():
    body, content_type = metrics_response()
    return Response(content=body, media_type=content_type)
