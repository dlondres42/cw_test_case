from fastapi import APIRouter
from app.database import insert_transactions
from app.models.commands import TransactionRecord, TransactionBatch, IngestionResponse

router = APIRouter(tags=["Ingestion"])


@router.post("/transactions", response_model=IngestionResponse)
def ingest_single(record: TransactionRecord):
    row = {
        "timestamp": record.timestamp.isoformat(),
        "status": record.status,
        "count": record.count,
    }
    insert_transactions([row])
    return IngestionResponse(records_inserted=1, timestamp=record.timestamp)


@router.post("/transactions/batch", response_model=IngestionResponse)
def ingest_batch(batch: TransactionBatch):
    rows = [
        {
            "timestamp": r.timestamp.isoformat(),
            "status": r.status,
            "count": r.count,
        }
        for r in batch.records
    ]
    count = insert_transactions(rows)
    # Handle empty batch gracefully
    latest_ts = max(r.timestamp for r in batch.records) if batch.records else None
    return IngestionResponse(records_inserted=count, timestamp=latest_ts)
