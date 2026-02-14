from datetime import datetime
from pydantic import BaseModel, Field


class TransactionRecord(BaseModel):
    timestamp: datetime
    status: str
    count: int = Field(ge=0)


class TransactionBatch(BaseModel):
    records: list[TransactionRecord]


class IngestionResponse(BaseModel):
    records_inserted: int
    timestamp: datetime
