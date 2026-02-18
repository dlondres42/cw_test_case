"""Pydantic models for the anomaly alerting system."""

from datetime import datetime
from pydantic import BaseModel, Field


class AnomalyAlert(BaseModel):
    """Single per-status anomaly alert detail."""

    status: str
    severity: str = Field(description="NORMAL, WARNING, or CRITICAL")
    z_score: float = Field(description="Z-score relative to rolling baseline")
    current_value: int
    baseline_mean: float
    baseline_std: float
    is_anomalous: bool
    message: str = ""


class AlertResponse(BaseModel):
    """Full anomaly analysis response."""

    timestamp: datetime
    max_z_score: float = Field(description="Highest absolute Z-score across statuses")
    overall_severity: str = Field(description="NORMAL, WARNING, or CRITICAL")
    alerts: list[AnomalyAlert]
    recommendation: str = Field(
        description="Human-readable recommendation based on analysis"
    )
    window_minutes: int = Field(description="History window used for analysis")
    detection_method: str = Field(
        description="Detection method used, e.g. 'policy_zscore'"
    )


class AlertStatusResponse(BaseModel):
    """Lightweight current anomaly status check."""

    timestamp: datetime
    overall_severity: str
    max_z_score: float
    statuses: dict[str, float] = Field(
        description="Per-status anomaly z-scores"
    )


class StatusRateRecord(BaseModel):
    """Single data point in the status rate time series."""

    timestamp: str
    status: str
    count: int


class StatusRateResponse(BaseModel):
    """Status rate time series response."""

    window_minutes: int
    data: list[StatusRateRecord]
    total_points: int


# ── Single-transaction evaluation ────────────────────────────────


class TransactionEvaluateRequest(BaseModel):
    """Request to evaluate a single transaction status count."""

    status: str = Field(
        description="Transaction status to evaluate, e.g. 'denied'"
    )
    count: int = Field(
        ge=0, description="Observed count for that status in the current minute"
    )
    timestamp: str | None = Field(
        default=None,
        description="Optional timestamp (YYYY-MM-DD HH:MM:SS). Defaults to current time."
    )


class TransactionEvaluateResponse(BaseModel):
    """Severity assessment for a single transaction status count."""

    status: str
    severity: str = Field(description="NORMAL, WARNING, or CRITICAL")
    z_score: float
    is_anomalous: bool
    baseline_mean: float
    baseline_std: float
    message: str = ""
    timestamp: str = Field(description="Timestamp of the evaluated transaction")
    alert_dispatched: bool = Field(
        description="Whether an alert was dispatched to the webhook"
    )
    db_inserted: bool = Field(
        description="Whether the record was inserted into the database"
    )
