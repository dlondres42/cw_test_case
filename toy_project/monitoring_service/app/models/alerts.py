"""Pydantic models for the anomaly alerting system."""

from datetime import datetime
from pydantic import BaseModel, Field


class AnomalyAlert(BaseModel):
    """Single per-status anomaly alert detail."""

    status: str
    severity: str = Field(description="NORMAL, WARNING, or CRITICAL")
    score: float = Field(description="Anomaly score (lower = more anomalous)")
    current_value: int
    baseline_mean: float
    baseline_std: float
    z_score: float
    is_anomalous: bool
    message: str = ""


class AlertResponse(BaseModel):
    """Full anomaly analysis response."""

    timestamp: datetime
    overall_score: float = Field(description="Combined anomaly score from ML model")
    overall_severity: str = Field(description="NORMAL, WARNING, or CRITICAL")
    alerts: list[AnomalyAlert]
    recommendation: str = Field(
        description="Human-readable recommendation based on analysis"
    )
    window_minutes: int = Field(description="History window used for analysis")
    model_mode: str = Field(
        description="'ml' if Isolation Forest loaded, 'rule' if fallback"
    )


class AlertStatusResponse(BaseModel):
    """Lightweight current anomaly status check."""

    timestamp: datetime
    overall_severity: str
    overall_score: float
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
