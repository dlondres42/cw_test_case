"""
Policy-based anomaly detector using rolling Z-score.

Replaces the Isolation Forest ML model with a simpler, transparent
rule-based approach. For each monitored status (denied, failed, reversed,
backend_reversed, approved) the detector computes a Z-score against the
rolling history and flags anomalies when the score exceeds a configurable
threshold.

Design derived from the EDA in ``eda_transactions_anomaly_detection.ipynb``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import sqrt
from typing import Optional

logger = logging.getLogger("detector")

# Statuses we monitor for anomalies (spikes or drops)
ALERT_STATUSES = ("denied", "failed", "reversed", "backend_reversed", "approved")

# Problem statuses: ANY occurrence is bad (unlike "approved" where high is good)
PROBLEM_STATUSES = ("denied", "failed", "reversed", "backend_reversed")


# ── Data classes ─────────────────────────────────────────────────


@dataclass
class AnomalyDetail:
    """Per-status anomaly breakdown."""

    status: str
    current_value: int
    baseline_mean: float
    baseline_std: float
    z_score: float
    is_anomalous: bool
    contribution: str = ""


@dataclass
class AnomalyResult:
    """Full anomaly detection result for a single evaluation."""

    max_z_score: float
    severity: str  # NORMAL | WARNING | CRITICAL
    anomalies: list[AnomalyDetail] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def has_anomalies(self) -> bool:
        return any(d.is_anomalous for d in self.anomalies)


# ── Detector ─────────────────────────────────────────────────────


class PolicyAnomalyDetector:
    """Rolling Z-score anomaly detector.

    For each alert status the detector computes::

        z = (current_count - rolling_mean) / max(rolling_std, 1.0)

    and flags the status as anomalous when ``z > z_score_threshold``.

    Args:
        z_score_threshold: Z-score above which a status is flagged WARNING.
        critical_threshold: Z-score above which a status is flagged CRITICAL.
        min_history: Minimum number of history entries required for a
            meaningful baseline.  With fewer entries the detector returns
            NORMAL to avoid false positives during warm-up.
    """

    def __init__(
        self,
        z_score_threshold: float = 2.5,
        critical_threshold: float = 4.0,
        min_history: int = 30,
    ):
        self.z_score_threshold = z_score_threshold
        self.critical_threshold = critical_threshold
        self.min_history = min_history

    # ── public API ───────────────────────────────────────────────

    def detect(
        self,
        status_counts: dict[str, int],
        history: list[dict],
        timestamp: Optional[datetime] = None,
    ) -> AnomalyResult:
        """Run anomaly detection on the current minute's transaction counts.

        Args:
            status_counts: Mapping of status → count for the current minute,
                e.g. ``{"approved": 120, "denied": 15, "failed": 0}``.
            history: List of per-minute dicts (oldest first), each mapping
                status → count.  Typically the last 60 minutes.
            timestamp: Optional evaluation timestamp (defaults to now).

        Returns:
            AnomalyResult with severity, per-status details, and max Z-score.
        """
        ts = timestamp or datetime.now(timezone.utc)
        details: list[AnomalyDetail] = []

        if len(history) < self.min_history:
            logger.debug(
                "Insufficient history (%d < %d), returning NORMAL",
                len(history), self.min_history,
            )
            return AnomalyResult(
                max_z_score=0.0,
                severity="NORMAL",
                anomalies=[],
                timestamp=ts,
            )

        for status in ALERT_STATUSES:
            current = status_counts.get(status, 0)
            mean, std = self._rolling_stats(history, status)
            z = (current - mean) / max(std, 1.0)
            is_anomalous = z > self.z_score_threshold

            contribution = ""
            if is_anomalous:
                contribution = (
                    f"{status} count {current} is {z:.1f}σ above "
                    f"baseline (mean={mean:.1f}, std={std:.1f})"
                )

            details.append(AnomalyDetail(
                status=status,
                current_value=current,
                baseline_mean=round(mean, 2),
                baseline_std=round(std, 2),
                z_score=round(z, 2),
                is_anomalous=is_anomalous,
                contribution=contribution,
            ))

        max_z = max((d.z_score for d in details), default=0.0)
        severity = self._compute_severity(details)

        return AnomalyResult(
            max_z_score=round(max_z, 2),
            severity=severity,
            anomalies=details,
            timestamp=ts,
        )

    def evaluate_single(
        self,
        status: str,
        count: int,
        history: list[dict],
    ) -> dict:
        """Evaluate a single status count against the rolling baseline.

        Args:
            status: Transaction status to evaluate (e.g. ``"denied"``).
            count: Observed count for that status.
            history: Rolling per-minute history dicts (oldest first).

        Returns:
            Dict with keys: severity, z_score, baseline_mean, baseline_std,
            is_anomalous, message.
        """
        # For problem statuses, ANY occurrence with insufficient history is anomalous
        # (baseline is implicitly 0 since we have no history showing these are normal)
        if len(history) < self.min_history:
            if status in PROBLEM_STATUSES and count > 0:
                # Treat as high severity since we have no baseline showing this is normal
                z_score = 10.0  # Arbitrary high z-score to indicate significance
                return {
                    "severity": "CRITICAL",
                    "z_score": z_score,
                    "baseline_mean": 0.0,
                    "baseline_std": 0.0,
                    "is_anomalous": True,
                    "message": (
                        f"{status} count {count} detected with no historical baseline "
                        "(problem status should be rare/zero)"
                    ),
                }
            # For approved or zero counts, insufficient history means we can't evaluate
            return {
                "severity": "NORMAL",
                "z_score": 0.0,
                "baseline_mean": 0.0,
                "baseline_std": 0.0,
                "is_anomalous": False,
                "message": (
                    f"Insufficient history ({len(history)} < {self.min_history}) "
                    "for reliable evaluation."
                ),
            }

        mean, std = self._rolling_stats(history, status)
        z = (count - mean) / max(std, 1.0)
        is_anomalous = z > self.z_score_threshold

        if z > self.critical_threshold:
            severity = "CRITICAL"
        elif z > self.z_score_threshold:
            severity = "WARNING"
        else:
            severity = "NORMAL"

        message = ""
        if is_anomalous:
            message = (
                f"{status} count {count} is {z:.1f}\u03c3 above "
                f"baseline (mean={mean:.1f}, std={std:.1f})"
            )

        return {
            "severity": severity,
            "z_score": round(z, 2),
            "baseline_mean": round(mean, 2),
            "baseline_std": round(std, 2),
            "is_anomalous": is_anomalous,
            "message": message,
        }

    # ── internals ────────────────────────────────────────────────

    @staticmethod
    def _rolling_stats(history: list[dict], status: str) -> tuple[float, float]:
        """Compute mean and std of a status count over the history window."""
        values = [entry.get(status, 0) for entry in history]
        n = len(values)
        if n == 0:
            return 0.0, 0.0
        mean = sum(values) / n
        if n < 2:
            return mean, 0.0
        variance = sum((v - mean) ** 2 for v in values) / (n - 1)
        return mean, sqrt(variance)

    def _compute_severity(self, details: list[AnomalyDetail]) -> str:
        """Derive overall severity from per-status details."""
        has_critical = any(
            d.z_score > self.critical_threshold for d in details
        )
        has_warning = any(
            d.z_score > self.z_score_threshold for d in details
        )
        if has_critical:
            return "CRITICAL"
        if has_warning:
            return "WARNING"
        return "NORMAL"
