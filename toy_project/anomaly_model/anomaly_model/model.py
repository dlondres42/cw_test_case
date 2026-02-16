"""
Isolation Forest anomaly detector for transaction status monitoring.

Combines a trained Isolation Forest model with rule-based baselines
to produce severity-graded anomaly alerts per transaction status.

The detector operates on minute-granularity status counts and uses
rolling statistics from recent history to engineer features.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import joblib

logger = logging.getLogger(__name__)

ARTIFACTS_DIR = Path(__file__).parent / "artifacts"

# Statuses we monitor for anomalies
ALERT_STATUSES = ("denied", "failed", "reversed", "backend_reversed")

# Default rolling windows (in minutes)
ROLLING_WINDOWS = [5, 15, 60]

# Severity thresholds on anomaly score (0 = most anomalous, 1 = most normal)
# Isolation Forest decision_function: lower = more anomalous
SCORE_CRITICAL = -0.15
SCORE_WARNING = -0.05


@dataclass
class AnomalyDetail:
    """Per-status anomaly breakdown."""

    status: str
    current_value: int
    baseline_mean: float
    baseline_std: float
    z_score: float
    is_anomalous: bool
    contribution: str = ""  # human-readable explanation


@dataclass
class AnomalyResult:
    """Full anomaly detection result for a single evaluation."""

    score: float
    severity: str  # "NORMAL", "WARNING", "CRITICAL"
    anomalies: list[AnomalyDetail] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    features_used: dict = field(default_factory=dict)

    @property
    def has_anomalies(self) -> bool:
        return any(a.is_anomalous for a in self.anomalies)


def _compute_rolling_stats(
    history: list[dict], status: str, window: int
) -> tuple[float, float]:
    """Compute mean and std of a status count over the last `window` minutes."""
    values = []
    for record in history[-window:]:
        values.append(record.get(status, 0))
    if not values:
        return 0.0, 1.0
    mean = float(np.mean(values))
    std = float(np.std(values))
    return mean, max(std, 0.5)  # floor std at 0.5 to avoid division by zero


def _engineer_features(
    status_counts: dict[str, int],
    history: list[dict],
) -> np.ndarray:
    """
    Build a feature vector from current counts and rolling history.

    Features per alert status (for each rolling window):
      - current count
      - rolling mean
      - rolling std
      - z-score (current - mean) / std
      - ratio to total transactions

    Plus global features:
      - total transaction count
      - approval rate
      - hour of day (cyclical sin/cos)
    """
    features = []
    total = max(sum(status_counts.values()), 1)

    for status in ALERT_STATUSES:
        current = status_counts.get(status, 0)
        ratio = current / total

        features.append(current)
        features.append(ratio)

        for window in ROLLING_WINDOWS:
            mean, std = _compute_rolling_stats(history, status, window)
            z = (current - mean) / std
            features.extend([mean, std, z])

    # Global features
    features.append(total)
    approved = status_counts.get("approved", 0)
    features.append(approved / total)  # approval rate

    return np.array(features, dtype=np.float64).reshape(1, -1)


def _get_feature_names() -> list[str]:
    """Return ordered feature names matching _engineer_features output."""
    names = []
    for status in ALERT_STATUSES:
        names.append(f"{status}_count")
        names.append(f"{status}_ratio")
        for w in ROLLING_WINDOWS:
            names.extend([
                f"{status}_mean_{w}m",
                f"{status}_std_{w}m",
                f"{status}_zscore_{w}m",
            ])
    names.append("total_count")
    names.append("approval_rate")
    return names


class AnomalyDetector:
    """
    Isolation Forest anomaly detector with rule-based fallback.

    If a pre-trained model exists in ``artifacts/``, it is loaded
    automatically. Otherwise the detector operates in rule-only mode
    using Z-score thresholds.

    Args:
        model_path: Path to a joblib-serialized model bundle.
            Defaults to ``artifacts/isolation_forest.joblib``.
        z_score_threshold: Z-score above which a single status is
            flagged as anomalous in rule mode. Default ``2.5``.
    """

    def __init__(
        self,
        model_path: Optional[Path] = None,
        z_score_threshold: float = 2.5,
    ):
        self.z_score_threshold = z_score_threshold
        self._model = None
        self._scaler = None
        self._feature_names = _get_feature_names()

        path = model_path or ARTIFACTS_DIR / "isolation_forest.joblib"
        if path.exists():
            self._load_model(path)
        else:
            logger.info(
                "No pre-trained model at %s — running in rule-only mode", path
            )

    def _load_model(self, path: Path) -> None:
        """Load a serialized model bundle (model + scaler + metadata)."""
        try:
            bundle = joblib.load(path)
            self._model = bundle["model"]
            self._scaler = bundle.get("scaler")
            logger.info(
                "Loaded anomaly model from %s (trained: %s)",
                path,
                bundle.get("trained_at", "unknown"),
            )
        except Exception as exc:
            logger.warning("Failed to load model from %s: %s", path, exc)

    def detect(
        self,
        status_counts: dict[str, int],
        history: list[dict],
        timestamp: Optional[datetime] = None,
    ) -> AnomalyResult:
        """
        Run anomaly detection on the current minute's transaction counts.

        Args:
            status_counts: Mapping of status → count for the current minute,
                e.g. ``{"approved": 120, "denied": 15, "failed": 0, ...}``.
            history: List of recent minute-by-minute status count dicts
                (oldest first). Each dict has the same shape as ``status_counts``.
            timestamp: Optional timestamp for the evaluation (defaults to now).

        Returns:
            AnomalyResult with score, severity, and per-status details.
        """
        ts = timestamp or datetime.utcnow()
        features = _engineer_features(status_counts, history)

        # ── ML scoring ────────────────────────────────────────
        if self._model is not None:
            X = features.copy()
            if self._scaler is not None:
                X = self._scaler.transform(X)
            ml_score = float(self._model.decision_function(X)[0])
        else:
            ml_score = 0.0  # neutral when no model loaded

        # ── Per-status rule-based analysis ────────────────────
        anomaly_details = []
        max_z = 0.0

        for status in ALERT_STATUSES:
            current = status_counts.get(status, 0)
            # Use the longest window for baseline
            mean, std = _compute_rolling_stats(history, status, max(ROLLING_WINDOWS))
            z = (current - mean) / std

            is_anomalous = abs(z) > self.z_score_threshold and current > 0
            max_z = max(max_z, abs(z))

            detail = AnomalyDetail(
                status=status,
                current_value=current,
                baseline_mean=round(mean, 2),
                baseline_std=round(std, 2),
                z_score=round(z, 2),
                is_anomalous=is_anomalous,
                contribution=(
                    f"{status} count ({current}) is {abs(z):.1f}σ above baseline "
                    f"(mean={mean:.1f}, std={std:.1f})"
                    if is_anomalous
                    else ""
                ),
            )
            anomaly_details.append(detail)

        # ── Combined severity ─────────────────────────────────
        if self._model is not None:
            # ML model loaded: use its score as primary signal
            if ml_score < SCORE_CRITICAL or max_z > 4.0:
                severity = "CRITICAL"
            elif ml_score < SCORE_WARNING or max_z > self.z_score_threshold:
                severity = "WARNING"
            else:
                severity = "NORMAL"
            final_score = ml_score
        else:
            # Rule-only fallback
            if max_z > 4.0:
                severity = "CRITICAL"
            elif max_z > self.z_score_threshold:
                severity = "WARNING"
            else:
                severity = "NORMAL"
            # Normalize z-score to a [-1, 0] scale for compatibility
            final_score = -min(max_z / 10.0, 1.0)

        return AnomalyResult(
            score=round(final_score, 4),
            severity=severity,
            anomalies=anomaly_details,
            timestamp=ts,
            features_used=dict(zip(self._feature_names, features[0].tolist())),
        )

    def train(
        self,
        history_matrix: list[list[dict]],
        contamination: float = 0.02,
    ) -> dict:
        """
        Train the Isolation Forest on historical data.

        Args:
            history_matrix: List of evaluation windows. Each element is
                a history list (as passed to ``detect``), where the last
                entry is the "current" minute.
            contamination: Expected proportion of anomalies.

        Returns:
            Dict with training metadata (n_samples, feature_names, etc.).
        """
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler

        # Build feature matrix
        X_rows = []
        for window in history_matrix:
            if not window:
                continue
            current = window[-1]
            hist = window[:-1]
            features = _engineer_features(current, hist)
            X_rows.append(features[0])

        X = np.array(X_rows)
        logger.info("Training on %d samples, %d features", X.shape[0], X.shape[1])

        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Train Isolation Forest
        model = IsolationForest(
            n_estimators=200,
            contamination=contamination,
            max_samples="auto",
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X_scaled)

        # Store in instance
        self._model = model
        self._scaler = scaler

        # Compute score distribution for threshold calibration
        scores = model.decision_function(X_scaled)
        predictions = model.predict(X_scaled)

        metadata = {
            "n_samples": int(X.shape[0]),
            "n_features": int(X.shape[1]),
            "feature_names": self._feature_names,
            "contamination": contamination,
            "score_mean": float(np.mean(scores)),
            "score_std": float(np.std(scores)),
            "score_min": float(np.min(scores)),
            "score_p5": float(np.percentile(scores, 5)),
            "score_p50": float(np.percentile(scores, 50)),
            "n_anomalies_detected": int(np.sum(predictions == -1)),
            "trained_at": datetime.utcnow().isoformat(),
        }
        logger.info("Training complete: %s", metadata)
        return metadata

    def save(self, path: Optional[Path] = None) -> Path:
        """Serialize the model bundle to disk."""
        if self._model is None:
            raise RuntimeError("No model to save — call train() first")

        dest = path or ARTIFACTS_DIR / "isolation_forest.joblib"
        dest.parent.mkdir(parents=True, exist_ok=True)

        bundle = {
            "model": self._model,
            "scaler": self._scaler,
            "feature_names": self._feature_names,
            "trained_at": datetime.utcnow().isoformat(),
            "alert_statuses": ALERT_STATUSES,
            "rolling_windows": ROLLING_WINDOWS,
        }
        joblib.dump(bundle, dest)
        logger.info("Saved model bundle to %s", dest)
        return dest
