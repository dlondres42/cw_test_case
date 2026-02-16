"""Tests for the anomaly detection model."""

import pytest
import numpy as np

from anomaly_model.model import (
    AnomalyDetector,
    AnomalyResult,
    AnomalyDetail,
    _compute_rolling_stats,
    _engineer_features,
    _get_feature_names,
    ALERT_STATUSES,
    ROLLING_WINDOWS,
)


# ── Fixtures ──────────────────────────────────────────────────────

def _make_normal_history(minutes: int = 120) -> list[dict]:
    """Generate normal-looking transaction history."""
    rng = np.random.RandomState(42)
    history = []
    for _ in range(minutes):
        history.append({
            "approved": int(rng.normal(115, 10)),
            "denied": int(rng.normal(5, 2)),
            "failed": int(max(rng.normal(1, 0.5), 0)),
            "reversed": int(max(rng.normal(1, 0.5), 0)),
            "backend_reversed": int(max(rng.normal(0.2, 0.3), 0)),
            "refunded": int(max(rng.normal(1, 0.5), 0)),
        })
    return history


def _make_anomalous_counts() -> dict:
    """Return counts representing a denied-transaction spike."""
    return {
        "approved": 110,
        "denied": 45,  # ~8σ above normal mean of 5
        "failed": 12,  # ~22σ above normal
        "reversed": 8,  # ~14σ above normal
        "backend_reversed": 0,
        "refunded": 1,
    }


def _make_normal_counts() -> dict:
    return {
        "approved": 118,
        "denied": 6,
        "failed": 1,
        "reversed": 1,
        "backend_reversed": 0,
        "refunded": 1,
    }


# ── Unit tests ────────────────────────────────────────────────────

class TestRollingStats:
    def test_simple_mean_std(self):
        history = [{"denied": 5}, {"denied": 7}, {"denied": 3}, {"denied": 5}]
        mean, std = _compute_rolling_stats(history, "denied", 4)
        assert mean == 5.0
        assert std >= 0.5  # floor applied

    def test_empty_history(self):
        mean, std = _compute_rolling_stats([], "denied", 10)
        assert mean == 0.0
        assert std == 1.0  # default

    def test_window_larger_than_history(self):
        history = [{"denied": 10}]
        mean, std = _compute_rolling_stats(history, "denied", 60)
        assert mean == 10.0

    def test_missing_status(self):
        history = [{"approved": 100}]
        mean, std = _compute_rolling_stats(history, "denied", 5)
        assert mean == 0.0


class TestFeatureEngineering:
    def test_feature_vector_shape(self):
        counts = _make_normal_counts()
        history = _make_normal_history(60)
        features = _engineer_features(counts, history)
        expected_len = len(ALERT_STATUSES) * (2 + 3 * len(ROLLING_WINDOWS)) + 2
        assert features.shape == (1, expected_len)

    def test_feature_names_match_vector(self):
        names = _get_feature_names()
        counts = _make_normal_counts()
        history = _make_normal_history(60)
        features = _engineer_features(counts, history)
        assert len(names) == features.shape[1]


class TestAnomalyDetector:
    def test_rule_mode_normal(self):
        """Normal counts should return NORMAL severity in rule-only mode."""
        detector = AnomalyDetector()
        history = _make_normal_history(120)
        counts = _make_normal_counts()
        result = detector.detect(counts, history)

        assert isinstance(result, AnomalyResult)
        assert result.severity == "NORMAL"
        assert not result.has_anomalies

    def test_rule_mode_anomalous(self):
        """Extreme spike should return WARNING or CRITICAL."""
        detector = AnomalyDetector()
        history = _make_normal_history(120)
        counts = _make_anomalous_counts()
        result = detector.detect(counts, history)

        assert result.severity in ("WARNING", "CRITICAL")
        assert result.has_anomalies
        # denied should be flagged
        denied_detail = next(a for a in result.anomalies if a.status == "denied")
        assert denied_detail.is_anomalous
        assert denied_detail.z_score > 2.5

    def test_all_zero_counts(self):
        """All zeros should not crash."""
        detector = AnomalyDetector()
        history = [{"approved": 0, "denied": 0, "failed": 0, "reversed": 0}] * 60
        counts = {"approved": 0, "denied": 0, "failed": 0, "reversed": 0}
        result = detector.detect(counts, history)
        assert result.severity == "NORMAL"

    def test_result_fields(self):
        """Result should contain all expected fields."""
        detector = AnomalyDetector()
        history = _make_normal_history(60)
        counts = _make_normal_counts()
        result = detector.detect(counts, history)

        assert hasattr(result, "score")
        assert hasattr(result, "severity")
        assert hasattr(result, "anomalies")
        assert hasattr(result, "timestamp")
        assert hasattr(result, "features_used")
        assert len(result.anomalies) == len(ALERT_STATUSES)

    def test_anomaly_detail_fields(self):
        detector = AnomalyDetector()
        history = _make_normal_history(60)
        counts = _make_anomalous_counts()
        result = detector.detect(counts, history)

        for detail in result.anomalies:
            assert isinstance(detail, AnomalyDetail)
            assert isinstance(detail.status, str)
            assert isinstance(detail.current_value, int)
            assert isinstance(detail.baseline_mean, float)
            assert isinstance(detail.z_score, float)


class TestTrainAndPredict:
    def test_train_and_detect(self):
        """Train a model on synthetic data and verify it can detect anomalies."""
        history = _make_normal_history(200)

        # Build 60-minute sliding windows
        windows = []
        for i in range(60, len(history)):
            windows.append(history[i - 60 : i + 1])

        detector = AnomalyDetector()
        metadata = detector.train(windows, contamination=0.05)

        assert metadata["n_samples"] > 0
        assert metadata["n_features"] > 0
        assert detector._model is not None

        # Normal data should be NORMAL
        normal_result = detector.detect(_make_normal_counts(), history[-60:])
        assert normal_result.severity == "NORMAL"

        # Anomalous data should flag something
        anomalous_result = detector.detect(_make_anomalous_counts(), history[-60:])
        assert anomalous_result.severity in ("WARNING", "CRITICAL")

    def test_save_and_load(self, tmp_path):
        """Model can round-trip through save/load."""
        history = _make_normal_history(120)
        windows = [history[i - 60 : i + 1] for i in range(60, len(history))]

        detector = AnomalyDetector()
        detector.train(windows, contamination=0.05)

        model_path = tmp_path / "test_model.joblib"
        detector.save(model_path)
        assert model_path.exists()

        # Load into new detector
        loaded = AnomalyDetector(model_path=model_path)
        assert loaded._model is not None

        # Should produce same results
        counts = _make_normal_counts()
        r1 = detector.detect(counts, history[-60:])
        r2 = loaded.detect(counts, history[-60:])
        assert r1.severity == r2.severity
        assert abs(r1.score - r2.score) < 0.01
