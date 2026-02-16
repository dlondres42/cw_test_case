"""
Transaction Anomaly Detection Model.

Provides an Isolation Forest-based anomaly detector for real-time
transaction monitoring. The model learns normal transaction patterns
and flags deviations in failed, denied, and reversed transaction rates.

Usage::

    from anomaly_model.model import AnomalyDetector

    detector = AnomalyDetector()           # loads pre-trained model if available
    result = detector.detect(
        status_counts={"approved": 120, "denied": 15, "failed": 3, "reversed": 2},
        history=[...],                     # recent minute-by-minute status counts
    )
    print(result.severity, result.anomalies)
"""

from .model import AnomalyDetector, AnomalyResult, AnomalyDetail

__all__ = ["AnomalyDetector", "AnomalyResult", "AnomalyDetail"]
