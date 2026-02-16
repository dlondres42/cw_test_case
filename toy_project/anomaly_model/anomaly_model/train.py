"""
Train the anomaly detection model from the transactions CSV data.

Usage::

    python -m anomaly_model.train --csv ../../sample_data/transactions/transactions.csv
"""

import argparse
import logging
from pathlib import Path

import pandas as pd
import numpy as np

from .model import AnomalyDetector, ALERT_STATUSES

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_and_pivot(csv_path: str) -> pd.DataFrame:
    """Load transactions CSV and pivot to one row per minute with status columns."""
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    pivoted = df.pivot_table(
        index="timestamp", columns="status", values="count", fill_value=0
    ).reset_index()
    pivoted = pivoted.sort_values("timestamp").reset_index(drop=True)
    return pivoted


def build_training_windows(
    pivoted: pd.DataFrame, window_size: int = 60
) -> list[list[dict]]:
    """
    Build sliding windows of minute-by-minute status counts.

    Each window is a list of dicts (one per minute), the last being
    the "current" minute for feature engineering.
    """
    statuses = [c for c in pivoted.columns if c != "timestamp"]
    windows = []
    for i in range(window_size, len(pivoted)):
        window = []
        for j in range(i - window_size, i + 1):
            row = {s: int(pivoted.iloc[j][s]) for s in statuses}
            window.append(row)
        windows.append(window)
    return windows


def inject_synthetic_anomalies(
    pivoted: pd.DataFrame, n_anomalies: int = 50, seed: int = 42
) -> pd.DataFrame:
    """
    Inject synthetic anomaly spikes so the model can learn boundaries.

    Randomly selects minutes and multiplies denied/failed/reversed
    counts by 5-20x to simulate real incidents.
    """
    rng = np.random.RandomState(seed)
    df = pivoted.copy()
    n = len(df)
    indices = rng.choice(range(60, n), size=min(n_anomalies, n - 60), replace=False)

    for idx in indices:
        for status in ALERT_STATUSES:
            if status in df.columns:
                base = max(df.iloc[idx][status], 1)
                multiplier = rng.randint(5, 21)
                df.at[idx, status] = base * multiplier

    logger.info("Injected %d synthetic anomaly points", len(indices))
    return df


def main():
    parser = argparse.ArgumentParser(description="Train anomaly detection model")
    parser.add_argument(
        "--csv",
        type=str,
        default=str(
            Path(__file__).parent.parent.parent
            / "sample_data"
            / "transactions"
            / "transactions.csv"
        ),
        help="Path to transactions CSV file",
    )
    parser.add_argument(
        "--contamination", type=float, default=0.02, help="Expected anomaly fraction"
    )
    parser.add_argument(
        "--window", type=int, default=60, help="Rolling window size in minutes"
    )
    parser.add_argument(
        "--inject-anomalies",
        type=int,
        default=50,
        help="Number of synthetic anomalies to inject (0 to disable)",
    )
    args = parser.parse_args()

    logger.info("Loading data from %s", args.csv)
    pivoted = load_and_pivot(args.csv)
    logger.info("Loaded %d minute records, statuses: %s",
                len(pivoted), [c for c in pivoted.columns if c != "timestamp"])

    if args.inject_anomalies > 0:
        pivoted = inject_synthetic_anomalies(pivoted, n_anomalies=args.inject_anomalies)

    logger.info("Building %d-minute sliding windows...", args.window)
    windows = build_training_windows(pivoted, window_size=args.window)
    logger.info("Built %d training windows", len(windows))

    detector = AnomalyDetector()
    metadata = detector.train(windows, contamination=args.contamination)

    model_path = detector.save()
    logger.info("Model saved to %s", model_path)
    logger.info("Training metadata: %s", metadata)

    # Quick validation: detect on last window
    last_window = windows[-1]
    result = detector.detect(last_window[-1], last_window[:-1])
    logger.info(
        "Validation on last window: score=%.4f severity=%s anomalies=%d",
        result.score,
        result.severity,
        sum(1 for a in result.anomalies if a.is_anomalous),
    )


if __name__ == "__main__":
    main()
