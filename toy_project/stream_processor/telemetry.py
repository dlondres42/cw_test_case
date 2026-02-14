"""
Service-specific metrics for stream-processor.

Domain counters and histograms that sit on top of the shared
``cw_common.observability`` metric factories.
"""

from cw_common.observability import create_counter, create_histogram

# ── Metrics (Prometheus) ──────────────────────────────────────────

BATCHES_PRODUCED = create_counter(
    "batches_produced_total",
    "Total number of batches produced to Kafka",
)

RECORDS_PRODUCED = create_counter(
    "records_produced_total",
    "Total number of transaction records produced to Kafka",
)

PRODUCE_DURATION = create_histogram(
    "produce_duration_seconds",
    "Time spent producing a batch to Kafka",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
