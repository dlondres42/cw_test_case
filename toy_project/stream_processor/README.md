# Stream Processor

Mock stream processor that replays transaction CSV data into Kafka, simulating a real-time
transaction feed. It reads a pre-recorded CSV file, groups records by timestamp, and produces
them as JSON batches to a Kafka topic with a configurable delay between batches.

This component exists to drive the monitoring pipeline with realistic data without needing
a live payment system.

## How it works

1. **CSV Loading** -- `loader.py` reads the transactions CSV with pandas, groups rows by
   timestamp, and returns a list of per-minute batches.
2. **Kafka Production** -- `producer.py` iterates over the batches, serialises each as JSON,
   and produces it to the configured Kafka topic. W3C trace context is injected into Kafka
   headers so the consumer can continue the distributed trace.
3. **User Controls** -- The main loop supports interactive commands: `p` to pause, `r` to
   resume, and `q` to quit. When running detached (Docker), it falls back to a non-interactive
   mode.

## Observability

Each produced batch creates an OpenTelemetry span (`{topic} publish`, kind=PRODUCER) with
attributes for batch size, timestamp, and stream progress. Trace context is propagated via
Kafka headers using the shared `cw_common.observability.propagation` helpers.

Prometheus metrics exposed on port 8001:

| Metric | Type | Description |
|---|---|---|
| `batches_produced_total` | Counter | Total batches sent to Kafka |
| `records_produced_total` | Counter | Total individual records produced |
| `produce_duration_seconds` | Histogram | Time spent producing each batch |

## Configuration

All configuration is via environment variables, with CLI overrides available:

| Variable | Default | Description |
|---|---|---|
| `CSV_PATH` | `../../sample_data/transactions/transactions.csv` | Path to the source CSV |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `transactions` | Target Kafka topic |
| `STREAM_DELAY` | `0.1` | Seconds between batches |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | Jaeger OTLP endpoint |
| `METRICS_PORT` | `8001` | Prometheus metrics port |

## Running locally

```bash
pip install -e ../common
pip install -e .
python -m stream_processor.main --csv ../../sample_data/transactions/transactions.csv
```

## Running tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```
