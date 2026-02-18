# Transaction Monitoring System

A real-time transaction monitoring system with anomaly detection, alerting, and full
observability. The system ingests transaction data via Kafka, runs statistical anomaly
detection on per-minute aggregates, and dispatches alerts to a webhook when anomalies
are detected.

The design mimics a production monitoring pipeline: a streaming data source, a consumer
service that owns detection and alerting, and a Grafana stack for dashboards, logs, and
traces. Alerting is handled entirely by the monitoring service -- Grafana is used for
visualization only.

## Components

The project is split into three packages, each with its own documentation:

| Component | Description | Docs |
|---|---|---|
| [monitoring\_service](monitoring_service/) | FastAPI service: Kafka consumer, SQLite storage, anomaly detection, alerting API | [monitoring\_service/README.md](monitoring_service/README.md) |
| [stream\_processor](stream_processor/) | Mock producer that replays CSV transaction data into Kafka | [stream\_processor/README.md](stream_processor/README.md) |
| [common](common/) | Shared observability library: logging, tracing, metrics, propagation | [common/README.md](common/README.md) |

## System Architecture

```
                    +-------------------+
                    |  stream_processor |
                    |  (CSV -> Kafka)   |
                    +--------+----------+
                             |
                        Kafka topic
                      (transactions)
                             |
                    +--------v----------+
                    | monitoring_service |
                    |  (FastAPI)         |
                    |                    |
                    |  - Consumer        |
                    |  - SQLite DB       |
                    |  - Detector        |
                    |  - Scheduler       |
                    |  - Alert API       |
                    +--+-----+------+---+
                       |     |      |
          +------------+     |      +------------+
          |                  |                   |
     Prometheus           Jaeger              Webhook
     (metrics)           (traces)            (alerts)
          |                  |                   |
          +-------+  +------+                   |
                  |  |                           |
              +---v--v---+                       |
              |  Grafana  |                      |
              | Dashboards|           webhook.site / Slack / etc.
              +-----------+
```

## Data Flow

1. **stream\_processor** reads a CSV file of historical transactions, groups them by
   timestamp, and produces each minute's data as a JSON batch to the `transactions`
   Kafka topic. OpenTelemetry trace context is injected into Kafka headers.

2. **monitoring\_service** consumes from Kafka in a background thread. Each batch is
   inserted into SQLite and Prometheus counters are updated. The consumer span links
   back to the producer span via propagated trace context.

3. A **background scheduler** (every 30 seconds) queries the database for the latest
   status counts and a 60-minute history window, runs the `PolicyAnomalyDetector`, and
   dispatches alerts via `AlertDispatcher` when anomalies exceed the configured
   thresholds.

4. The **`/alerts/evaluate` endpoint** provides on-demand evaluation: submit a
   `{status, count}` payload and get back a severity assessment. If anomalous, the
   endpoint dispatches a webhook alert, inserts the record into the database, and
   updates Prometheus metrics.

5. **Grafana** queries Prometheus for metric dashboards, Jaeger for distributed traces,
   and Loki for structured logs. Datasources and dashboards are provisioned
   automatically.

## Alerting Policy

Detection uses rolling Z-scores computed over a 60-minute sliding window. The detector
monitors five transaction statuses: `denied`, `failed`, `reversed`, `backend_reversed`,
and `approved`.

| Severity | Condition | Action |
|---|---|---|
| CRITICAL | Z-score > 4.0 sigma | Webhook POST + Loki log |
| WARNING | Z-score > 2.5 sigma | Loki log only |
| NORMAL | Below threshold | No action |

For problem statuses (`denied`, `failed`, `reversed`, `backend_reversed`), any occurrence
with no established baseline is immediately flagged as CRITICAL. The rationale is that
these statuses should be rare or zero under normal conditions.

A 5-minute cooldown per (status, severity) pair prevents duplicate alerts during sustained
anomalies.

## Observability Stack

| Tool | Role | Port |
|---|---|---|
| Prometheus | Metrics scraping (5s interval) | 9090 |
| Jaeger | Distributed tracing (OTLP HTTP) | 16686 (UI), 4318 (collector) |
| Loki | Log aggregation | 3100 |
| Promtail | Docker log collection -> Loki | -- |
| Grafana | Dashboards, log exploration, trace viewer | 3000 |

Grafana is provisioned with four datasources (Prometheus, Jaeger, Loki, Monitoring API)
and a pre-built dashboard (`monitoring-overview.json`).

### Distributed Tracing

Traces flow end-to-end from producer to consumer. The stream processor creates a root
span for each Kafka message. The monitoring service extracts the trace context from Kafka
headers and creates a child consumer span, so both sides appear in the same trace in
Jaeger.

The `/alerts/evaluate` endpoint creates its own trace with child spans for each operation:
history fetch, anomaly evaluation, alert dispatch, and database insertion.

### Structured Logging

All services use JSON-formatted logs with OpenTelemetry trace/span IDs injected into
every record. This allows correlating logs with traces in Grafana by clicking through
from Loki to Jaeger.

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development and tests)
- Transaction CSV data in `../sample_data/transactions/transactions.csv`

### Starting the full stack

```bash
cd toy_project
docker compose up -d
```

This starts all services: Kafka, Jaeger, Loki, Promtail, Prometheus, Grafana,
monitoring-api, and stream-processor. The stream processor begins replaying transaction
data immediately.

### Accessing services

| Service | URL |
|---|---|
| Monitoring API | http://localhost:8000 |
| Monitoring API docs | http://localhost:8000/docs |
| Grafana | http://localhost:3000 (admin/admin) |
| Jaeger UI | http://localhost:16686 |
| Prometheus | http://localhost:9090 |

### Testing the evaluate endpoint

```bash
# Evaluate a failed transaction (will trigger CRITICAL alert)
curl -X POST http://localhost:8000/alerts/evaluate \
  -H "Content-Type: application/json" \
  -d '{"status": "failed", "count": 100}'

# Check current anomaly status
curl http://localhost:8000/alerts/status
```

### Running tests

Each component has its own test suite:

```bash
# Common library
cd common && pip install -e ".[dev]" && pytest tests/ -v

# Monitoring service
cd monitoring_service && pip install -e ../common && pip install -e ".[dev]" && pytest tests/ -v

# Stream processor
cd stream_processor && pip install -e ../common && pip install -e ".[dev]" && pytest tests/ -v
```

### Stopping

```bash
docker compose down
```

## Project Structure

```
toy_project/
  docker-compose.yml          -- Full stack orchestration
  prometheus.yml              -- Prometheus scrape config
  promtail-config.yml         -- Promtail log collection config
  common/                     -- Shared observability library
    cw_common/
      observability/
        logging.py            -- JSON logging + webhook handler
        tracing.py            -- OpenTelemetry tracing init
        propagation.py        -- W3C trace context for Kafka
        metrics.py            -- Prometheus metric factories
        middleware.py          -- HTTP request counting middleware
        testing.py            -- Test helpers (in-memory exporter, metric reset)
  monitoring_service/         -- Core monitoring API
    app/
      main.py                 -- FastAPI app, lifespan, Kafka consumer start
      consumer.py             -- Kafka consumer thread
      database.py             -- SQLite operations with tracing
      detector.py             -- Rolling Z-score anomaly detection
      alerting.py             -- Alert dispatcher with cooldown
      scheduler.py            -- Background periodic detection loop
      telemetry.py            -- Service-specific Prometheus metrics
      routes/
        alerts.py             -- Anomaly analysis and evaluation endpoints
        queries.py            -- Transaction query endpoints
        health.py             -- Health check and metrics endpoint
      models/
        alerts.py             -- Pydantic models for alerting
        queries.py            -- Pydantic models for queries
  stream_processor/           -- Mock Kafka producer
    main.py                   -- CLI entry point with user controls
    loader.py                 -- CSV loading and grouping
    producer.py               -- Kafka producer with trace propagation
    telemetry.py              -- Producer-specific metrics
  grafana/
    dashboards/               -- Pre-built Grafana dashboards
    provisioning/             -- Auto-provisioned datasources and config
```

## Configuration Reference

All services are configured via environment variables. See each component's README for
the full list. The key variables for the Docker Compose stack are set in
`docker-compose.yml` and can be overridden with a `.env` file.
