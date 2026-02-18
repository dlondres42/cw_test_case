# Monitoring Service

FastAPI-based monitoring service that consumes transaction data from Kafka, stores it in SQLite,
runs anomaly detection, and dispatches alerts via webhook. This is the core of the alerting pipeline --
it owns the data, the detection logic, and the notification channels.

## Architecture

The service is structured around four concerns:

- **Ingestion** -- A background Kafka consumer thread reads transaction batches, inserts them into
  SQLite, and updates Prometheus counters and gauges.
- **Detection** -- A policy-based anomaly detector computes rolling Z-scores per transaction status
  against a 60-minute history window. Thresholds are configurable but default to 2.5 sigma for
  WARNING and 4.0 sigma for CRITICAL.
- **Alerting** -- An alert dispatcher bridges detection results to structured log alerts. CRITICAL
  alerts are forwarded to a webhook via the `WebhookAlertHandler` in the common library. A 5-minute
  cooldown per status+severity pair prevents alert storms.
- **Scheduling** -- A background async loop runs the full detection cycle every 30 seconds, querying
  the database and dispatching alerts when anomalies are found.

## API Endpoints

### Health and Metrics

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Service health check with DB status and record count |
| `GET` | `/metrics` | Prometheus exposition format metrics |

### Query Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/transactions/summary` | Aggregated status counts over a configurable window |
| `GET` | `/transactions/recent` | Latest N transaction records |
| `GET` | `/transactions/status-distribution` | Per-status counts for the last N minutes |

### Alert Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/alerts/analyze` | Run full anomaly detection on recent data and return recommendations |
| `GET` | `/alerts/status` | Lightweight current anomaly status (Z-scores per status) |
| `GET` | `/alerts/rates` | Status rate time series for visualization |
| `POST` | `/alerts/evaluate` | Evaluate a single transaction count against the baseline |

### The evaluate endpoint

`POST /alerts/evaluate` is the main entry point for on-demand anomaly assessment. It accepts a
`{status, count, timestamp?}` payload and:

1. Computes the Z-score against the 60-minute rolling baseline.
2. If anomalous, dispatches an alert to the configured webhook.
3. Inserts the record into the database.
4. Updates Prometheus metrics so Grafana dashboards reflect the transaction.

For problem statuses (`denied`, `failed`, `reversed`, `backend_reversed`), any occurrence with
insufficient historical baseline is treated as CRITICAL -- the assumption is that these statuses
should be rare or zero in normal operation, so their presence alone warrants alerting.

## Anomaly Detection

The `PolicyAnomalyDetector` uses a rolling Z-score approach:

```
z = (current_count - rolling_mean) / max(rolling_std, 1.0)
```

For each monitored status, the detector needs at least 30 data points to establish a meaningful
baseline. With fewer entries it returns NORMAL for non-problem statuses to avoid false positives
during warm-up. Problem statuses bypass this requirement.

Monitored statuses: `denied`, `failed`, `reversed`, `backend_reversed`, `approved`.

Severity mapping:

| Severity | Condition |
|---|---|
| CRITICAL | Z-score > 4.0 sigma |
| WARNING | Z-score > 2.5 sigma |
| NORMAL | Below threshold |

## Observability

### Tracing

OpenTelemetry spans are created for:

- Kafka message consumption (with trace context propagated from the producer)
- Database operations (INSERT, SELECT)
- The `/alerts/evaluate` endpoint (parent span with children for history fetch, evaluation,
  dispatching, and DB insertion)

Traces are exported to Jaeger via OTLP HTTP.

### Prometheus Metrics

| Metric | Type | Description |
|---|---|---|
| `messages_consumed_total` | Counter | Records consumed from Kafka |
| `consume_batch_duration_seconds` | Histogram | Kafka batch processing time |
| `db_insert_duration_seconds` | Histogram | SQLite insert latency |
| `http_requests_total` | Counter | HTTP requests by method, path, status |
| `transactions_by_status_total` | Counter | Total transactions by status |
| `transaction_status_rate` | Gauge | Current per-minute rate by status |
| `transaction_anomaly_score` | Gauge | Current Z-score by status |
| `transaction_alerts_total` | Counter | Alerts fired by status and severity |
| `overall_anomaly_score` | Gauge | Highest Z-score from the detector |

### Logging

Structured JSON logs with OpenTelemetry trace/span IDs injected into every record.
CRITICAL alerts include `alert=True` extra data, which triggers the `WebhookAlertHandler`
to POST to the configured webhook URL.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `DATABASE_PATH` | `./transactions.db` | SQLite database path |
| `DB_RESET_ON_START` | `false` | Wipe the database on startup |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `transactions` | Kafka topic to consume |
| `KAFKA_GROUP_ID` | `monitoring-service` | Kafka consumer group |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | Jaeger OTLP endpoint |
| `ALERT_WEBHOOK_URL` | _(unset)_ | Webhook URL for CRITICAL alerts |
| `ALERT_CHECK_INTERVAL_SECONDS` | `30` | Background scheduler poll interval |

## Running locally

```bash
pip install -e ../common
pip install -e ".[dev]"
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Running tests

```bash
pytest tests/ -v
```
