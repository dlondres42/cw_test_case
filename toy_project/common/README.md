# cw\_common -- Shared Observability Library

Centralised observability utilities shared across all services in the monitoring stack.
The goal is to keep instrumentation consistent: every service gets the same log format,
the same tracing setup, and the same metric factory patterns by calling a single bootstrap
function.

## Quick start

```python
from cw_common.observability import init_observability, get_logger

init_observability("my-service", "1.0.0")
logger = get_logger("my-service")
```

`init_observability` wires up three things in one call:

1. **Structured JSON logging** with OpenTelemetry trace/span ID injection.
2. **Distributed tracing** via OTLP HTTP exporter to Jaeger (only when
   `OTEL_EXPORTER_OTLP_ENDPOINT` is set; failures are swallowed so they never crash
   the service).
3. **Prometheus service-info metric** (`<service>_info{version, environment}`).

## Submodules

### logging

Configures the root logger with a JSON formatter (`JsonTraceFormatter`) that includes
`timestamp`, `level`, `logger`, `message`, and OpenTelemetry context fields
(`otelTraceID`, `otelSpanID`, `otelTraceSampled`).

Includes `WebhookAlertHandler` -- a logging handler that POSTs CRITICAL-level records
(tagged with `alert=True`) to a webhook URL. Configured automatically when the
`ALERT_WEBHOOK_URL` environment variable is set. The POST happens in a daemon thread
to avoid blocking the logger.

Severity routing:

| Level | Destination |
|---|---|
| WARNING + `alert=True` | Loki (Grafana log queries) |
| CRITICAL + `alert=True` | Loki + Webhook POST |

### tracing

Initialises an OpenTelemetry `TracerProvider` with a `BatchSpanProcessor` exporting to
Jaeger via OTLP HTTP. Provides `init_tracing(service_name)` and `shutdown_tracing()`.

Services then create spans with the standard OpenTelemetry API:

```python
from opentelemetry import trace
tracer = trace.get_tracer(__name__)
with tracer.start_as_current_span("my-operation", kind=SpanKind.INTERNAL):
    ...
```

### propagation

W3C TraceContext inject/extract helpers for Kafka message headers. This is how the
producer and consumer share trace context across the message bus:

- `inject_trace_context(headers)` -- call before producing a message.
- `extract_trace_context(headers)` -- call when consuming a message, returns a `Context`
  to use as the parent of the consumer span.
- `kafka_headers_to_dict` / `dict_to_kafka_headers` -- convert between confluent-kafka
  header tuples and plain dicts.

### metrics

Prometheus metric factory functions with idempotent registration. Wraps the standard
`prometheus_client` constructors so that duplicate registrations (common in test suites
and module reloads) are handled gracefully:

- `create_counter`, `create_histogram`, `create_gauge`, `create_info`
- `create_service_info(name, version, environment)` -- creates and populates a service
  metadata Info metric.
- `metrics_response()` -- returns Prometheus exposition-format bytes and content type.

### middleware

Reusable Starlette middleware (`MetricsMiddleware`) that increments a labelled Counter
per HTTP request. Labels: `method`, `path`, `status`.

```python
app.add_middleware(MetricsMiddleware, counter=HTTP_REQUESTS)
```

### testing

Helpers for unit tests:

- `setup_test_tracing(service_name)` -- replaces the global TracerProvider with an
  in-memory exporter. Returns the exporter so tests can inspect spans.
- `get_spans_by_name(exporter, name)` -- filter exported spans by operation name.
- `find_span_links(span)` -- get all trace links from a span.
- `reset_metrics()` -- unregister all user-created Prometheus collectors between tests.

## Dependencies

- `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp-proto-http`
- `opentelemetry-instrumentation-logging`
- `prometheus-client`
- `python-json-logger`
- `starlette`

## Installation

```bash
pip install -e .
```
