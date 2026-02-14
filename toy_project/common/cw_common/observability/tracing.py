import os
import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

logger = logging.getLogger(__name__)

def init_tracing(service_name: str, endpoint: str | None = None) -> None:
    """
    Initialize OpenTelemetry tracing with OTLP HTTP exporter.
    
    Args:
        service_name: Name of the service (e.g. "monitoring-api")
        endpoint: OTLP HTTP endpoint (e.g. "http://jaeger:4318"). 
                 If None, tries OTEL_EXPORTER_OTLP_ENDPOINT env var, 
                 defaults to http://localhost:4318.
    """
    if endpoint is None:
        endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
        
    # HTTP exporter needs full URL with /v1/traces path if not already present
    if not endpoint.endswith("/v1/traces"):
        traces_endpoint = f"{endpoint}/v1/traces"
    else:
        traces_endpoint = endpoint

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=traces_endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    
    logger.info(f"Tracing initialized for {service_name} (exporting to {traces_endpoint})")

def shutdown_tracing() -> None:
    """Flush and shutdown the global tracer provider."""
    try:
        provider = trace.get_tracer_provider()
        if hasattr(provider, "shutdown"):
            provider.shutdown()
            logger.info("Tracer shutdown complete")
    except Exception as e:
        logger.warning(f"Tracer shutdown warning: {e}")
