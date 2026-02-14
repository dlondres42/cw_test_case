"""
Test utilities for the observability stack.

Provides helpers to set up in-memory tracing exporters, query exported
spans, and reset the Prometheus collector registry between tests.
"""

from prometheus_client import REGISTRY
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Link


def setup_test_tracing(service_name: str = "test-service") -> InMemorySpanExporter:
    """
    Set up a TracerProvider with InMemorySpanExporter for tests.
    Returns the exporter instance so you can inspect spans.
    Forcefully replaces any existing provider to work across multiple tests.
    """
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    # Forcefully replace the global provider (bypass the "already set" guard)
    trace._TRACER_PROVIDER = None
    trace._TRACER_PROVIDER_SET_ONCE._done = False
    trace.set_tracer_provider(provider)
    return exporter


def get_spans_by_name(exporter: InMemorySpanExporter, name: str) -> list[ReadableSpan]:
    """Filter exported spans by operation name."""
    return [s for s in exporter.get_finished_spans() if s.name == name]


def find_span_links(span: ReadableSpan) -> list[Link]:
    """Get all links from a span."""
    return list(span.links) if span.links else []


def reset_metrics() -> None:
    """
    Unregister all user-created collectors from the default Prometheus
    registry so the next test gets a clean slate.

    Keeps platform collectors (``gc``, ``process``, ``platform``) intact.
    """
    to_remove = []
    for name, collector in list(REGISTRY._names_to_collectors.items()):
        # Platform / internal collectors don't have _name
        if hasattr(collector, "_name"):
            to_remove.append(collector)

    seen = set()
    for collector in to_remove:
        cid = id(collector)
        if cid not in seen:
            seen.add(cid)
            try:
                REGISTRY.unregister(collector)
            except Exception:
                pass
