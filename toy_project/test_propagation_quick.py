#!/usr/bin/env python
"""Quick test to verify the propagation changes work correctly."""
import sys
sys.path.insert(0, '.')

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource

from cw_common.observability.propagation import inject_trace_context, extract_trace_context

# Set up minimal tracing
resource = Resource.create({"service.name": "test"})
provider = TracerProvider(resource=resource)
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

# Test the functionality
with tracer.start_as_current_span("producer_span") as producer_span:
    # Inject context into headers
    headers = {}
    inject_trace_context(headers)
    
    print(f"✓ Injected traceparent: {headers.get('traceparent', 'MISSING')}")
    
    # Extract context and metadata
    links, metadata = extract_trace_context(headers)
    
    print(f"✓ Extracted {len(links)} link(s)")
    print(f"✓ Metadata keys: {list(metadata.keys())}")
    
    # Verify metadata
    expected_trace_id = format(producer_span.context.trace_id, '032x')
    expected_span_id = format(producer_span.context.span_id, '016x')
    
    assert metadata.get("producer_trace_id") == expected_trace_id, "Trace ID mismatch!"
    assert metadata.get("producer_span_id") == expected_span_id, "Span ID mismatch!"
    
    print(f"✓ Producer trace ID: {metadata.get('producer_trace_id')}")
    print(f"✓ Producer span ID: {metadata.get('producer_span_id')}")
    
    print("\n✅ All checks passed! The propagation changes work correctly.")
