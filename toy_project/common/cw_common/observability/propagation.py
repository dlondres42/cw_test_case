from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.trace import Link

def inject_trace_context(headers: dict) -> dict:
    """
    Inject W3C traceparent into a headers dict.
    Use this before producing a message to Kafka.
    """
    propagator = TraceContextTextMapPropagator()
    propagator.inject(headers)
    return headers

def extract_trace_context(headers: dict) -> list[Link]:
    """
    Extract W3C traceparent from headers and return span Links.
    Use this when consuming a message from Kafka.
    """
    propagator = TraceContextTextMapPropagator()
    ctx = propagator.extract(carrier=headers)
    
    # Create a link if a valid context was extracted
    if ctx:
        span_ctx = trace.get_current_span(ctx).get_span_context()
        if span_ctx.is_valid:
            return [Link(span_ctx)]
    
    return []

def kafka_headers_to_dict(headers: list[tuple] | None) -> dict:
    """
    Convert confluent-kafka header tuples to a plain dict.
    Handles bytes decoding.
    """
    if not headers:
        return {}
        
    headers_dict = {}
    for key, value in headers:
        if value:
            headers_dict[key] = value.decode('utf-8') if isinstance(value, bytes) else value
    return headers_dict

def dict_to_kafka_headers(headers: dict) -> list[tuple]:
    """
    Convert a plain dict to confluent-kafka header tuples.
    Handles string encoding.
    """
    return [(k, v.encode('utf-8') if isinstance(v, str) else v) for k, v in headers.items()]
