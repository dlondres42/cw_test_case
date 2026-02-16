from opentelemetry import trace, context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

def inject_trace_context(headers: dict) -> dict:
    """
    Inject W3C traceparent into a headers dict.
    Use this before producing a message to Kafka.
    """
    propagator = TraceContextTextMapPropagator()
    propagator.inject(headers)
    return headers

def extract_trace_context(headers: dict) -> context.Context:
    """
    Extract W3C traceparent from headers and return the context.
    
    Returns:
        Context: The extracted context that should be set as parent for consumer spans.
                 Returns None if no valid context found.
    
    Use this when consuming a message from Kafka to continue the distributed trace
    started by the producer.
    """
    propagator = TraceContextTextMapPropagator()
    ctx = propagator.extract(carrier=headers)
    return ctx

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
