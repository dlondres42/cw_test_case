import unittest
from unittest.mock import MagicMock
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from cw_common.observability.testing import setup_test_tracing, find_span_links
from cw_common.observability.propagation import inject_trace_context, extract_trace_context

class TestTracePropagationIntegration(unittest.TestCase):
    def setUp(self):
        # Setup independent exporters for "producer" and "consumer"
        # In a real scenario these are separate processes, but here we enforce separation
        # by creating separate TracerProviders if we could, but setup_test_tracing sets the global provider.
        # So we'll share the global provider but distinguish by service name or just assume they share it 
        # (which is fine for checking ids).
        self.exporter = setup_test_tracing("integration-test")
        self.tracer = trace.get_tracer(__name__)

    def test_producer_to_consumer_flow(self):
        """
        Simulate the full flow:
        1. Producer creates span & injects headers
        2. Headers passed (via 'Kafka') to consumer
        3. Consumer extracts headers & creates span with link
        """
        
        # --- PRODUCER SIDE ---
        producer_headers = {}
        with self.tracer.start_as_current_span("producer_span", kind=SpanKind.PRODUCER) as prod_span:
            # Inject context into headers
            inject_trace_context(producer_headers)
            producer_context = prod_span.get_span_context()
            
        # Verify headers have traceparent
        self.assertIn("traceparent", producer_headers)
        
        # --- MIDDLEWARE (Kafka) ---
        # Simulate text map propagation (Kafka headers are just passed along)
        consumer_headers = producer_headers.copy()
        
        # --- CONSUMER SIDE ---
        # Extract context
        links = extract_trace_context(consumer_headers)
        
        with self.tracer.start_as_current_span(
            "consumer_span", 
            kind=SpanKind.CONSUMER,
            links=links
        ) as cons_span:
            pass
            
        # --- VERIFICATION ---
        spans = self.exporter.get_finished_spans()
        prod_span_data = next(s for s in spans if s.name == "producer_span")
        cons_span_data = next(s for s in spans if s.name == "consumer_span")
        
        # 1. Traces should be different (because we are using Links, not Parent-Child)
        # Wait, if we use start_as_current_span without passing context, it creates a new root span.
        # Correct, that's what we want for independent traces referenced by links.
        self.assertNotEqual(prod_span_data.context.trace_id, cons_span_data.context.trace_id)
        
        # 2. Consumer span should have a link to Producer span
        cons_links = find_span_links(cons_span_data)
        self.assertEqual(len(cons_links), 1)
        
        link = cons_links[0]
        self.assertEqual(link.context.trace_id, prod_span_data.context.trace_id)
        self.assertEqual(link.context.span_id, prod_span_data.context.span_id)
