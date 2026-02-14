import unittest
from unittest.mock import MagicMock, patch
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from cw_common.observability.testing import setup_test_tracing, get_spans_by_name
# Import the module to be tested
import stream_processor.producer as sp

class TestProducerTracing(unittest.TestCase):
    def setUp(self):
        # Set up in-memory tracing
        self.exporter = setup_test_tracing("stream-processor")
        
    def test_produce_span_created(self):
        """Test that produce operations create spans with correct kind/attributes."""
        # Mock batch data
        batch = [{"timestamp": "2023-01-01T12:00:00", "count": 10, "status": "ok"}]
        topic = "test-topic"
        
        # Mock Kafka Producer
        mock_producer = MagicMock()
        
        # We need to mock TraceContextTextMapPropagator behavior indirectly used by stream_loop
        # but since we are running stream_loop, we can just let it run with mocks
        
        # Patch the producer creation in stream_loop
        with patch('confluent_kafka.Producer', return_value=mock_producer):
            # We want to run just one iteration of the loop
            # So we set total batches = 1
            batches = [batch]
            
            # Run stream loop in a separate thread so we can stop it? 
            # Or simplified: we can extract the produce logic.
            # But let's try to run the loop with mocked time.sleep and immediate stop
            
            # Actually, stream_loop is designed to run until finish. 
            # If we pass 1 batch, it finishes after 1 batch.
            sp.stream_loop(batches, "localhost:9092", topic, 0)
            
        # Verify spans
        spans = self.exporter.get_finished_spans()
        produce_spans = [s for s in spans if s.kind == SpanKind.PRODUCER]
        
        self.assertEqual(len(produce_spans), 1)
        span = produce_spans[0]
        
        self.assertEqual(span.name, f"{topic} publish")
        self.assertEqual(span.attributes["messaging.system"], "kafka")
        self.assertEqual(span.attributes["messaging.destination.name"], topic)
        self.assertEqual(span.attributes["messaging.operation.name"], "publish")
        self.assertEqual(span.attributes["messaging.batch.message_count"], 1)

    def test_trace_context_propagation(self):
        """Test that trace context is injected into Kafka headers."""
        batch = [{"timestamp": "2023-01-01T12:00:00", "count": 10, "status": "ok"}]
        topic = "test-topic"
        mock_producer = MagicMock()
        
        with patch('stream_processor.producer.Producer', return_value=mock_producer):
            sp.stream_loop([batch], "localhost:9092", topic, 0)
            
        # Verify producer.produce was called with headers
        self.assertIsNotNone(mock_producer.produce.call_args,
                            "producer.produce should have been called")
        args, kwargs = mock_producer.produce.call_args
        headers = kwargs.get('headers', [])
        
        # Convert headers list of tuples back to dict for checking
        headers_dict = {k: v.decode('utf-8') if isinstance(v, bytes) else v for k, v in headers}
        
        self.assertIn("traceparent", headers_dict)
        self.assertTrue(headers_dict["traceparent"].startswith("00-"))
