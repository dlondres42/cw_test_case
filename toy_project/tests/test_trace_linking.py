import unittest
from unittest.mock import MagicMock, patch
from opentelemetry.trace import SpanKind

from cw_common.observability.testing import setup_test_tracing, get_spans_by_name, find_span_links


class TestCrossServiceTracing(unittest.TestCase):
    """Test actual trace linking between stream-processor and monitoring-api."""
    
    def setUp(self):
        self.exporter = setup_test_tracing("cross-service-test")
    
    def test_producer_consumer_trace_linking(self):
        """
        Verify that producer and consumer create properly linked traces.
        This test simulates the full producer -> Kafka -> consumer flow.
        """
        from stream_processor.producer import stream_loop
        from monitoring_service.app.consumer import _consume_loop
        from confluent_kafka import Consumer, KafkaError
        import threading
        
        # Mock Kafka Producer
        # Prevent shutdown_tracing from killing the test TracerProvider
        with patch('stream_processor.producer.shutdown_tracing'):
          with patch('stream_processor.producer.Producer') as MockProducer:
            mock_producer = MagicMock()
            MockProducer.return_value = mock_producer
            
            # Capture headers passed to producer
            headers_captured = []
            def capture_produce(*args, **kwargs):
                if 'headers' in kwargs:
                    headers_captured.append(kwargs['headers'])
            
            mock_producer.produce.side_effect = capture_produce
            mock_producer.poll.return_value = None
            mock_producer.flush.return_value = 0
            
            # Create a single batch
            test_batch = [[{
                "timestamp": "2024-01-01 00:00:00",
                "amount": 100.0,
                "status": "success"
            }]]
            
            # Run producer
            stream_loop(test_batch, "fake:9092", "test-topic", 0.0)
        
        # Verify producer created spans and injected headers
        self.assertGreater(len(headers_captured), 0, "Producer should inject headers")
        kafka_headers = headers_captured[0]
        
        # Convert list of tuples back to dict
        header_dict = {k: v.decode('utf-8') if isinstance(v, bytes) else v 
                      for k, v in kafka_headers}
        self.assertIn('traceparent', header_dict)
        
        producer_spans = get_spans_by_name(self.exporter, "test-topic publish")
        self.assertEqual(len(producer_spans), 1)
        producer_span = producer_spans[0]
        self.assertEqual(producer_span.kind, SpanKind.PRODUCER)
        
        # Mock Kafka Consumer  
        # Consumer uses KAFKA_TOPIC env var (default "transactions") for span names
        with patch('monitoring_service.app.consumer.Consumer') as MockConsumer:
            with patch('monitoring_service.app.consumer.insert_transactions', return_value=1):
                mock_consumer = MagicMock()
                MockConsumer.return_value = mock_consumer
                
                # Create mock message with the captured headers
                mock_msg = MagicMock()
                mock_msg.error.return_value = None
                mock_msg.value.return_value = b'{"records": [{"timestamp": "2024-01-01 00:00:00", "amount": 100.0}]}'
                mock_msg.headers.return_value = kafka_headers
                mock_msg.partition.return_value = 0
                mock_msg.offset.return_value = 0
                
                # Mock poll to return message once, then trigger stop
                stop_event = threading.Event()
                
                def mock_poll(timeout):
                    if not hasattr(mock_poll, '_called'):
                        mock_poll._called = True
                        return mock_msg
                    # Signal stop after processing the first message
                    stop_event.set()
                    return None
                
                mock_consumer.poll.side_effect = mock_poll
                
                _consume_loop(stop_event)
        
        # Consumer uses KAFKA_TOPIC env var (default "transactions")
        from monitoring_service.app.consumer import KAFKA_TOPIC
        consumer_topic = KAFKA_TOPIC
        
        # Verify consumer created spans with links
        consumer_spans = get_spans_by_name(self.exporter, f"{consumer_topic} receive")
        self.assertGreater(len(consumer_spans), 0, "Consumer should create spans")
        
        consumer_span = consumer_spans[0]
        self.assertEqual(consumer_span.kind, SpanKind.CONSUMER)
        
        # Verify the link exists
        links = find_span_links(consumer_span)
        self.assertEqual(len(links), 1, "Consumer span should have exactly one link")
        
        link = links[0]
        # Link should point to producer's trace and span
        self.assertEqual(link.context.trace_id, producer_span.context.trace_id,
                        "Link should reference producer's trace ID")
        self.assertEqual(link.context.span_id, producer_span.context.span_id,
                        "Link should reference producer's span ID")
        
        # Verify they have different trace IDs (independent traces)
        self.assertNotEqual(consumer_span.context.trace_id, producer_span.context.trace_id,
                           "Consumer and producer should have different trace IDs (linked, not parent-child)")
        
        # Verify producer trace/span IDs are stored as attributes
        producer_trace_id_hex = format(producer_span.context.trace_id, '032x')
        producer_span_id_hex = format(producer_span.context.span_id, '016x')
        
        self.assertEqual(
            consumer_span.attributes.get("messaging.producer.trace_id"),
            producer_trace_id_hex,
            "Consumer span should have producer trace ID as attribute"
        )
        self.assertEqual(
            consumer_span.attributes.get("messaging.producer.span_id"),
            producer_span_id_hex,
            "Consumer span should have producer span ID as attribute"
        )
    
    def test_trace_context_format(self):
        """Verify injected trace context follows W3C TraceContext format."""
        from cw_common.observability.propagation import inject_trace_context
        from opentelemetry import trace
        
        tracer = trace.get_tracer(__name__)
        
        with tracer.start_as_current_span("test_span"):
            headers = {}
            inject_trace_context(headers)
            
            self.assertIn('traceparent', headers)
            traceparent = headers['traceparent']
            
            # W3C format: version-trace_id-span_id-flags
            # Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
            parts = traceparent.split('-')
            self.assertEqual(len(parts), 4)
            self.assertEqual(parts[0], '00', "Should use version 00")
            self.assertEqual(len(parts[1]), 32, "Trace ID should be 32 hex chars")
            self.assertEqual(len(parts[2]), 16, "Span ID should be 16 hex chars")
    
    def test_extract_trace_context_returns_metadata(self):
        """Verify extract_trace_context returns both links and metadata."""
        from cw_common.observability.propagation import inject_trace_context, extract_trace_context
        from opentelemetry import trace
        
        tracer = trace.get_tracer(__name__)
        
        with tracer.start_as_current_span("test_span") as span:
            # Inject context
            headers = {}
            inject_trace_context(headers)
            
            # Extract context
            links, metadata = extract_trace_context(headers)
            
            # Verify we got both links and metadata
            self.assertEqual(len(links), 1, "Should extract one link")
            self.assertIn("producer_trace_id", metadata)
            self.assertIn("producer_span_id", metadata)
            
            # Verify metadata matches the span context
            expected_trace_id = format(span.context.trace_id, '032x')
            expected_span_id = format(span.context.span_id, '016x')
            
            self.assertEqual(metadata["producer_trace_id"], expected_trace_id)
            self.assertEqual(metadata["producer_span_id"], expected_span_id)


if __name__ == '__main__':
    unittest.main()
