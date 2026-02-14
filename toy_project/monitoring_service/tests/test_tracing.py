import unittest
from unittest.mock import MagicMock, patch
import json
import threading
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from cw_common.observability.testing import setup_test_tracing, find_span_links
import app.consumer as consumer_module

class TestConsumerTracing(unittest.TestCase):
    def setUp(self):
        self.exporter = setup_test_tracing("monitoring-api")
        
    @patch("app.consumer.Consumer")
    @patch("app.consumer.insert_transactions")
    def test_consume_span_with_link(self, mock_insert, MockConsumer):
        """Test that consuming a message creates a span with a link to the producer."""
        
        # 1. Create a dummy producer trace ID and span ID
        producer_trace_id = 0x12345678123456781234567812345678
        producer_span_id = 0x1111222233334444
        traceparent = f"00-{producer_trace_id:032x}-{producer_span_id:016x}-01"
        
        # 2. Mock Kafka message with traceparent header
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.headers.return_value = [("traceparent", traceparent.encode("utf-8"))]
        mock_msg.value.return_value = json.dumps({"records": [{"timestamp": "2023-01-01"}]}).encode("utf-8")
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100
        
        # 3. Mock Consumer poll to return msg once, then stop
        mock_consumer_instance = MockConsumer.return_value
        # We need to stop the loop after one message. 
        # We can use a side_effect on poll that returns msg once, 
        # then sets the stop_event?
        
        stop_event = threading.Event()
        
        def side_effect(*args, **kwargs):
            if not getattr(side_effect, 'called', False):
                side_effect.called = True
                return mock_msg
            stop_event.set()
            return None
            
        mock_consumer_instance.poll.side_effect = side_effect
        
        # 4. Run consume loop
        consumer_module._consume_loop(stop_event)
        
        # 5. Verify spans
        spans = self.exporter.get_finished_spans()
        consume_spans = [s for s in spans if s.kind == SpanKind.CONSUMER]
        
        self.assertEqual(len(consume_spans), 1)
        span = consume_spans[0]
        
        # Verify attributes
        self.assertEqual(span.attributes["messaging.operation.name"], "receive")
        self.assertEqual(span.attributes["messaging.kafka.offset"], 100)
        
        # Verify Link
        links = find_span_links(span)
        self.assertEqual(len(links), 1)
        link = links[0]
        self.assertEqual(link.context.trace_id, producer_trace_id)
        self.assertEqual(link.context.span_id, producer_span_id)
