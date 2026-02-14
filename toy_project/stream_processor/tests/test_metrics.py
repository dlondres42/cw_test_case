import unittest
from unittest.mock import patch, MagicMock
import prometheus_client


def _histogram_count(histogram):
    """Get the total observation count from a Histogram via collect()."""
    for sample in histogram.collect()[0].samples:
        if sample.name.endswith('_count'):
            return sample.value
    return 0.0


class TestStreamProcessorMetrics(unittest.TestCase):
    """Test stream-processor metrics integration."""
    
    def test_metrics_recorded_during_stream_loop(self):
        """Test that metrics are recorded when producing batches."""
        from stream_processor.producer import stream_loop
        from stream_processor import telemetry
        
        # Get initial metric values
        initial_batches = telemetry.BATCHES_PRODUCED._value.get()
        initial_records = telemetry.RECORDS_PRODUCED._value.get()
        initial_duration_count = _histogram_count(telemetry.PRODUCE_DURATION)
        
        # Mock Kafka Producer
        with patch('stream_processor.producer.Producer') as MockProducer:
            mock_producer = MagicMock()
            MockProducer.return_value = mock_producer
            mock_producer.poll.return_value = None
            mock_producer.flush.return_value = 0
            
            # Create test batches
            test_batches = [
                [{"timestamp": "2024-01-01 00:00:00", "amount": 100.0}],
                [{"timestamp": "2024-01-01 00:00:01", "amount": 200.0}, 
                 {"timestamp": "2024-01-01 00:00:01", "amount": 300.0}],
                [{"timestamp": "2024-01-01 00:00:02", "amount": 50.0}]
            ]
            
            # Run stream loop with no delay
            stream_loop(test_batches, "fake:9092", "test-topic", 0.0)
        
        # Verify metrics were incremented
        final_batches = telemetry.BATCHES_PRODUCED._value.get()
        final_records = telemetry.RECORDS_PRODUCED._value.get()
        final_duration_count = _histogram_count(telemetry.PRODUCE_DURATION)
        
        # Should have processed 3 batches
        self.assertEqual(final_batches - initial_batches, 3,
                        "Should have incremented batch counter 3 times")
        
        # Should have processed 1 + 2 + 1 = 4 records
        self.assertEqual(final_records - initial_records, 4,
                        "Should have incremented records counter by total record count")
        
        # Should have recorded 3 duration observations
        self.assertEqual(final_duration_count - initial_duration_count, 3,
                        "Should have recorded duration for each batch")
    
    def test_service_info_via_init_observability(self):
        """Test that service info metric is created by init_observability."""
        from cw_common.observability import create_service_info
        from prometheus_client import REGISTRY

        info = create_service_info("stream_processor", "0.2.0")
        self.assertIsNotNone(info)

        # Verify the metric is registered
        output = prometheus_client.generate_latest(REGISTRY).decode()
        self.assertIn("stream_processor", output)


if __name__ == '__main__':
    unittest.main()
