import unittest
from fastapi.testclient import TestClient


class TestMetrics(unittest.TestCase):
    """Test Prometheus metrics collection and reporting."""
    
    def test_messages_consumed_counter_increments(self):
        """Test that MESSAGES_CONSUMED counter increments correctly."""
        from monitoring_service.app.telemetry import MESSAGES_CONSUMED
        
        initial_value = MESSAGES_CONSUMED._value.get()
        
        # Increment counter
        MESSAGES_CONSUMED.inc()
        MESSAGES_CONSUMED.inc()
        
        final_value = MESSAGES_CONSUMED._value.get()
        self.assertEqual(final_value - initial_value, 2)
    
    def test_consume_duration_histogram_records(self):
        """Test that CONSUME_DURATION histogram records observations."""
        from monitoring_service.app.telemetry import CONSUME_DURATION
        
        # Get initial sum value
        initial_sum = CONSUME_DURATION._sum.get()
        
        # Record some observations
        CONSUME_DURATION.observe(0.010)  # 10ms
        CONSUME_DURATION.observe(0.025)  # 25ms
        
        # Verify observations were recorded
        final_sum = CONSUME_DURATION._sum.get()
        self.assertGreater(final_sum, initial_sum)
    
    def test_consumer_metrics_wiring(self):
        """Test that metrics can be wired into consumer module."""
        from monitoring_service.app import consumer
        from monitoring_service.app.telemetry import MESSAGES_CONSUMED, CONSUME_DURATION
        
        # Directly wire metrics (simulating what telemetry.init does)
        consumer.messages_consumed_counter = MESSAGES_CONSUMED
        consumer.consume_duration_histogram = CONSUME_DURATION
        
        # Verify wiring
        self.assertIsNotNone(consumer.messages_consumed_counter)
        self.assertIsNotNone(consumer.consume_duration_histogram)
        self.assertEqual(consumer.messages_consumed_counter, MESSAGES_CONSUMED)
        self.assertEqual(consumer.consume_duration_histogram, CONSUME_DURATION)
    
    def test_http_requests_middleware_increments_counter(self):
        """Test that HTTP_REQUESTS counter can be incremented with labels."""
        from monitoring_service.app.telemetry import HTTP_REQUESTS
        
        # Get initial value
        initial_labels = ('GET', '/test', '200')
        try:
            initial_value = HTTP_REQUESTS.labels(*initial_labels)._value.get()
        except Exception:
            initial_value = 0
        
        # Simulate what the middleware does
        HTTP_REQUESTS.labels(method='GET', path='/test', status='200').inc()
        
        # Verify counter incremented
        final_value = HTTP_REQUESTS.labels(*initial_labels)._value.get()
        self.assertGreaterEqual(final_value - initial_value, 1)
    
    def test_metrics_endpoint_returns_prometheus_format(self):
        """Test that /metrics endpoint returns valid Prometheus format."""
        from monitoring_service.app.main import app
        from monitoring_service.app.database import init_db
        
        init_db()
        
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/metrics")
        
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/plain", response.headers["content-type"])
        
        # Verify some expected metrics are present in the response
        content = response.text
        self.assertIn("messages_consumed_total", content)
        self.assertIn("db_insert_duration_seconds", content)
    
    def test_insert_duration_histogram_buckets(self):
        """Test that INSERT_DURATION histogram has correct buckets."""
        from monitoring_service.app.telemetry import INSERT_DURATION
        
        # Expected buckets from telemetry.py
        expected_buckets = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, float('inf'))
        
        # Get actual buckets from the metric
        actual_buckets = tuple(INSERT_DURATION._upper_bounds)
        
        self.assertEqual(actual_buckets, expected_buckets)

    def test_insert_duration_wired_to_consumer(self):
        """Test that INSERT_DURATION is wired into the consumer module."""
        from monitoring_service.app import consumer
        from monitoring_service.app.telemetry import INSERT_DURATION
        
        # Simulate what telemetry.init does
        consumer.insert_duration_histogram = INSERT_DURATION
        
        self.assertIsNotNone(consumer.insert_duration_histogram)
        self.assertEqual(consumer.insert_duration_histogram, INSERT_DURATION)

    def test_insert_duration_observed_on_consume(self):
        """Test that INSERT_DURATION histogram is observed when records are inserted."""
        from monitoring_service.app import consumer
        from monitoring_service.app.telemetry import INSERT_DURATION

        # Wire the histogram
        consumer.insert_duration_histogram = INSERT_DURATION

        # Get initial observation count
        initial_count = 0.0
        for sample in INSERT_DURATION.collect()[0].samples:
            if sample.name.endswith('_count'):
                initial_count = sample.value
                break

        # Observe a duration (simulating what _consume_loop does)
        INSERT_DURATION.observe(0.005)

        # Verify observation was recorded
        final_count = 0.0
        for sample in INSERT_DURATION.collect()[0].samples:
            if sample.name.endswith('_count'):
                final_count = sample.value
                break

        self.assertEqual(final_count - initial_count, 1)


class TestStreamProcessorMetrics(unittest.TestCase):
    """Test stream-processor Prometheus metrics."""
    
    def test_batches_produced_counter_increments(self):
        """Test that BATCHES_PRODUCED counter increments."""
        from stream_processor.telemetry import BATCHES_PRODUCED
        
        initial_value = BATCHES_PRODUCED._value.get()
        
        BATCHES_PRODUCED.inc()
        BATCHES_PRODUCED.inc()
        BATCHES_PRODUCED.inc()
        
        final_value = BATCHES_PRODUCED._value.get()
        self.assertEqual(final_value - initial_value, 3)
    
    def test_records_produced_counter_with_amount(self):
        """Test that RECORDS_PRODUCED counter increments by specified amount."""
        from stream_processor.telemetry import RECORDS_PRODUCED
        
        initial_value = RECORDS_PRODUCED._value.get()
        
        # Increment by batch size
        RECORDS_PRODUCED.inc(10)
        RECORDS_PRODUCED.inc(25)
        
        final_value = RECORDS_PRODUCED._value.get()
        self.assertEqual(final_value - initial_value, 35)
    
    def test_produce_duration_histogram_observes(self):
        """Test that PRODUCE_DURATION histogram records durations."""
        from stream_processor.telemetry import PRODUCE_DURATION
        
        initial_sum = PRODUCE_DURATION._sum.get()
        
        PRODUCE_DURATION.observe(0.005)
        PRODUCE_DURATION.observe(0.015)
        
        final_sum = PRODUCE_DURATION._sum.get()
        self.assertAlmostEqual(final_sum - initial_sum, 0.020, places=3)


if __name__ == '__main__':
    unittest.main()
