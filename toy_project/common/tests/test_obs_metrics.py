"""Tests for cw_common.observability.metrics submodule."""

import unittest

from cw_common.observability.testing import reset_metrics


class TestMetricFactories(unittest.TestCase):
    """Verify create_* functions and idempotent registration."""

    def setUp(self):
        reset_metrics()

    def tearDown(self):
        reset_metrics()

    def test_create_counter_basic(self):
        from cw_common.observability.metrics import create_counter

        c = create_counter("test_counter_basic", "A test counter")
        c.inc()
        self.assertEqual(c._value.get(), 1.0)

    def test_create_counter_idempotent(self):
        from cw_common.observability.metrics import create_counter

        c1 = create_counter("test_counter_idem", "counter")
        c2 = create_counter("test_counter_idem", "counter")
        self.assertIs(c1, c2)

    def test_create_histogram_with_buckets(self):
        from cw_common.observability.metrics import create_histogram

        h = create_histogram(
            "test_hist_buckets", "A test histogram", buckets=[0.1, 0.5, 1.0]
        )
        h.observe(0.3)
        self.assertGreater(h._sum.get(), 0)

    def test_create_histogram_idempotent(self):
        from cw_common.observability.metrics import create_histogram

        h1 = create_histogram("test_hist_idem", "hist")
        h2 = create_histogram("test_hist_idem", "hist")
        self.assertIs(h1, h2)

    def test_create_info(self):
        from cw_common.observability.metrics import create_info

        info = create_info("test_info_metric", "info")
        info.info({"version": "1.0"})
        # Just verify it doesn't raise
        self.assertIsNotNone(info)

    def test_create_gauge(self):
        from cw_common.observability.metrics import create_gauge

        g = create_gauge("test_gauge_basic", "A test gauge")
        g.set(42)
        self.assertEqual(g._value.get(), 42.0)


class TestCreateServiceInfo(unittest.TestCase):
    """Verify the create_service_info convenience helper."""

    def setUp(self):
        reset_metrics()

    def tearDown(self):
        reset_metrics()

    def test_creates_and_populates(self):
        from cw_common.observability.metrics import create_service_info

        info = create_service_info("test_svc", "1.2.3", "staging")
        # The info metric should have been populated — check via collect()
        samples = list(info.collect()[0].samples)
        sample_dict = {s.labels.get("version"): s for s in samples if "version" in s.labels}
        self.assertIn("1.2.3", sample_dict)

    def test_defaults_environment(self):
        from cw_common.observability.metrics import create_service_info

        info = create_service_info("test_svc_default", "0.0.1")
        samples = list(info.collect()[0].samples)
        env_values = [s.labels.get("environment") for s in samples if "environment" in s.labels]
        self.assertIn("development", env_values)


class TestMetricsResponse(unittest.TestCase):
    """Verify the metrics_response helper."""

    def test_returns_bytes_and_content_type(self):
        from cw_common.observability.metrics import metrics_response

        body, content_type = metrics_response()
        self.assertIsInstance(body, bytes)
        self.assertIn("text/plain", content_type)
        # Should contain at least some prometheus output
        self.assertGreater(len(body), 0)


if __name__ == "__main__":
    unittest.main()
