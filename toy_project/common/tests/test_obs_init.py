"""Tests for the init_observability bootstrap function."""

import logging
import os
import unittest
from unittest.mock import patch

from cw_common.observability.testing import reset_metrics


class TestInitObservability(unittest.TestCase):
    """Verify the one-call bootstrap wires logging, tracing, and metrics."""

    def setUp(self):
        reset_metrics()
        # Reset logging idempotency guard
        import cw_common.observability.logging as log_mod
        self._orig_setup_done = log_mod._setup_done
        log_mod._setup_done = False

        self._root = logging.getLogger()
        self._original_handlers = self._root.handlers[:]

    def tearDown(self):
        import cw_common.observability.logging as log_mod
        log_mod._setup_done = self._orig_setup_done

        self._root.handlers = self._original_handlers
        reset_metrics()

    def test_sets_up_logging(self):
        """init_observability should configure JSON logging on the root logger."""
        from cw_common.observability import init_observability
        from cw_common.observability.logging import JsonTraceFormatter

        init_observability("test-svc", "1.0.0")

        json_handlers = [
            h for h in self._root.handlers
            if isinstance(h.formatter, JsonTraceFormatter)
        ]
        self.assertGreaterEqual(len(json_handlers), 1)

    def test_creates_service_info_metric(self):
        """init_observability should populate the service-info Prometheus metric."""
        from cw_common.observability import init_observability
        from prometheus_client import REGISTRY

        init_observability("test-init-svc", "2.0.0")

        # Find the info collector
        collector = None
        for c in REGISTRY._names_to_collectors.values():
            if hasattr(c, "_name") and c._name == "test_init_svc":
                collector = c
                break
        self.assertIsNotNone(collector, "Service info metric should exist")

        samples = list(collector.collect()[0].samples)
        versions = [s.labels.get("version") for s in samples if "version" in s.labels]
        self.assertIn("2.0.0", versions)

    def test_tracing_skipped_without_env(self):
        """Without OTEL_EXPORTER_OTLP_ENDPOINT, tracing should be skipped."""
        from cw_common.observability import init_observability

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)
            # Should not raise
            init_observability("test-no-trace", "0.1.0")

    @patch("cw_common.observability.init_tracing", side_effect=RuntimeError("boom"))
    def test_tracing_failure_is_non_fatal(self, mock_init):
        """If tracing init fails, the error should be swallowed."""
        from cw_common.observability import init_observability

        with patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318"}):
            # Should not raise despite the side effect
            init_observability("test-trace-fail", "0.1.0")

    def test_idempotent_logging(self):
        """Calling init_observability twice should not double handlers."""
        from cw_common.observability import init_observability

        init_observability("test-idem", "1.0.0")
        count1 = len(self._root.handlers)
        init_observability("test-idem2", "1.0.0")
        self.assertEqual(len(self._root.handlers), count1)


if __name__ == "__main__":
    unittest.main()
