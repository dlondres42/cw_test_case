"""Tests for cw_common.observability.middleware submodule."""

import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from cw_common.observability.middleware import MetricsMiddleware
from cw_common.observability.metrics import create_counter
from cw_common.observability.testing import reset_metrics


class TestMetricsMiddleware(unittest.TestCase):
    """Verify the reusable HTTP-metrics middleware."""

    def setUp(self):
        reset_metrics()

        self.counter = create_counter(
            "test_http_mw_total",
            "test counter",
            ["method", "path", "status"],
        )
        self.app = FastAPI()
        self.app.add_middleware(MetricsMiddleware, counter=self.counter)

        @self.app.get("/ping")
        def ping():
            return {"ok": True}

        @self.app.get("/health")
        def health():
            return {"status": "up"}

        self.client = TestClient(self.app)

    def tearDown(self):
        reset_metrics()

    def test_increments_counter_on_request(self):
        self.client.get("/ping")
        val = self.counter.labels(method="GET", path="/ping", status=200)._value.get()
        self.assertEqual(val, 1.0)

    def test_counts_multiple_requests(self):
        self.client.get("/ping")
        self.client.get("/ping")
        self.client.get("/ping")
        val = self.counter.labels(method="GET", path="/ping", status=200)._value.get()
        self.assertEqual(val, 3.0)

    def test_distinguishes_paths(self):
        self.client.get("/ping")
        self.client.get("/health")
        ping_val = self.counter.labels(method="GET", path="/ping", status=200)._value.get()
        health_val = self.counter.labels(method="GET", path="/health", status=200)._value.get()
        self.assertEqual(ping_val, 1.0)
        self.assertEqual(health_val, 1.0)

    def test_records_status_codes(self):
        self.client.get("/nonexistent")
        val = self.counter.labels(method="GET", path="/nonexistent", status=404)._value.get()
        self.assertEqual(val, 1.0)


class TestMetricsMiddlewareIgnoredPaths(unittest.TestCase):
    """Verify the ignored_paths parameter."""

    def setUp(self):
        reset_metrics()

        self.counter = create_counter(
            "test_http_mw_ignored_total",
            "test counter",
            ["method", "path", "status"],
        )
        self.app = FastAPI()
        self.app.add_middleware(
            MetricsMiddleware,
            counter=self.counter,
            ignored_paths={"/metrics", "/health"},
        )

        @self.app.get("/metrics")
        def metrics():
            return "ok"

        @self.app.get("/health")
        def health():
            return "ok"

        @self.app.get("/api/data")
        def data():
            return {"data": []}

        self.client = TestClient(self.app)

    def tearDown(self):
        reset_metrics()

    def test_ignored_path_not_counted(self):
        self.client.get("/metrics")
        self.client.get("/health")
        # These paths should not increment the counter
        val = self.counter.labels(method="GET", path="/metrics", status=200)._value.get()
        self.assertEqual(val, 0.0)
        val = self.counter.labels(method="GET", path="/health", status=200)._value.get()
        self.assertEqual(val, 0.0)

    def test_non_ignored_path_counted(self):
        self.client.get("/api/data")
        val = self.counter.labels(method="GET", path="/api/data", status=200)._value.get()
        self.assertEqual(val, 1.0)


if __name__ == "__main__":
    unittest.main()
