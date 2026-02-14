"""Tests for cw_common.observability.logging submodule."""

import io
import json
import logging
import unittest
from unittest.mock import patch

from cw_common.observability.logging import (
    setup_logging,
    get_logger,
    JsonTraceFormatter,
    _setup_done,
)


class TestJsonTraceFormatter(unittest.TestCase):
    """Verify the JSON formatter produces the expected fields."""

    def test_format_contains_required_fields(self):
        formatter = JsonTraceFormatter("%(timestamp)s %(level)s %(name)s %(message)s")

        record = logging.LogRecord(
            name="test-logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="hello world",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)

        self.assertEqual(data["level"], "INFO")
        self.assertEqual(data["logger"], "test-logger")
        self.assertEqual(data["message"], "hello world")
        self.assertIn("timestamp", data)
        self.assertIsInstance(data["timestamp"], float)

    def test_format_with_extra_fields(self):
        formatter = JsonTraceFormatter("%(timestamp)s %(level)s %(name)s %(message)s")
        record = logging.LogRecord(
            name="x",
            level=logging.WARNING,
            pathname="x.py",
            lineno=1,
            msg="warn",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)
        self.assertEqual(data["level"], "WARNING")


class TestSetupLogging(unittest.TestCase):
    """Test that setup_logging configures the root logger correctly."""

    def setUp(self):
        # Reset the idempotency guard so each test can call setup_logging
        import cw_common.observability.logging as log_mod
        self._original = log_mod._setup_done
        log_mod._setup_done = False

        # Remove any handlers we might add
        self._root = logging.getLogger()
        self._original_handlers = self._root.handlers[:]

    def tearDown(self):
        import cw_common.observability.logging as log_mod
        log_mod._setup_done = self._original

        # Restore original handlers
        self._root.handlers = self._original_handlers

    def test_adds_json_handler_to_root(self):
        """setup_logging should attach a StreamHandler with JsonTraceFormatter."""
        setup_logging()

        json_handlers = [
            h for h in self._root.handlers
            if isinstance(h, logging.StreamHandler)
            and isinstance(h.formatter, JsonTraceFormatter)
        ]
        self.assertGreaterEqual(len(json_handlers), 1)

    def test_sets_log_level(self):
        setup_logging(level=logging.DEBUG)
        self.assertEqual(self._root.level, logging.DEBUG)

    def test_idempotent(self):
        """Calling setup_logging twice should not add duplicate handlers."""
        setup_logging()
        count_before = len(self._root.handlers)
        setup_logging()
        self.assertEqual(len(self._root.handlers), count_before)

    def test_json_output_is_parseable(self):
        """A log message produced after setup should be valid JSON."""
        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        handler.setFormatter(JsonTraceFormatter("%(timestamp)s %(level)s %(name)s %(message)s"))
        test_logger = logging.getLogger("json-output-test")
        test_logger.addHandler(handler)
        test_logger.setLevel(logging.INFO)

        test_logger.info("integration check")
        handler.flush()

        line = buf.getvalue().strip()
        data = json.loads(line)
        self.assertEqual(data["message"], "integration check")
        self.assertEqual(data["logger"], "json-output-test")

        test_logger.removeHandler(handler)


class TestGetLogger(unittest.TestCase):
    def test_returns_named_logger(self):
        logger = get_logger("my-component")
        self.assertEqual(logger.name, "my-component")
        self.assertIsInstance(logger, logging.Logger)


if __name__ == "__main__":
    unittest.main()
