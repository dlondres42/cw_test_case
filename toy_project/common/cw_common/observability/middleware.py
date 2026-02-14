"""
Reusable ASGI / Starlette middleware for HTTP request metrics.

Usage::

    from cw_common.observability.middleware import MetricsMiddleware
    from cw_common.observability import create_counter

    HTTP_REQUESTS = create_counter(
        "http_requests_total", "Total HTTP requests", ["method", "path", "status"]
    )

    app.add_middleware(MetricsMiddleware, counter=HTTP_REQUESTS)
"""

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from prometheus_client import Counter


class MetricsMiddleware(BaseHTTPMiddleware):
    """Starlette middleware that increments a labelled Counter per request.

    Args:
        app: The ASGI application.
        counter: A ``prometheus_client.Counter`` with labels
            ``["method", "path", "status"]``.
        ignored_paths: Optional set of paths to skip counting
            (e.g. ``{"/metrics", "/health"}``).
    """

    def __init__(self, app, counter: Counter, ignored_paths: set[str] | None = None):
        super().__init__(app)
        self.counter = counter
        self.ignored_paths = ignored_paths or set()

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        if request.url.path not in self.ignored_paths:
            self.counter.labels(
                method=request.method,
                path=request.url.path,
                status=response.status_code,
            ).inc()

        return response
