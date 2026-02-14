"""
Prometheus metrics factory functions with idempotent registration.

Provides ``create_counter``, ``create_histogram``, ``create_info``,
``create_gauge`` wrappers that safely handle duplicate registrations,
plus ``create_service_info`` for the standard service-metadata pattern
and ``metrics_response`` for generating a Prometheus HTTP response.
"""

import os

from prometheus_client import Counter, Histogram, Info, Gauge, REGISTRY, generate_latest, CONTENT_TYPE_LATEST


def _get_or_create(metric_cls, name, documentation, **kwargs):
    """Create a metric or return the existing one if already registered."""
    try:
        return metric_cls(name, documentation, **kwargs)
    except ValueError:
        # Already registered — look it up in the default registry
        for collector in REGISTRY._names_to_collectors.values():
            if hasattr(collector, '_name') and (
                collector._name == name or 
                getattr(collector, '_original_name', None) == name
            ):
                return collector
        raise


def create_counter(name: str, documentation: str, labelnames: list[str] = None) -> Counter:
    """Create (or retrieve) a Prometheus Counter."""
    return _get_or_create(Counter, name, documentation, labelnames=labelnames or [])


def create_histogram(name: str, documentation: str, buckets: list[float] = None, labelnames: list[str] = None) -> Histogram:
    """Create (or retrieve) a Prometheus Histogram."""
    kwargs = {}
    if buckets:
        kwargs["buckets"] = buckets
    if labelnames:
        kwargs["labelnames"] = labelnames
    return _get_or_create(Histogram, name, documentation, **kwargs)


def create_info(name: str, documentation: str) -> Info:
    """Create (or retrieve) a Prometheus Info metric."""
    return _get_or_create(Info, name, documentation)


def create_gauge(name: str, documentation: str, labelnames: list[str] = None) -> Gauge:
    """Create (or retrieve) a Prometheus Gauge."""
    return _get_or_create(Gauge, name, documentation, labelnames=labelnames or [])


def create_service_info(service_name: str, version: str, environment: str | None = None) -> Info:
    """
    Create and populate a service-metadata Info metric.

    Args:
        service_name: Prometheus metric name prefix (e.g. ``"monitoring_service"``).
        version: Service version string (e.g. ``"0.3.0"``).
        environment: Deployment environment.  Falls back to the
            ``ENVIRONMENT`` env-var, then ``"development"``.

    Returns:
        The populated ``Info`` collector.
    """
    info = create_info(service_name, "Service metadata")
    info.info({
        "version": version,
        "environment": environment or os.environ.get("ENVIRONMENT", "development"),
    })
    return info


def metrics_response():
    """
    Return Prometheus exposition-format bytes and the matching content-type.

    Returns:
        tuple[bytes, str]: ``(body, content_type)`` ready for an HTTP response.
    """
    return generate_latest(), CONTENT_TYPE_LATEST
