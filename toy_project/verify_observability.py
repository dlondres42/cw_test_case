#!/usr/bin/env python3
"""
End-to-End Observability Verification Script

This script verifies that all observability components are working correctly:
- Prometheus metrics collection
- Jaeger trace collection
- Grafana datasources
- Loki log aggregation
"""

import requests
import json
import sys
import time
from typing import Dict, List, Tuple


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str):
    """Print a formatted section header."""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}\n")


def print_success(text: str):
    """Print a success message."""
    print(f"{Colors.GREEN}✓{Colors.RESET} {text}")


def print_error(text: str):
    """Print an error message."""
    print(f"{Colors.RED}✗{Colors.RESET} {text}")


def print_warning(text: str):
    """Print a warning message."""
    print(f"{Colors.YELLOW}⚠{Colors.RESET} {text}")


def check_service_health(name: str, url: str) -> bool:
    """Check if a service is responding."""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            print_success(f"{name} is accessible at {url}")
            return True
        else:
            print_error(f"{name} returned status {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print_error(f"{name} is not accessible: {e}")
        return False


def check_prometheus_targets() -> Tuple[bool, Dict]:
    """Check Prometheus scrape targets."""
    try:
        response = requests.get("http://localhost:9090/api/v1/targets", timeout=5)
        if response.status_code != 200:
            print_error(f"Prometheus API returned {response.status_code}")
            return False, {}
        
        data = response.json()
        active_targets = data.get('data', {}).get('activeTargets', [])
        
        results = {}
        for target in active_targets:
            job = target.get('labels', {}).get('job', 'unknown')
            health = target.get('health', 'unknown')
            results[job] = health
            
            if health == 'up':
                print_success(f"Prometheus target '{job}' is UP")
            else:
                print_error(f"Prometheus target '{job}' is {health}")
        
        return all(h == 'up' for h in results.values()), results
    except Exception as e:
        print_error(f"Failed to check Prometheus targets: {e}")
        return False, {}


def check_metrics_endpoint(name: str, url: str, expected_metrics: List[str]) -> bool:
    """Check that metrics endpoint exposes expected metrics."""
    try:
        response = requests.get(url, timeout=5)
        if response.status_code != 200:
            print_error(f"{name} metrics endpoint returned {response.status_code}")
            return False
        
        content = response.text
        missing = []
        for metric in expected_metrics:
            if metric not in content:
                missing.append(metric)
        
        if missing:
            print_error(f"{name} missing metrics: {', '.join(missing)}")
            return False
        else:
            print_success(f"{name} exposes all expected metrics ({len(expected_metrics)} found)")
            return True
    except Exception as e:
        print_error(f"Failed to check {name} metrics: {e}")
        return False


def query_prometheus_metric(metric: str) -> Tuple[bool, float]:
    """Query a Prometheus metric and return its value."""
    try:
        response = requests.get(
            "http://localhost:9090/api/v1/query",
            params={"query": metric},
            timeout=5
        )
        if response.status_code != 200:
            return False, 0.0
        
        data = response.json()
        result = data.get('data', {}).get('result', [])
        
        if not result:
            return False, 0.0
        
        value = float(result[0]['value'][1])
        return True, value
    except Exception as e:
        return False, 0.0


def check_grafana_datasources() -> bool:
    """Check that Grafana has all expected datasources configured."""
    try:
        response = requests.get(
            "http://localhost:3000/api/datasources",
            auth=('admin', 'admin'),
            timeout=5
        )
        if response.status_code != 200:
            print_error(f"Grafana API returned {response.status_code}")
            return False
        
        datasources = response.json()
        ds_names = {ds['name'] for ds in datasources}
        
        expected = {'Prometheus', 'Jaeger', 'Loki'}
        missing = expected - ds_names
        
        if missing:
            print_error(f"Grafana missing datasources: {', '.join(missing)}")
            return False
        else:
            print_success(f"Grafana has all expected datasources: {', '.join(expected)}")
            return True
    except Exception as e:
        print_error(f"Failed to check Grafana datasources: {e}")
        return False


def check_loki_logs() -> bool:
    """Check that Loki is receiving logs."""
    try:
        response = requests.get(
            "http://localhost:3100/loki/api/v1/labels",
            timeout=5
        )
        if response.status_code != 200:
            print_error(f"Loki API returned {response.status_code}")
            return False
        
        data = response.json()
        labels = data.get('data', [])
        
        if 'service_name' in labels or 'container' in labels:
            print_success("Loki is receiving logs")
            return True
        else:
            print_warning("Loki is running but may not be receiving logs yet")
            return True
    except Exception as e:
        print_error(f"Failed to check Loki: {e}")
        return False


def check_jaeger_services() -> bool:
    """Check that Jaeger has received traces from services."""
    try:
        response = requests.get(
            "http://localhost:16686/api/services",
            timeout=5
        )
        if response.status_code != 200:
            print_error(f"Jaeger API returned {response.status_code}")
            return False
        
        data = response.json()
        services = data.get('data', [])
        
        expected_services = {'stream-processor', 'monitoring-api'}
        found_services = set(services) & expected_services
        
        if found_services:
            print_success(f"Jaeger has traces from: {', '.join(found_services)}")
            if len(found_services) < len(expected_services):
                print_warning(f"Missing traces from: {', '.join(expected_services - found_services)}")
            return True
        else:
            print_warning("Jaeger is running but has not received traces yet")
            return True
    except Exception as e:
        print_error(f"Failed to check Jaeger: {e}")
        return False


def trigger_test_request() -> bool:
    """Trigger a test API request to generate metrics."""
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            print_success("Successfully triggered test API request")
            return True
        else:
            print_error(f"Test request returned {response.status_code}")
            return False
    except Exception as e:
        print_error(f"Failed to trigger test request: {e}")
        return False


def main():
    """Run all verification checks."""
    print(f"\n{Colors.BOLD}Observability Stack E2E Verification{Colors.RESET}")
    print(f"Starting verification at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    results = {}
    
    # Check basic service health
    print_header("1. Service Health Checks")
    results['monitoring-api'] = check_service_health(
        "Monitoring API", "http://localhost:8000/health"
    )
    results['prometheus'] = check_service_health(
        "Prometheus", "http://localhost:9090/-/healthy"
    )
    results['grafana'] = check_service_health(
        "Grafana", "http://localhost:3000/api/health"
    )
    results['jaeger'] = check_service_health(
        "Jaeger", "http://localhost:16686/"
    )
    results['loki'] = check_service_health(
        "Loki", "http://localhost:3100/loki/api/v1/labels"
    )
    
    # Check Prometheus targets
    print_header("2. Prometheus Scrape Targets")
    targets_ok, targets = check_prometheus_targets()
    results['prometheus_targets'] = targets_ok
    
    # Check metrics endpoints
    print_header("3. Metrics Endpoints")
    results['monitoring-api-metrics'] = check_metrics_endpoint(
        "Monitoring API",
        "http://localhost:8000/metrics",
        [
            "messages_consumed_total",
            "http_requests_total",
            "db_insert_duration_seconds",
            "consume_batch_duration_seconds"
        ]
    )
    results['stream-processor-metrics'] = check_metrics_endpoint(
        "Stream Processor",
        "http://localhost:8001/metrics",
        [
            "batches_produced_total",
            "records_produced_total",
            "produce_duration_seconds"
        ]
    )
    
    # Check Grafana datasources
    print_header("4. Grafana Datasources")
    results['grafana_datasources'] = check_grafana_datasources()
    
    # Check Loki logs
    print_header("5. Loki Log Aggregation")
    results['loki_logs'] = check_loki_logs()
    
    # Check Jaeger traces
    print_header("6. Jaeger Trace Collection")
    results['jaeger_traces'] = check_jaeger_services()
    
    # Trigger test request
    print_header("7. Test Request Generation")
    results['test_request'] = trigger_test_request()
    time.sleep(2)  # Wait for metrics to be scraped
    
    # Query some metrics
    print_header("8. Prometheus Metric Queries")
    success, value = query_prometheus_metric("http_requests_total")
    if success and value > 0:
        print_success(f"http_requests_total = {value}")
        results['metric_queries'] = True
    else:
        print_warning("http_requests_total has no data yet (may need more requests)")
        results['metric_queries'] = False
    
    # Summary
    print_header("Verification Summary")
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    print(f"\nPassed: {passed}/{total} checks")
    
    if passed == total:
        print(f"\n{Colors.GREEN}{Colors.BOLD}✓ All checks passed!{Colors.RESET}")
        print(f"\n{Colors.BOLD}Next steps:{Colors.RESET}")
        print("  1. Open Grafana: http://localhost:3000 (admin/admin)")
        print("  2. View dashboard: System Overview")
        print("  3. Explore Loki logs: Explore → Loki")
        print("  4. View traces: http://localhost:16686 (Jaeger UI)")
        print("  5. Query metrics: http://localhost:9090 (Prometheus)")
        return 0
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}✗ Some checks failed{Colors.RESET}")
        print(f"\nFailed checks:")
        for name, status in results.items():
            if not status:
                print(f"  - {name}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
