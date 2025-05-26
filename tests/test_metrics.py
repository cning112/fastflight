import pytest
from fastapi.testclient import TestClient
import time

from src.fastflight.fastapi.app import create_app
from src.fastflight.config import fastapi_settings as global_fastapi_settings

# Minimal list of module paths for testing
TEST_MODULE_PATHS = ["src.fastflight.demo_services"]

@pytest.fixture(scope="module")
def client_with_metrics_enabled():
    original_metrics_enabled = global_fastapi_settings.metrics_enabled
    global_fastapi_settings.metrics_enabled = True # Ensure metrics are on

    app = create_app(module_paths=list(TEST_MODULE_PATHS))
    client = TestClient(app)
    
    yield client
    
    global_fastapi_settings.metrics_enabled = original_metrics_enabled

@pytest.fixture(scope="module")
def client_with_metrics_disabled():
    original_metrics_enabled = global_fastapi_settings.metrics_enabled
    global_fastapi_settings.metrics_enabled = False # Ensure metrics are off

    app = create_app(module_paths=list(TEST_MODULE_PATHS))
    client = TestClient(app)
    
    yield client
    
    global_fastapi_settings.metrics_enabled = original_metrics_enabled


def test_metrics_endpoint_available_when_enabled(client_with_metrics_enabled: TestClient):
    response = client_with_metrics_enabled.get("/metrics")
    assert response.status_code == 200
    assert "prometheus_client" in response.text or "starlette_requests_total" in response.text # Check for typical metric content

def test_metrics_endpoint_not_available_when_disabled(client_with_metrics_disabled: TestClient):
    response = client_with_metrics_disabled.get("/metrics")
    # Expect 404 if the route is not added when metrics are disabled
    assert response.status_code == 404 

def test_basic_fastapi_request_metric_increment(client_with_metrics_enabled: TestClient):
    # This test is a bit more involved and can be flaky if other activities affect metrics.
    # We target a known metric from starlette-prometheus: starlette_requests_total

    # Helper to parse Prometheus text format (simplified)
    def get_metric_value(metrics_text: str, metric_name: str, labels: dict = None):
        for line in metrics_text.splitlines():
            if line.startswith("#") or not line.strip():
                continue
            name_part = line.split(" ")[0]
            value_part = line.split(" ")[1]
            
            current_metric_name = name_part.split("{")[0] if "{" in name_part else name_part
            
            if current_metric_name == metric_name:
                if labels:
                    label_match = True
                    for k, v in labels.items():
                        if f'{k}="{v}"' not in name_part:
                            label_match = False
                            break
                    if label_match:
                        return float(value_part)
                else: # No labels to match, return first one found (use with caution)
                    return float(value_part)
        return None

    # Get initial metrics
    metrics_before_response = client_with_metrics_enabled.get("/metrics")
    assert metrics_before_response.status_code == 200
    
    # Define labels for the health endpoint request we are about to make
    # Note: starlette-prometheus might use slightly different label names or include more.
    # Common labels: method, path, status_code (after request).
    # For just counting requests to a path before status_code is known, it might be simpler.
    # starlette_requests_total usually has method and path.
    
    health_path_for_metrics = "/fastflight/health" # The actual path, not the full URL

    # Make a request to the health endpoint (or any other simple GET endpoint)
    # This specific request will be for the /fastflight/health endpoint.
    # starlette-prometheus automatically adds a trailing slash to path if not root.
    # However, the actual path registered in FastAPI is what matters for the label.
    # Our health endpoint is "/fastflight/health"
    client_with_metrics_enabled.get(health_path_for_metrics) 

    # Get metrics again
    metrics_after_response = client_with_metrics_enabled.get("/metrics")
    assert metrics_after_response.status_code == 200

    # Check if the counter for the health endpoint (GET requests) has incremented
    # The label for path in starlette-prometheus might be specific, e.g. including the prefix.
    # Let's try to find the metric for the health endpoint.
    # The path label used by starlette-prometheus needs to be precise.
    # It might be '/fastflight/health' or similar.
    
    # Try to get the value before
    # The labels for starlette_requests_total are method, path, and status_code (for completed requests)
    # If we are checking for a request that just happened, its metric will include its status code.
    labels_for_health_check = {"method": "GET", "path": health_path_for_metrics, "status_code": "200"}

    value_before = get_metric_value(metrics_before_response.text, "starlette_requests_total", labels_for_health_check) or 0.0
    value_after = get_metric_value(metrics_after_response.text, "starlette_requests_total", labels_for_health_check)

    assert value_after is not None, f"Metric starlette_requests_total with labels {labels_for_health_check} not found after request."
    assert value_after > value_before, \
        f"Metric starlette_requests_total for {health_path_for_metrics} did not increment. Before: {value_before}, After: {value_after}"

    # Also test a bouncer metric if possible, but this requires setting up a bouncer and client calls.
    # For now, focusing on starlette-prometheus auto-metrics.
    # e.g. bouncer_pool_size should be present if a bouncer was initialized by lifespan.
    # This depends on whether create_app's lifespan initializes a bouncer.
    # FastFlightBouncer.__init__ sets bouncer_pool_size.set(effective_pool_size)
    # So, if the lifespan in create_app runs, this metric should exist.
    pool_size_metric_name = "bouncer_pool_size" # As defined in metrics.py
    pool_size_value = get_metric_value(metrics_after_response.text, pool_size_metric_name)
    
    # The bouncer is initialized in the lifespan of the app.
    # The default pool size is 10 from BouncerSettings.
    assert pool_size_value is not None, f"Metric {pool_size_metric_name} not found."
    assert pool_size_value == 10, f"Expected {pool_size_metric_name} to be 10, got {pool_size_value}"
