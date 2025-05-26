import pytest
from fastapi.testclient import TestClient

from src.fastflight.fastapi.app import create_app
from src.fastflight.config import fastapi_settings as global_fastapi_settings

# Minimal list of module paths for testing
TEST_MODULE_PATHS = ["src.fastflight.demo_services"]

@pytest.fixture(scope="module")
def client():
    # We can use any configuration of FastAPI settings for health check,
    # as it's not dependent on API keys or metrics enabled status.
    # Using default settings for simplicity.
    app = create_app(module_paths=list(TEST_MODULE_PATHS))
    test_client = TestClient(app)
    yield test_client

def test_health_endpoint(client: TestClient):
    """
    Tests the /fastflight/health endpoint.
    It should return 200 OK and a specific JSON body.
    """
    response = client.get("/fastflight/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_health_endpoint_trailing_slash(client: TestClient):
    """
    Tests the /fastflight/health/ endpoint with a trailing slash.
    FastAPI typically redirects this if the route is defined without a trailing slash.
    """
    response = client.get("/fastflight/health/")
    # Default FastAPI behavior is to redirect a trailing slash URL to the non-slash version if only non-slash is defined.
    # This might result in a 200 if the redirect is followed by TestClient, or a 30x if not.
    # For a simple health check, often only the non-slash version is explicitly tested,
    # but it's good to be aware of FastAPI's behavior.
    # TestClient follows redirects by default.
    assert response.status_code == 200 
    assert response.json() == {"status": "healthy"}
    # If strict no-redirect is desired, one might configure the router differently or TestClient(app, follow_redirects=False)
    # and assert a 307/308, but for health checks, 200 is the main goal.
