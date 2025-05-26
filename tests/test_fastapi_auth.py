import pytest
from fastapi.testclient import TestClient
from unittest import mock
import os

# Assuming src.fastflight.fastapi.app.create_app is the entry point
# and src.fastflight.config.fastapi_settings is the global settings instance used by the app
from src.fastflight.fastapi.app import create_app
from src.fastflight.config import fastapi_settings as global_fastapi_settings
from src.fastflight.fastapi.security import API_KEY_NAME # To use the correct header name

# Minimal list of module paths for testing, assuming demo_services has what's needed
# or that the specific endpoints tested don't rely on deep service discovery.
TEST_MODULE_PATHS = ["src.fastflight.demo_services"] 
TEST_API_KEY_VALID = "test-api-key-valid"
TEST_API_KEY_INVALID = "test-api-key-invalid"

@pytest.fixture(scope="module")
def client_with_api_key_auth():
    # Temporarily modify global fastapi_settings for the test client
    original_valid_keys = global_fastapi_settings.valid_api_keys
    global_fastapi_settings.valid_api_keys = [TEST_API_KEY_VALID]

    # Create a TestClient for the FastAPI app
    # The create_app function should ideally be able to take settings overrides,
    # or use the globally patched one.
    app = create_app(module_paths=list(TEST_MODULE_PATHS))
    client = TestClient(app)
    
    yield client
    
    # Restore original settings
    global_fastapi_settings.valid_api_keys = original_valid_keys


@pytest.fixture(scope="module")
def client_with_no_api_keys_configured():
    # Test scenario where server has no API keys configured (auth effectively disabled by policy)
    original_valid_keys = global_fastapi_settings.valid_api_keys
    global_fastapi_settings.valid_api_keys = [] # No keys configured

    app = create_app(module_paths=list(TEST_MODULE_PATHS))
    client = TestClient(app)
    
    yield client
    
    global_fastapi_settings.valid_api_keys = original_valid_keys


# Test endpoints (assuming these exist and are protected by API key)
# From previous subtasks, /fastflight/registered_data_types and /fastflight/stream are protected.
# /fastflight/health is not.

PROTECTED_ENDPOINTS_GET = ["/fastflight/registered_data_types"]
# For POST, we'd need a valid body, e.g. for /fastflight/stream
# Let's focus on GET for simplicity of auth header testing.

@pytest.mark.parametrize("endpoint_path", PROTECTED_ENDPOINTS_GET)
def test_fastapi_auth_failure_no_key_header(client_with_api_key_auth: TestClient, endpoint_path: str):
    response = client_with_api_key_auth.get(endpoint_path)
    # Expect 401 if auto_error=False and no key, or 403 if auto_error=True (FastAPI default)
    # Our get_api_key raises 401 if header is missing and keys are configured.
    assert response.status_code == 401 
    assert "Not authenticated" in response.json()["detail"]

@pytest.mark.parametrize("endpoint_path", PROTECTED_ENDPOINTS_GET)
def test_fastapi_auth_failure_invalid_key(client_with_api_key_auth: TestClient, endpoint_path: str):
    response = client_with_api_key_auth.get(endpoint_path, headers={API_KEY_NAME: TEST_API_KEY_INVALID})
    assert response.status_code == 403
    assert "Invalid API Key" in response.json()["detail"]

@pytest.mark.parametrize("endpoint_path", PROTECTED_ENDPOINTS_GET)
def test_fastapi_auth_success_valid_key(client_with_api_key_auth: TestClient, endpoint_path: str):
    response = client_with_api_key_auth.get(endpoint_path, headers={API_KEY_NAME: TEST_API_KEY_VALID})
    assert response.status_code == 200 # Assuming this endpoint returns 200 on success

@pytest.mark.parametrize("endpoint_path", PROTECTED_ENDPOINTS_GET)
def test_fastapi_auth_effectively_disabled_if_no_keys_configured(client_with_no_api_keys_configured: TestClient, endpoint_path: str):
    # In get_api_key, if fastapi_settings.valid_api_keys is empty, it returns None (allowing access).
    response = client_with_no_api_keys_configured.get(endpoint_path)
    assert response.status_code == 200

    response_with_random_key = client_with_no_api_keys_configured.get(endpoint_path, headers={API_KEY_NAME: "random-key-should-still-pass"})
    assert response_with_random_key.status_code == 200

# Test for the /fastflight/health endpoint (should not require API key)
def test_fastapi_health_endpoint_no_auth_needed(client_with_api_key_auth: TestClient):
    response = client_with_api_key_auth.get("/fastflight/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_fastapi_health_endpoint_no_auth_needed_when_keys_not_set(client_with_no_api_keys_configured: TestClient):
    response = client_with_no_api_keys_configured.get("/fastflight/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

# TODO: Test for /fastflight/stream (POST endpoint) would require a valid body.
# Example structure for POST test:
# def test_fastapi_auth_success_stream_post(client_with_api_key_auth: TestClient):
#     # This requires a valid BaseParams serialized body that the demo_services can handle
#     # and potentially a running Flight server if the test client makes real calls through bouncer.
#     # If bouncer is mocked or demo service doesn't need live Flight, it's simpler.
#     # For now, focusing on header-based auth for GET.
#     pass
