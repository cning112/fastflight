import os
import pytest
from unittest import mock

# Before importing the config module, ensure any pre-existing relevant env vars are cleared
# or handled if they could interfere with tests. This is tricky globally.
# We'll rely on mock.patch.dict to set them specifically for each test.

from src.fastflight.config import (
    LoggingSettings,
    FlightServerSettings,
    FastAPISettings,
    BouncerSettings
)

@pytest.fixture(autouse=True)
def clear_env_vars():
    """Clear relevant environment variables before each test and restore after."""
    env_vars_to_manage = [
        "FASTFLIGHT_LOGGING_LOG_LEVEL", "FASTFLIGHT_LOGGING_LOG_FORMAT",
        "FASTFLIGHT_SERVER_HOST", "FASTFLIGHT_SERVER_PORT", "FASTFLIGHT_SERVER_LOG_LEVEL",
        "FASTFLIGHT_SERVER_AUTH_TOKEN", "FASTFLIGHT_SERVER_TLS_SERVER_CERT_PATH", "FASTFLIGHT_SERVER_TLS_SERVER_KEY_PATH",
        "FASTFLIGHT_API_HOST", "FASTFLIGHT_API_PORT", "FASTFLIGHT_API_LOG_LEVEL",
        "FASTFLIGHT_API_FLIGHT_SERVER_LOCATION", "FASTFLIGHT_API_VALID_API_KEYS",
        "FASTFLIGHT_API_SSL_KEYFILE", "FASTFLIGHT_API_SSL_CERTFILE", "FASTFLIGHT_API_METRICS_ENABLED",
        "FASTFLIGHT_BOUNCER_POOL_SIZE"
    ]
    original_values = {var: os.environ.get(var) for var in env_vars_to_manage}
    
    # Clear them for the test
    for var in env_vars_to_manage:
        if var in os.environ:
            del os.environ[var]
            
    yield # Test runs here

    # Restore original values
    for var, original_value in original_values.items():
        if original_value is not None:
            os.environ[var] = original_value
        elif var in os.environ: # If it was set during test but not originally
            del os.environ[var]


def test_logging_settings_defaults():
    settings = LoggingSettings()
    assert settings.log_level == "INFO"
    assert settings.log_format == "plain"

def test_logging_settings_from_env():
    with mock.patch.dict(os.environ, {
        "FASTFLIGHT_LOGGING_LOG_LEVEL": "DEBUG",
        "FASTFLIGHT_LOGGING_LOG_FORMAT": "json"
    }):
        settings = LoggingSettings()
        assert settings.log_level == "DEBUG"
        assert settings.log_format == "json"

def test_flight_server_settings_defaults():
    settings = FlightServerSettings()
    assert settings.host == "0.0.0.0"
    assert settings.port == 8815
    assert settings.log_level == "INFO"
    assert settings.auth_token is None
    assert settings.tls_server_cert_path is None
    assert settings.tls_server_key_path is None

def test_flight_server_settings_from_env():
    with mock.patch.dict(os.environ, {
        "FASTFLIGHT_SERVER_HOST": "127.0.0.1",
        "FASTFLIGHT_SERVER_PORT": "9000",
        "FASTFLIGHT_SERVER_LOG_LEVEL": "WARNING",
        "FASTFLIGHT_SERVER_AUTH_TOKEN": "test_token",
        "FASTFLIGHT_SERVER_TLS_SERVER_CERT_PATH": "/path/to/cert.pem",
        "FASTFLIGHT_SERVER_TLS_SERVER_KEY_PATH": "/path/to/key.pem"
    }):
        settings = FlightServerSettings()
        assert settings.host == "127.0.0.1"
        assert settings.port == 9000
        assert settings.log_level == "WARNING"
        assert settings.auth_token == "test_token"
        assert settings.tls_server_cert_path == "/path/to/cert.pem"
        assert settings.tls_server_key_path == "/path/to/key.pem"

def test_fastapi_settings_defaults():
    settings = FastAPISettings()
    assert settings.host == "0.0.0.0"
    assert settings.port == 8000
    assert settings.log_level == "INFO"
    assert settings.flight_server_location == "grpc://localhost:8815"
    assert settings.valid_api_keys == []
    assert settings.ssl_keyfile is None
    assert settings.ssl_certfile is None
    assert settings.metrics_enabled is True

def test_fastapi_settings_from_env():
    with mock.patch.dict(os.environ, {
        "FASTFLIGHT_API_HOST": "127.0.0.2",
        "FASTFLIGHT_API_PORT": "8001",
        "FASTFLIGHT_API_LOG_LEVEL": "CRITICAL",
        "FASTFLIGHT_API_FLIGHT_SERVER_LOCATION": "grpc://otherhost:1234",
        "FASTFLIGHT_API_VALID_API_KEYS": "key1,key2, key3", # Test with spaces
        "FASTFLIGHT_API_SSL_KEYFILE": "/ssl/key.pem",
        "FASTFLIGHT_API_SSL_CERTFILE": "/ssl/cert.pem",
        "FASTFLIGHT_API_METRICS_ENABLED": "false" # Test boolean parsing
    }):
        settings = FastAPISettings()
        assert settings.host == "127.0.0.2"
        assert settings.port == 8001
        assert settings.log_level == "CRITICAL"
        assert settings.flight_server_location == "grpc://otherhost:1234"
        assert settings.valid_api_keys == ["key1", "key2", "key3"]
        assert settings.ssl_keyfile == "/ssl/key.pem"
        assert settings.ssl_certfile == "/ssl/cert.pem"
        assert settings.metrics_enabled is False # Pydantic automatically converts "false"

def test_fastapi_settings_empty_api_keys_from_env():
     with mock.patch.dict(os.environ, {"FASTFLIGHT_API_VALID_API_KEYS": ""}):
        settings = FastAPISettings()
        # Pydantic v2 by default might convert "" to [''] for List[str] if not handled.
        # However, if the default is [], and the field is Optional or has a default_factory,
        # it might result in []. Let's verify Pydantic's behavior for comma-separated strings.
        # For pydantic_settings and comma-separated lists, an empty string usually results in an empty list
        # if the list items are simple strings. If it becomes `['']`, the test needs adjustment or the model needs refinement.
        # Based on typical pydantic-settings behavior for `list[str]`, an empty string for the env var
        # should result in an empty list, not `['']`.
        assert settings.valid_api_keys == []

def test_bouncer_settings_defaults():
    settings = BouncerSettings()
    assert settings.pool_size == 10

def test_bouncer_settings_from_env():
    with mock.patch.dict(os.environ, {"FASTFLIGHT_BOUNCER_POOL_SIZE": "20"}):
        settings = BouncerSettings()
        assert settings.pool_size == 20
