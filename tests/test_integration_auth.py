import pytest
from fastapi.testclient import TestClient
import threading
import time
import os
from unittest import mock
import pyarrow as pa

# Configuration and Server/App components
from src.fastflight.config import (
    FlightServerSettings,
    FastAPISettings,
    flight_server_settings as global_flight_server_settings_for_integration, # alias to avoid clash
    fastapi_settings as global_fastapi_settings_for_integration # alias
)
from src.fastflight.server import FastFlightServer, main as flight_server_main_runner
from src.fastflight.fastapi.app import create_app
from src.fastflight.security import ServerAuthHandler
from src.fastflight.fastapi.security import API_KEY_NAME
from src.fastflight.core.base import BaseParams, BaseDataService # For type hints and cleanup

# Try to import test service from test_flight_auth. If not found, define inline.
try:
    from .test_flight_auth import PingParams, PingService
except ImportError:
    # Redefine if import fails (e.g. if tests are run in a way that doesn't allow relative import)
    class PingParams(BaseParams): # type: ignore
        pass

    @BaseDataService._register(PingParams)
    class PingService(BaseDataService[PingParams]): # type: ignore
        def get_batches(self, params: PingParams, batch_size: int | None = None) -> pa.RecordBatchReader:
            data = [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])]
            schema = pa.schema([("col1", pa.int64()), ("col2", pa.string())])
            batch = pa.record_batch(data, schema=schema)
            return pa.RecordBatchReader.from_batches(schema, [batch])
        async def aget_batches(self, params: PingParams, batch_size: int | None = None):
            reader = self.get_batches(params, batch_size)
            for batch in reader:
                yield batch

# Test specific configurations
INTEGRATION_FLIGHT_HOST = "127.0.0.1"
INTEGRATION_FLIGHT_PORT = 8891 # Different from test_flight_auth
INTEGRATION_FLIGHT_TOKEN = "integration-flight-token-secure"

INTEGRATION_API_HOST = "127.0.0.1" # TestClient doesn't use this directly
INTEGRATION_API_PORT = 8001 # Different from other tests
INTEGRATION_API_KEY = "integration-api-key-valid"

# Module paths for FastAPI app creation
INTEGRATION_MODULE_PATHS = ["src.fastflight.demo_services", "tests.test_integration_auth"] # Add path to PingService if defined here

@pytest.fixture(scope="module")
def running_flight_server_for_integration():
    original_settings = {
        "auth_token": global_flight_server_settings_for_integration.auth_token,
        "host": global_flight_server_settings_for_integration.host,
        "port": global_flight_server_settings_for_integration.port,
    }
    global_flight_server_settings_for_integration.auth_token = INTEGRATION_FLIGHT_TOKEN
    global_flight_server_settings_for_integration.host = INTEGRATION_FLIGHT_HOST
    global_flight_server_settings_for_integration.port = INTEGRATION_FLIGHT_PORT
    
    # Ensure PingService is registered if it was redefined locally
    # This is only needed if the import failed and PingService was redefined in this file.
    # If PingService is in demo_services or imported correctly, this might not be needed or should be conditional.
    # For safety, if PingService is in this module's scope:
    if 'PingService' in globals() and PingParams.fqn() not in BaseDataService._registry:
         BaseDataService._register(PingParams, PingService) # type: ignore

    server_thread = threading.Thread(target=flight_server_main_runner, daemon=True)
    server_thread.start()
    time.sleep(1.5) # Give server time to start, increase if flaky

    # Basic check if server is up
    try:
        # Flight server requires auth, so this connect attempt might fail if it tries to do anything,
        # but it should at least resolve the port.
        # A client with auth handler would be better for a real ping.
        fl_client = pa.flight.connect(f"grpc://{INTEGRATION_FLIGHT_HOST}:{INTEGRATION_FLIGHT_PORT}", timeout=1)
        # Try a list_flights or similar simple, non-data call if server expects auth immediately for all ops
        # For now, just connecting is a basic check.
        fl_client.close()
    except Exception as e:
        # If connection itself fails, server is likely not up. Auth errors are secondary.
        if "Connection refused" in str(e) or "Deadline Exceeded" in str(e):
             pytest.fail(f"Integration Flight server did not start: {e}")
        print(f"Integration Flight server check connection got error (may be expected auth issue): {e}")

    yield f"grpc://{INTEGRATION_FLIGHT_HOST}:{INTEGRATION_FLIGHT_PORT}"
    
    # Restore original settings
    for key, value in original_settings.items():
        setattr(global_flight_server_settings_for_integration, key, value)
    # Note: Proper server shutdown for threaded server is complex. Relies on daemon thread.


@pytest.fixture(scope="module")
def integration_test_client(running_flight_server_for_integration):
    flight_server_loc = running_flight_server_for_integration

    original_api_settings = {
        "valid_api_keys": global_fastapi_settings_for_integration.valid_api_keys,
        "flight_server_location": global_fastapi_settings_for_integration.flight_server_location,
        # Store other relevant FastAPI settings if they are changed
    }
    global_fastapi_settings_for_integration.valid_api_keys = [INTEGRATION_API_KEY]
    global_fastapi_settings_for_integration.flight_server_location = flight_server_loc
    
    # Important: The FastFlightBouncer created by the FastAPI app's lifespan
    # needs to pick up the `auth_token` for the Flight server.
    # The FastFlightBouncer constructor takes `auth_token`.
    # The lifespan `fast_flight_client_lifespan` needs to be aware of this.
    # It currently doesn't pass `auth_token` to FastFlightBouncer.
    # This requires a modification to `fast_flight_client_lifespan` or how bouncer gets its token.
    # For this test, we can mock `FastFlightBouncer` or patch the lifespan,
    # or assume `FastAPISettings` could also include `flight_client_auth_token`.
    
    # Let's assume FastAPISettings can provide the token for the bouncer.
    # Add a temporary setting for the test:
    setattr(global_fastapi_settings_for_integration, 'flight_client_auth_token', INTEGRATION_FLIGHT_TOKEN)

    # Patch lifespan to use this token. This is a bit intrusive for a test.
    # A cleaner way would be for FastFlightBouncer to accept settings object or lifespan to read from config.
    # For now, let's assume `fast_flight_client_lifespan` is modified or bouncer configured correctly.
    # The current `fast_flight_client_lifespan` does not pass `auth_token` to `FastFlightBouncer`.
    # This test WILL FAIL unless the bouncer used by the app sends the token.
    # I will proceed assuming this gap needs to be fixed in `lifespan.py` or bouncer init.
    # For the purpose of this test structure, I'll mock the bouncer's token usage within the app context.
    # This is complex. A simpler path:
    # Modify `src/fastflight/fastapi/lifespan.py` so that `FastFlightBouncer` gets `auth_token`
    # from `fastapi_settings.flight_server_auth_token` (a new field to be added there).
    # This is out of scope for just writing tests.
    
    # Workaround for the test: We need the bouncer used by TestClient to be authenticated.
    # The `fast_flight_client_lifespan` creates the bouncer.
    # We can't easily modify that bouncer post-creation by TestClient.
    # The most robust way without app code change is to ensure settings are picked up:
    # 1. `FlightServerSettings.auth_token` is set (for the server).
    # 2. `FastAPISettings` needs a way to tell its bouncer to use a token.
    #    Let's add a new setting `flight_client_auth_token` to `FastAPISettings`.
    #    And modify `fast_flight_client_lifespan` to use it.
    #    (This change to `lifespan.py` is outside this current subtask of just writing tests,
    #    so this test might highlight that need).

    # Assuming `FastFlightBouncer` inside `create_app`'s lifespan is configured with `INTEGRATION_FLIGHT_TOKEN`.
    # This requires `fastapi_settings.flight_server_location` to be set, and potentially a new
    # `fastapi_settings.flight_server_auth_token` to be used by the lifespan to init the bouncer.
    # Let's assume `FastFlightBouncer` will be enhanced to pick up a client token from a new setting
    # like `fastapi_settings.flight_server_token_for_bouncer`.

    # For the test to pass with current code, we'd need `FastFlightBouncer` to be initialized
    # by the lifespan with `auth_token=INTEGRATION_FLIGHT_TOKEN`.
    # Let's mock the `FastFlightBouncer` initialization within the app's context for this test,
    # or more simply, ensure the global `flight_server_settings.auth_token` is what the bouncer might pick up if it defaulted to it.
    # This is messy. The cleanest is that `fast_flight_client_lifespan` should instantiate bouncer with a token if configured.
    
    # Given the constraints, I will write the test assuming the FastAPI app's bouncer is correctly
    # configured with `INTEGRATION_FLIGHT_TOKEN`. The success of this test will implicitly depend
    # on this setup being possible (e.g. via a new setting in FastAPISettings that the lifespan uses).

    app = create_app(
        module_paths=list(INTEGRATION_MODULE_PATHS),
        # Lifespan will use global_fastapi_settings_for_integration for flight_server_location
        # and needs to be aware of INTEGRATION_FLIGHT_TOKEN for its bouncer.
    )
    client = TestClient(app)
    
    yield client
    
    # Restore original settings
    for key, value in original_api_settings.items():
        setattr(global_fastapi_settings_for_integration, key, value)
    if hasattr(global_fastapi_settings_for_integration, 'flight_client_auth_token'):
        delattr(global_fastapi_settings_for_integration, 'flight_client_auth_token')

    # Clean up registries for PingService if it was defined locally
    if 'PingService' in globals():
        if PingParams.fqn() in BaseParams.registry:
            del BaseParams.registry[PingParams.fqn()]
        if PingParams.fqn() in BaseDataService._registry:
            del BaseDataService._registry[PingParams.fqn()]


def test_integration_e2e_authenticated_stream(integration_test_client: TestClient):
    # This test assumes that the FastAPI application's FastFlightBouncer is configured
    # to use INTEGRATION_FLIGHT_TOKEN when communicating with the Flight server.
    # This configuration would typically happen in the FastAPI app's lifespan,
    # where FastFlightBouncer is initialized.
    
    # Prepare request body for PingService
    ping_request_body = PingParams().model_dump_json() # Get JSON string
    
    # Make request to FastAPI's /fastflight/stream endpoint
    response = integration_test_client.post(
        "/fastflight/stream",
        content=ping_request_body, # Send JSON string as content
        headers={
            API_KEY_NAME: INTEGRATION_API_KEY,
            "Content-Type": "application/json" # Ensure correct content type
        }
    )
    
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
    
    # Process the Arrow stream response
    try:
        reader = pa.ipc.RecordBatchStreamReader(response.content)
        table = reader.read_all()
        assert table is not None
        assert len(table) == 3
        assert table.column_names == ["col1", "col2"]
        # Verify some data if necessary
        assert table.column("col1").to_pylist() == [1, 2, 3]
        assert table.column("col2").to_pylist() == ["a", "b", "c"]
    except Exception as e:
        pytest.fail(f"Error reading Arrow stream from response: {e}\nResponse content: {response.content[:500]}")

# Note: The success of this integration test heavily depends on the FastAPI application's
# lifespan correctly initializing its FastFlightBouncer instance with the
# `INTEGRATION_FLIGHT_TOKEN`. If `src.fastflight.fastapi.lifespan.fast_flight_client_lifespan`
# does not have a mechanism to pass `auth_token` to `FastFlightBouncer`, this test will fail
# at the stage where FastAPI tries to talk to the Flight server, likely resulting in an
# Unauthenticated error from the Flight server, which would then translate to an HTTP error
# (e.g., 500) from FastAPI.
#
# A potential way to handle this without modifying library code for the test would be to
# globally patch `FastFlightBouncer.__init__` to always use a specific token for this test run,
# but that's highly invasive.
# The best approach is ensuring the application code (`lifespan.py`) can configure the bouncer's auth.
# For this test, we assume such a mechanism exists or will be added.
# e.g. `FastAPISettings` could have `flight_server_client_token` used by lifespan.
