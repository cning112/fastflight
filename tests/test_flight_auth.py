import pytest
import pyarrow as pa
import pyarrow.flight as fl
import threading
import time
import os
from unittest import mock

from src.fastflight.server import FastFlightServer
from src.fastflight.client import FastFlightBouncer
from src.fastflight.security import ServerAuthHandler
from src.fastflight.config import FlightServerSettings, flight_server_settings as global_flight_server_settings
from src.fastflight.core.base import BaseDataService, BaseParams, DataServiceCls, ParamsCls

# Minimal Data Service for testing
class PingParams(BaseParams):
    pass

@BaseDataService._register(PingParams) # Manually register for the test
class PingService(BaseDataService[PingParams]):
    def get_batches(self, params: PingParams, batch_size: int | None = None) -> pa.RecordBatchReader:
        data = [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])]
        schema = pa.schema([("col1", pa.int64()), ("col2", pa.string())])
        batch = pa.record_batch(data, schema=schema)
        return pa.RecordBatchReader.from_batches(schema, [batch])

    async def aget_batches(self, params: PingParams, batch_size: int | None = None):
        # For simplicity, make async version call sync version via converter if needed,
        # or just implement directly for this test.
        reader = self.get_batches(params, batch_size)
        for batch in reader:
            yield batch


TEST_HOST = "127.0.0.1"
TEST_PORT = 8890 # Use a different port for tests
TEST_TOKEN = "test-auth-token-123"

@pytest.fixture(scope="module")
def flight_server_with_auth():
    # Patch global settings for the duration of this server fixture
    # This is tricky because config module might be already loaded.
    # A better way would be to pass settings to server constructor if it supported it,
    # or ensure server reads settings on demand.
    # For now, we modify the global flight_server_settings instance used by server.main or start_instance
    
    original_token = global_flight_server_settings.auth_token
    original_host = global_flight_server_settings.host
    original_port = global_flight_server_settings.port

    global_flight_server_settings.auth_token = TEST_TOKEN
    global_flight_server_settings.host = TEST_HOST
    global_flight_server_settings.port = TEST_PORT
    
    # The server's main() or start_instance() will pick up these patched settings
    # when it creates ServerAuthHandler and ServerTLSInfo
    
    # We need to run the server in a separate thread/process because server.serve() is blocking.
    # Using server.main which calls start_instance.
    from src.fastflight.server import main as flight_server_main_actual
    
    server_thread = threading.Thread(target=flight_server_main_actual, daemon=True)
    server_thread.start()
    
    # Wait for server to start - simplistic approach
    time.sleep(1.0) # Give server a moment to start
    
    # Check if server is up - more robust would be a client ping
    try:
        # Try a quick connection without auth to see if port is open (might fail on auth, that's fine)
        client = fl.connect(f"grpc://{TEST_HOST}:{TEST_PORT}", timeout=1)
        client.close()
    except Exception as e:
        # If it's an auth error, server is up. If connection refused, it's not.
        if "Connection refused" in str(e) or "Deadline Exceeded" in str(e): # Deadline Exceeded for timeout
             pytest.fail(f"Flight server did not start on {TEST_HOST}:{TEST_PORT}: {e}")
        # Other errors (like auth error) might be expected if server is up but requires auth immediately
        print(f"Flight server startup check got client connection error (potentially expected): {e}")


    yield f"grpc://{TEST_HOST}:{TEST_PORT}" # Provide the location
    
    # Teardown: Stop the server
    # PyArrow FlightServerBase needs shutdown to be called.
    # This is tricky as server_thread runs `server.main()`.
    # `server.main()` itself would need to handle signals to call server.shutdown().
    # For testing, directly finding and shutting down the server instance is hard.
    # Sending a signal to the thread is not straightforward.
    # Since it's a daemon thread, it will exit when the main test process exits.
    # This is usually acceptable for tests but not for production shutdown.
    # For more graceful shutdown in tests, server would need an explicit stop method or signal handling.
    # Reset global settings
    global_flight_server_settings.auth_token = original_token
    global_flight_server_settings.host = original_host
    global_flight_server_settings.port = original_port
    # Note: Proper server shutdown in a test thread is complex.
    # For now, rely on daemon thread + process exit. If tests hang, this needs improvement.
    # A common pattern is `server.shutdown()` if `server` object was accessible.
    # Or `server_process.terminate()` if it was a `multiprocessing.Process`.


def test_flight_auth_failure_no_token(flight_server_with_auth):
    location = flight_server_with_auth
    # Bouncer without auth_token
    bouncer = FastFlightBouncer(flight_server_location=location)
    params = PingParams()
    
    with pytest.raises(fl.FlightUnauthenticatedError) as exc_info:
        bouncer.get_pa_table(params)
    
    assert "No token provided" in str(exc_info.value) or "Invalid token" in str(exc_info.value)
    # The exact message depends on ServerAuthHandler logic with empty header
    # ServerAuthHandler.authenticate raises "No token provided." if incoming.read() is empty.

    bouncer.close_async_context_manager_sync_only() # Close bouncer's internal converter


def test_flight_auth_failure_incorrect_token(flight_server_with_auth):
    location = flight_server_with_auth
    bouncer = FastFlightBouncer(flight_server_location=location, auth_token="incorrect-token-value")
    params = PingParams()
    
    with pytest.raises(fl.FlightUnauthenticatedError) as exc_info:
        bouncer.get_pa_table(params)
        
    assert "Invalid token" in str(exc_info.value)
    bouncer.close_async_context_manager_sync_only()


def test_flight_auth_success(flight_server_with_auth):
    location = flight_server_with_auth
    bouncer = FastFlightBouncer(flight_server_location=location, auth_token=TEST_TOKEN)
    params = PingParams()
    
    try:
        table = bouncer.get_pa_table(params)
        assert table is not None
        assert len(table) == 3
        assert table.column_names == ["col1", "col2"]
    finally:
        bouncer.close_async_context_manager_sync_only()

# Add a helper to FastFlightBouncer for tests if not running full async tests
# This is a temporary measure for synchronous test cleanup.
def close_bouncer_sync(bouncer: FastFlightBouncer):
    """Synchronously closes the bouncer's async converter resources."""
    if hasattr(bouncer, '_converter') and bouncer._converter:
        # If the converter has a loop and thread running for async operations
        if hasattr(bouncer._converter, 'loop') and bouncer._converter.loop.is_running():
            bouncer._converter.close()

# Monkey-patch this method onto the class for testing purposes
FastFlightBouncer.close_async_context_manager_sync_only = close_bouncer_sync

# Cleanup for BaseParams and BaseDataService registries if tests are run multiple times in one session
# or if other tests also modify these global registries.
@pytest.fixture(autouse=True, scope="session")
def cleanup_registries():
    yield
    # Clear specific test entries after all tests in the session
    if PingParams.fqn() in BaseParams.registry:
        del BaseParams.registry[PingParams.fqn()]
    if PingParams.fqn() in BaseDataService._registry: # Accessing protected for test cleanup
        del BaseDataService._registry[PingParams.fqn()]
