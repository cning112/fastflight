import asyncio
import contextlib
import inspect
import logging
from collections.abc import AsyncGenerator, AsyncIterable, Callable, Generator
from contextlib import asynccontextmanager
from typing import Any, TypeVar

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from fastflight.core.base import BaseParams
from fastflight.exceptions import (
    FastFlightConnectionError,
    FastFlightError,
    FastFlightResourceExhaustionError,
    FastFlightServerError,
    FastFlightTimeoutError,
)
from fastflight.resilience import ResilienceConfig, ResilienceManager
from fastflight.utils.stream_utils import AsyncToSyncConverter, get_thread_local_converter, write_arrow_data_to_stream

logger = logging.getLogger(__name__)


def _handle_flight_error(error: Exception, operation_context: str) -> Exception:
    """
    Convert pyarrow.flight exceptions to FastFlight exception hierarchy.

    Args:
        error: The original exception from pyarrow.flight operations.
        operation_context: Description of the operation that failed.

    Returns:
        A FastFlight-specific exception with appropriate context.
    """
    if isinstance(error, flight.FlightUnavailableError):
        return FastFlightConnectionError(
            f"Flight server unavailable during {operation_context}: {error!s}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    elif isinstance(error, flight.FlightTimedOutError):
        return FastFlightTimeoutError(
            f"Operation timed out during {operation_context}: {error!s}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    elif isinstance(error, flight.FlightInternalError):
        return FastFlightServerError(
            f"Server internal error during {operation_context}: {error!s}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    elif isinstance(error, (ConnectionError, OSError)):
        return FastFlightConnectionError(
            f"Connection failed during {operation_context}: {error!s}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    elif isinstance(error, TimeoutError):
        return FastFlightTimeoutError(
            f"Timeout occurred during {operation_context}: {error!s}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    else:
        return FastFlightError(
            f"Unexpected error during {operation_context}: {error!s}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )


class _FlightClientPool:
    """
    Internal connection pool that manages raw Arrow Flight client connections.

    This pool provides connection reuse and resource management for the FastFlightBouncer.
    Users should not interact with this class directly - use FastFlightBouncer instead.

    Attributes:
        flight_server_location (str): The URI of the Flight server.
        queue (asyncio.Queue): Connection pool queue managing FlightClient instances.
        pool_size (int): Maximum number of concurrent connections in the pool.
    """

    def __init__(
        self, flight_server_location: str, size: int = 5, converter: AsyncToSyncConverter | None = None
    ) -> None:
        """
        Initialize the internal connection pool.

        Args:
            flight_server_location (str): The URI of the Flight server.
            size (int): The number of connections to maintain in the pool.
            converter (Optional[AsyncToSyncConverter]): Async-to-sync converter for compatibility.
        """
        self.flight_server_location = flight_server_location
        self.queue: asyncio.Queue[flight.FlightClient] = asyncio.Queue(maxsize=size)
        self.pool_size = size
        for _ in range(size):
            self.queue.put_nowait(flight.FlightClient(flight_server_location))
        self._converter = converter or get_thread_local_converter()
        logger.info(f"Created internal connection pool with {size} clients for {flight_server_location}")

    @asynccontextmanager
    async def acquire_async(self, timeout: float | None = None) -> AsyncGenerator[flight.FlightClient, Any]:
        """
        Acquire a connection from the pool asynchronously.

        Args:
            timeout: Maximum time to wait for an available connection.

        Yields:
            flight.FlightClient: A raw Flight client connection.

        Raises:
            FastFlightResourceExhaustionError: If no connection becomes available within timeout.
        """
        try:
            client = await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            raise FastFlightResourceExhaustionError(
                f"Connection pool exhausted - no connections available within {timeout}s (pool size: {self.pool_size})",
                resource_type="flight_connection_pool",
                details={"pool_size": self.pool_size, "timeout": timeout},
            ) from None

        try:
            yield client
        except Exception as e:
            logger.error(f"Error during client operation: {e}", exc_info=True)
            raise
        finally:
            await self.queue.put(client)

    @contextlib.contextmanager
    def acquire(self, timeout: float | None = None) -> Generator[flight.FlightClient, Any, None]:
        """
        Acquire a connection from the pool synchronously.

        Args:
            timeout: Maximum time to wait for an available connection.

        Yields:
            flight.FlightClient: A raw Flight client connection.

        Raises:
            FastFlightResourceExhaustionError: If no connection becomes available within timeout.
        """
        try:
            client = self._converter.run_coroutine(asyncio.wait_for(self.queue.get(), timeout=timeout))
        except asyncio.TimeoutError:
            raise FastFlightResourceExhaustionError(
                f"Connection pool exhausted - no connections available within {timeout}s (pool size: {self.pool_size})",
                resource_type="flight_connection_pool",
                details={"pool_size": self.pool_size, "timeout": timeout},
            ) from None

        try:
            yield client
        except Exception as e:
            logger.error(f"Error during client operation: {e}", exc_info=True)
            raise
        finally:
            self.queue.put_nowait(client)

    async def close_async(self):
        """Close all connections in the pool."""
        while not self.queue.empty():
            client = await self.queue.get()
            try:
                await asyncio.to_thread(client.close)
            except Exception as e:
                logger.error("Error closing client: %s", e, exc_info=True)


R = TypeVar("R")

ParamsData = bytes | BaseParams


def to_flight_ticket(params: ParamsData) -> flight.Ticket:
    if isinstance(params, bytes):
        return flight.Ticket(params)
    return flight.Ticket(params.to_bytes())


class FastFlightBouncer:
    """
    Intelligent Flight connection bouncer that manages pooled connections and request routing.

    FastFlightBouncer acts as a smart proxy between your application and Arrow Flight servers,
    providing connection pooling, load balancing, error handling, and resilience patterns.

    Like pgbouncer for PostgreSQL, it optimizes connection usage and provides transparent
    failover capabilities for production workloads.

    Key Features:
    - Connection pooling and reuse
    - Intelligent request routing
    - Circuit breaker protection
    - Automatic retry with backoff
    - Comprehensive error handling
    - Both sync and async interfaces

    Example:
        >>> bouncer = FastFlightBouncer("grpc://localhost:8815", client_pool_size=10)
        >>> df = await bouncer.aget_pd_dataframe(my_params)
        >>> # Or synchronously:
        >>> df = bouncer.get_pd_dataframe(my_params)
    """

    def __init__(
        self,
        flight_server_location: str,
        registered_data_types: dict[str, str] | None = None,
        client_pool_size: int = 5,
        converter: AsyncToSyncConverter | None = None,
        resilience_config: ResilienceConfig | None = None,
    ):
        """
        Initialize the Flight connection bouncer.

        Args:
            flight_server_location (str): Target Flight server URI (e.g., 'grpc://localhost:8815').
            registered_data_types (Dict[str, str] | None): Registry of available data service types.
            client_pool_size (int): Number of pooled connections to maintain. Defaults to 5.
            converter (Optional[AsyncToSyncConverter]): Async-to-sync converter for compatibility.
            resilience_config (Optional[ResilienceConfig]): Resilience patterns configuration
                (retry, circuit breaker, timeouts).
        """
        self._converter = converter or get_thread_local_converter()
        self._connection_pool = _FlightClientPool(flight_server_location, client_pool_size, converter=self._converter)
        self._registered_data_types = dict(registered_data_types or {})
        self._flight_server_location = flight_server_location

        resilience_config = resilience_config or ResilienceConfig.create_noop()
        if resilience_config.circuit_breaker_name is None:
            resilience_config = resilience_config.with_circuit_breaker_name(f"flight_bouncer_{flight_server_location}")
        self._resilience_manager = ResilienceManager(resilience_config)

        logger.info(f"Initialized FastFlightBouncer for {flight_server_location} with {client_pool_size} connections")

    def get_registered_data_types(self) -> dict[str, str]:
        """Get the registry of available data service types."""
        return self._registered_data_types

    def get_connection_pool_status(self) -> dict[str, Any]:
        """
        Get current status of the connection pool and bouncer.

        Returns:
            Dict containing pool size, available connections, and server location.
        """
        return {
            "server_location": self._flight_server_location,
            "pool_size": self._connection_pool.pool_size,
            "available_connections": self._connection_pool.queue.qsize(),
            "registered_services": len(self._registered_data_types),
        }

    def update_resilience_config(self, config: ResilienceConfig) -> None:
        """
        Update the resilience configuration for this bouncer.

        Args:
            config: The new resilience configuration to use.
        """
        self._resilience_manager.update_default_config(config)

    def get_circuit_breaker_status(self) -> dict[str, Any]:
        """
        Get the current status of the circuit breaker for this bouncer.

        Returns:
            A dictionary containing circuit breaker status information.
        """
        default_config = self._resilience_manager.default_config
        if not default_config.enable_circuit_breaker or not default_config.circuit_breaker_name:
            return {"enabled": False}

        circuit_name = default_config.circuit_breaker_name
        if circuit_name in self._resilience_manager.circuit_breakers:
            cb = self._resilience_manager.circuit_breakers[circuit_name]
            return {
                "enabled": True,
                "name": circuit_name,
                "state": cb.state.value,
                "failure_count": cb.failure_count,
                "success_count": cb.success_count,
                "last_failure_time": cb.last_failure_time,
            }
        else:
            return {"enabled": True, "initialized": False}

    async def aget_stream_reader_with_callback(
        self,
        params: ParamsData,
        callback: Callable[[flight.FlightStreamReader], R],
        *,
        run_in_thread: bool = True,
        resilience_config: ResilienceConfig | None = None,
    ) -> R:
        """
        Route a request through the connection bouncer and apply a callback to the stream.

        The bouncer acquires a connection from the pool, routes the request, and processes
        the response through the provided callback function.

        Args:
            params (ParamsData): Flight request parameters or raw bytes.
            callback (Callable[[flight.FlightStreamReader], R]): Function to process the stream reader.
            run_in_thread (bool): Whether to execute synchronous callbacks in a thread pool.
            resilience_config (Optional[ResilienceConfig]): Override default resilience settings.

        Returns:
            R: Result of applying the callback to the stream reader.

        Raises:
            FastFlightError: Connection, timeout, or server errors with detailed context.
        """

        async def _bounce_request():
            """Internal request bouncing logic."""
            try:
                flight_ticket = to_flight_ticket(params)
                async with self._connection_pool.acquire_async() as client:
                    reader = client.do_get(flight_ticket)
                    if inspect.iscoroutinefunction(callback):
                        return await callback(reader)
                    elif run_in_thread:
                        return await asyncio.to_thread(lambda: callback(reader))
                    else:
                        return callback(reader)
            except Exception as e:
                logger.error(f"Request bouncing failed for {self._flight_server_location}: {e}", exc_info=True)
                raise _handle_flight_error(e, "request bouncing") from e

        try:
            return await self._resilience_manager.execute_with_resilience(_bounce_request, config=resilience_config)
        except Exception as e:
            if isinstance(e, FastFlightError):
                raise
            raise _handle_flight_error(e, "bouncer request processing") from e

    async def aget_stream_reader(
        self, params: ParamsData, resilience_config: ResilienceConfig | None = None
    ) -> flight.FlightStreamReader:
        """
        Bounce a request to get a Flight stream reader.

        Args:
            params: Flight request parameters or raw ticket bytes.
            resilience_config: Override default resilience settings for this request.

        Returns:
            flight.FlightStreamReader: Stream reader from the Flight server.
        """
        return await self.aget_stream_reader_with_callback(
            params, callback=lambda x: x, run_in_thread=False, resilience_config=resilience_config
        )

    async def aget_pa_table(self, params: ParamsData, resilience_config: ResilienceConfig | None = None) -> pa.Table:
        """
        Bounce a request to get a PyArrow table asynchronously.

        Args:
            params: Flight request parameters or raw ticket bytes.
            resilience_config: Override default resilience settings for this request.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return await self.aget_stream_reader_with_callback(
            params, callback=lambda reader: reader.read_all(), resilience_config=resilience_config
        )

    async def aget_pd_dataframe(
        self, params: ParamsData, resilience_config: ResilienceConfig | None = None
    ) -> pd.DataFrame:
        """
        Bounce a request to get a pandas DataFrame asynchronously.

        Args:
            params: Flight request parameters or raw ticket bytes.
            resilience_config: Override default resilience settings for this request.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        return await self.aget_stream_reader_with_callback(
            params, callback=lambda reader: reader.read_all().to_pandas(), resilience_config=resilience_config
        )

    async def aget_stream(
        self, params: ParamsData, resilience_config: ResilienceConfig | None = None
    ) -> AsyncIterable[bytes]:
        """
        Bounce a request to generate a stream of Arrow data bytes asynchronously.

        Args:
            params: Flight request parameters or raw ticket bytes.
            resilience_config: Override default resilience settings for this request.

        Yields:
            bytes: A stream of bytes from the Flight server.
        """
        reader = await self.aget_stream_reader(params, resilience_config=resilience_config)
        async for chunk in await write_arrow_data_to_stream(reader):
            yield chunk

    def get_pa_table(self, params: ParamsData, resilience_config: ResilienceConfig | None = None) -> pa.Table:
        """
        Synchronously bounce a request to get an Arrow Table.

        Args:
            params: Flight request parameters or raw ticket bytes.
            resilience_config: Override default resilience settings for this request.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return self._converter.run_coroutine(self.aget_pa_table(params, resilience_config=resilience_config))

    def get_pd_dataframe(self, params: ParamsData, resilience_config: ResilienceConfig | None = None) -> pd.DataFrame:
        """
        Synchronously bounce a request to get a pandas DataFrame.

        Args:
            params: Flight request parameters or raw ticket bytes.
            resilience_config: Override default resilience settings for this request.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        return self._converter.run_coroutine(self.aget_pd_dataframe(params, resilience_config=resilience_config))

    async def close_async(self) -> None:
        """
        Shutdown the bouncer and close all pooled connections.
        """
        await self._connection_pool.close_async()
        logger.info(f"FastFlightBouncer closed for {self._flight_server_location}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, _exc_val, _exc_tb):
        self._converter.run_coroutine(self.close_async())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, _exc_val, _exc_tb):
        await self.close_async()
