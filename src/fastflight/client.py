import asyncio
import contextlib
import inspect
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterable, Callable, Dict, Generator, Optional, TypeVar, Union

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
from fastflight.utils.stream_utils import AsyncToSyncConverter, write_arrow_data_to_stream

logger = logging.getLogger(__name__)

GLOBAL_CONVERTER = AsyncToSyncConverter()


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
            f"Flight server unavailable during {operation_context}: {str(error)}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    elif isinstance(error, flight.FlightTimedOutError):
        return FastFlightTimeoutError(
            f"Operation timed out during {operation_context}: {str(error)}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    elif isinstance(error, flight.FlightInternalError):
        return FastFlightServerError(
            f"Server internal error during {operation_context}: {str(error)}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    elif isinstance(error, (ConnectionError, OSError)):
        return FastFlightConnectionError(
            f"Connection failed during {operation_context}: {str(error)}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    elif isinstance(error, TimeoutError):
        return FastFlightTimeoutError(
            f"Timeout occurred during {operation_context}: {str(error)}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )
    else:
        return FastFlightError(
            f"Unexpected error during {operation_context}: {str(error)}",
            details={"original_error": str(error), "error_type": type(error).__name__},
        )


class FlightClientPool:
    """
    Manages a pool of clients to connect to an Arrow Flight server.

    Attributes:
        flight_server_location (str): The URI of the Flight server.
        queue (asyncio.Queue): A queue to manage the FlightClient instances.
        _converter (AsyncToSyncConverter): An optional converter to convert async to synchronous
    """

    def __init__(
        self, flight_server_location: str, size: int = 5, converter: Optional[AsyncToSyncConverter] = None
    ) -> None:
        """
        Initializes the FlightClientPool with a specified number of FlightClient instances.

        Args:
            flight_server_location (str): The URI of the Flight server.
            size (int): The number of FlightClient instances to maintain in the pool.
            converter (Optional[AsyncToSyncConverter]): An optional converter to convert async to synchronous
        """
        self.flight_server_location = flight_server_location
        self.queue: asyncio.Queue[flight.FlightClient] = asyncio.Queue(maxsize=size)
        self.pool_size = size
        for _ in range(size):
            self.queue.put_nowait(flight.FlightClient(flight_server_location))
        self._converter = converter or GLOBAL_CONVERTER
        logger.info(f"Created FlightClientPool with {size} clients at {flight_server_location}")

    @asynccontextmanager
    async def acquire_async(self, timeout: Optional[float] = None) -> AsyncGenerator[flight.FlightClient, Any]:
        try:
            client = await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            raise FastFlightResourceExhaustionError(
                f"Timeout waiting for FlightClient from pool (pool size: {self.pool_size})",
                resource_type="connection_pool",
                details={"pool_size": self.pool_size, "timeout": timeout},
            )

        try:
            yield client
        except Exception as e:
            logger.error(f"Error during client operation: {e}", exc_info=True)
            raise
        finally:
            await self.queue.put(client)

    @contextlib.contextmanager
    def acquire(self, timeout: Optional[float] = None) -> Generator[flight.FlightClient, Any, None]:
        try:
            client = self._converter.run_coroutine(asyncio.wait_for(self.queue.get(), timeout=timeout))
        except asyncio.TimeoutError:
            raise FastFlightResourceExhaustionError(
                f"Timeout waiting for FlightClient from pool (pool size: {self.pool_size})",
                resource_type="connection_pool",
                details={"pool_size": self.pool_size, "timeout": timeout},
            )

        try:
            yield client
        except Exception as e:
            logger.error(f"Error during client operation: {e}", exc_info=True)
            raise
        finally:
            self.queue.put_nowait(client)

    async def close_async(self):
        while not self.queue.empty():
            client = await self.queue.get()
            try:
                await asyncio.to_thread(client.close)
            except Exception as e:
                logger.error("Error closing client: %s", e, exc_info=True)


R = TypeVar("R")

ParamsData = Union[bytes, BaseParams]


def to_flight_ticket(params: ParamsData) -> flight.Ticket:
    if isinstance(params, bytes):
        return flight.Ticket(params)
    return flight.Ticket(params.to_bytes())


class FastFlightClient:
    """
    A resilient helper class to get data from the Flight server using a pool of `FlightClient`s.

    This client includes comprehensive error handling with unified resilience configuration
    to ensure robust operation in production environments.
    """

    def __init__(
        self,
        flight_server_location: str,
        registered_data_types: Dict[str, str] | None = None,
        client_pool_size: int = 5,
        converter: Optional[AsyncToSyncConverter] = None,
        resilience_config: Optional[ResilienceConfig] = None,
    ):
        """
        Initializes the FastFlightClient with unified resilience configuration.

        Args:
            flight_server_location (str): The URI of the Flight server.
            registered_data_types (Dict[str, str] | None): A dictionary of registered data types.
            client_pool_size (int): The number of FlightClient instances to maintain in the pool.
            converter (Optional[AsyncToSyncConverter]): An optional converter to convert async to synchronous.
            resilience_config (Optional[ResilienceConfig]): Unified resilience configuration.
        """
        self._converter = converter or GLOBAL_CONVERTER
        self._client_pool = FlightClientPool(flight_server_location, client_pool_size, converter=self._converter)
        self._registered_data_types = dict(registered_data_types or {})
        self._flight_server_location = flight_server_location

        # Create default config with circuit breaker name set to server location
        default_config = resilience_config or ResilienceConfig.create_default()
        if default_config.circuit_breaker_name is None:
            default_config = default_config.with_circuit_breaker_name(f"flight_client_{flight_server_location}")

        self._resilience_manager = ResilienceManager(default_config)

        logger.info(f"Initialized FastFlightClient with unified resilience configuration for {flight_server_location}")

    def get_registered_data_types(self) -> Dict[str, str]:
        return self._registered_data_types

    def update_resilience_config(self, config: ResilienceConfig) -> None:
        """
        Update the resilience configuration for this client.

        Args:
            config: The new resilience configuration to use.
        """
        self._resilience_manager.update_default_config(config)

    def get_circuit_breaker_status(self) -> Dict[str, Any]:
        """
        Get the current status of the circuit breaker for this client.

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
        resilience_config: Optional[ResilienceConfig] = None,
    ) -> R:
        """
        Retrieves a `FlightStreamReader` from the Flight server asynchronously and processes it with a callback.

        This method includes comprehensive error handling, retry logic, and circuit breaker protection
        using unified resilience configuration.

        Args:
            params (BaseParams): The params used to request data.
            callback (Callable[[flight.FlightStreamReader], R]): A function to process the stream.
            run_in_thread (bool): Whether to run the synchronous callback in a separate thread.
            resilience_config (Optional[ResilienceConfig]): Override resilience configuration for this operation.

        Returns:
            R: The result of the callback function applied to the FlightStreamReader.

        Raises:
            FastFlightError: Various subclasses based on the type of error encountered.
        """

        async def _execute_operation():
            try:
                flight_ticket = to_flight_ticket(params)
                async with self._client_pool.acquire_async() as client:
                    reader = client.do_get(flight_ticket)
                    if inspect.iscoroutinefunction(callback):
                        return await callback(reader)
                    elif run_in_thread:
                        return await asyncio.to_thread(lambda: callback(reader))
                    else:
                        return callback(reader)
            except Exception as e:
                logger.error(f"Error fetching data from {self._flight_server_location}: {e}", exc_info=True)
                raise _handle_flight_error(e, "data retrieval")

        try:
            return await self._resilience_manager.execute_with_resilience(_execute_operation, config=resilience_config)
        except Exception as e:
            if isinstance(e, FastFlightError):
                raise
            raise _handle_flight_error(e, "stream reader retrieval with callback")

    async def aget_stream_reader(
        self, params: ParamsData, resilience_config: Optional[ResilienceConfig] = None
    ) -> flight.FlightStreamReader:
        """
        Returns a `FlightStreamReader` from the Flight server using the provided flight ticket data asynchronously.

        Args:
            params: The params to request data from the Flight server.
            resilience_config (Optional[ResilienceConfig]): Override resilience configuration for this operation.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.

        Raises:
            FastFlightError: Various subclasses based on the type of error encountered.
        """
        return await self.aget_stream_reader_with_callback(
            params, callback=lambda x: x, run_in_thread=False, resilience_config=resilience_config
        )

    async def aget_pa_table(self, params: ParamsData, resilience_config: Optional[ResilienceConfig] = None) -> pa.Table:
        """
        Returns a pyarrow table from the Flight server using the provided flight ticket data asynchronously.

        Args:
            params: The params to request data from the Flight server.
            resilience_config (Optional[ResilienceConfig]): Override resilience configuration for this operation.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.

        Raises:
            FastFlightError: Various subclasses based on the type of error encountered.
        """
        return await self.aget_stream_reader_with_callback(
            params, callback=lambda reader: reader.read_all(), resilience_config=resilience_config
        )

    async def aget_pd_dataframe(
        self, params: ParamsData, resilience_config: Optional[ResilienceConfig] = None
    ) -> pd.DataFrame:
        """
        Returns a pandas dataframe from the Flight server using the provided flight ticket data asynchronously.

        Args:
            params: The params to request data from the Flight server.
            resilience_config (Optional[ResilienceConfig]): Override resilience configuration for this operation.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.

        Raises:
            FastFlightError: Various subclasses based on the type of error encountered.
        """
        return await self.aget_stream_reader_with_callback(
            params, callback=lambda reader: reader.read_all().to_pandas(), resilience_config=resilience_config
        )

    async def aget_stream(
        self, params: ParamsData, resilience_config: Optional[ResilienceConfig] = None
    ) -> AsyncIterable[bytes]:
        """
        Generates a stream of bytes of arrow data from a Flight server ticket data asynchronously.

        Args:
            params: The params to request data from the Flight server.
            resilience_config (Optional[ResilienceConfig]): Override resilience configuration for this operation.

        Yields:
            bytes: A stream of bytes from the Flight server.

        Raises:
            FastFlightError: Various subclasses based on the type of error encountered.
        """
        reader = await self.aget_stream_reader(params, resilience_config=resilience_config)
        async for chunk in await write_arrow_data_to_stream(reader):
            yield chunk

    def get_stream_reader(
        self, params: ParamsData, resilience_config: Optional[ResilienceConfig] = None
    ) -> flight.FlightStreamReader:
        """
        Returns a `FlightStreamReader` from the Flight server using the provided flight ticket data synchronously.

        Args:
            params: The params to request data from the Flight server.
            resilience_config (Optional[ResilienceConfig]): Override resilience configuration for this operation.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.

        Raises:
            FastFlightError: Various subclasses based on the type of error encountered.
        """

        def _execute_operation():
            try:
                flight_ticket = to_flight_ticket(params)
                with self._client_pool.acquire() as client:
                    return client.do_get(flight_ticket)
            except Exception as e:
                logger.error(f"Error fetching data from {self._flight_server_location}: {e}", exc_info=True)
                raise _handle_flight_error(e, "synchronous data retrieval")

        try:
            # For synchronous operations, we use the converter to run async resilience patterns
            return self._converter.run_coroutine(
                self._resilience_manager.execute_with_resilience(_execute_operation, config=resilience_config)
            )
        except Exception as e:
            if isinstance(e, FastFlightError):
                raise
            raise _handle_flight_error(e, "synchronous stream reader retrieval")

    def get_pa_table(self, params: ParamsData, resilience_config: Optional[ResilienceConfig] = None) -> pa.Table:
        """
        Returns an Arrow Table from the Flight server using the provided flight ticket data synchronously.

        Args:
            params: The params to request data from the Flight server.
            resilience_config (Optional[ResilienceConfig]): Override resilience configuration for this operation.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.

        Raises:
            FastFlightError: Various subclasses based on the type of error encountered.
        """
        return self.get_stream_reader(params, resilience_config=resilience_config).read_all()

    def get_pd_dataframe(
        self, params: ParamsData, resilience_config: Optional[ResilienceConfig] = None
    ) -> pd.DataFrame:
        """
        Returns a pandas dataframe from the Flight server using the provided flight ticket data synchronously.

        Args:
            params: The params to request data from the Flight server.
            resilience_config (Optional[ResilienceConfig]): Override resilience configuration for this operation.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.

        Raises:
            FastFlightError: Various subclasses based on the type of error encountered.
        """
        return self.get_stream_reader(params, resilience_config=resilience_config).read_all().to_pandas()

    async def close_async(self) -> None:
        """
        Closes the client asynchronously.
        """
        await self._client_pool.close_async()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._converter.run_coroutine(self.close_async())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_async()
