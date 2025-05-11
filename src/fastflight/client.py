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
from fastflight.utils.stream_utils import AsyncToSyncConverter, write_arrow_data_to_stream

logger = logging.getLogger(__name__)

GLOBAL_CONVERTER = AsyncToSyncConverter()


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
        for _ in range(size):
            self.queue.put_nowait(flight.FlightClient(flight_server_location))
        self._converter = converter or GLOBAL_CONVERTER
        logger.info(f"Created FlightClientPool with {size} clients at {flight_server_location}")

    @asynccontextmanager
    async def acquire_async(self, timeout: Optional[float] = None) -> AsyncGenerator[flight.FlightClient, Any]:
        try:
            client = await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            raise RuntimeError("Timeout waiting for FlightClient from pool")

        try:
            yield client
        finally:
            await self.queue.put(client)

    @contextlib.contextmanager
    def acquire(self, timeout: Optional[float] = None) -> Generator[flight.FlightClient, Any, None]:
        try:
            client = self._converter.run_coroutine(asyncio.wait_for(self.queue.get(), timeout=timeout))
        except asyncio.TimeoutError:
            raise RuntimeError("Timeout waiting for FlightClient from pool")

        try:
            yield client
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
    A helper class to get data from the Flight server using a pool of `FlightClient`s.
    """

    def __init__(
        self,
        flight_server_location: str,
        registered_data_types: Dict[str, str] | None = None,
        client_pool_size: int = 5,
        converter: Optional[AsyncToSyncConverter] = None,
    ):
        """
        Initializes the FlightClient.

        Args:
            flight_server_location (str): The URI of the Flight server.
            registered_data_types (Dict[str, str] | None): A dictionary of registered data types.
            client_pool_size (int): The number of FlightClient instances to maintain in the pool.
            converter (Optional[AsyncToSyncConverter]): An optional converter to convert async to synchronous
        """
        self._converter = converter or GLOBAL_CONVERTER
        self._client_pool = FlightClientPool(flight_server_location, client_pool_size, converter=self._converter)
        self._registered_data_types = dict(registered_data_types or {})

    def get_registered_data_types(self) -> Dict[str, str]:
        return self._registered_data_types

    async def aget_stream_reader_with_callback(
        self, params: ParamsData, callback: Callable[[flight.FlightStreamReader], R], *, run_in_thread: bool = True
    ) -> R:
        """
        Retrieves a `FlightStreamReader` from the Flight server asynchronously and processes it with a callback.

        This method ensures that:
        - The data service for the given `data_type` is registered before making a request.
        - If the data type is not registered, it triggers a preflight request.
        - If a callback is provided, it processes the `FlightStreamReader` accordingly.

        Args:
            params (BaseParams): The params used to request data.
            callback (Callable[[flight.FlightStreamReader], R]): A function to process the stream.
            run_in_thread (bool): Whether to run the synchronous callback in a separate thread. Default is True, can be set to False for faster execution especially the callback is lightweight and non-blocking.

        Returns:
            R: The result of the callback function applied to the FlightStreamReader.

        Raises:
            RuntimeError: If the preflight request fails.
        """

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
            logger.error(f"Error fetching data: {e}", exc_info=True)
            raise

    async def aget_stream_reader(self, params: ParamsData) -> flight.FlightStreamReader:
        """
        Returns a `FlightStreamReader` from the Flight server using the provided flight ticket data asynchronously.

        Args:
            params: The params to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        return await self.aget_stream_reader_with_callback(params, callback=lambda x: x, run_in_thread=False)

    async def aget_pa_table(self, params: ParamsData) -> pa.Table:
        """
        Returns a pyarrow table from the Flight server using the provided flight ticket data asynchronously.

        Args:
            params: The params to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return await self.aget_stream_reader_with_callback(params, callback=lambda reader: reader.read_all())

    async def aget_pd_dataframe(self, params: ParamsData) -> pd.DataFrame:
        """
        Returns a pandas dataframe from the Flight server using the provided flight ticket data asynchronously.

        Args:
            params: The params to request data from the Flight server.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        return await self.aget_stream_reader_with_callback(
            params, callback=lambda reader: reader.read_all().to_pandas()
        )

    async def aget_stream(self, params: ParamsData) -> AsyncIterable[bytes]:
        """
        Generates a stream of bytes of arrow data from a Flight server ticket data asynchronously.

        Args:
            params: The params to request data from the Flight server.

        Yields:
            bytes: A stream of bytes from the Flight server.
        """
        reader = await self.aget_stream_reader(params)
        async for chunk in await write_arrow_data_to_stream(reader):
            yield chunk

    def get_stream_reader(self, params: ParamsData) -> flight.FlightStreamReader:
        """
        Returns a `FlightStreamReader` from the Flight server using the provided flight ticket data synchronously.
        This method ensures that the data service is registered via a preflight request before proceeding.

        Args:
            params: The params to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        # return self._converter.run_coroutine(self.aget_stream_reader(params))
        try:
            flight_ticket = to_flight_ticket(params)
            with self._client_pool.acquire() as client:
                return client.do_get(flight_ticket)
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise

    def get_pa_table(self, params: ParamsData) -> pa.Table:
        """
        Returns an Arrow Table from the Flight server using the provided flight ticket data synchronously.

        Args:
            params: The params to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return self.get_stream_reader(params).read_all()

    def get_pd_dataframe(self, params: ParamsData) -> pd.DataFrame:
        """
        Returns a pandas dataframe from the Flight server using the provided flight ticket data synchronously.

        Args:
            params: The params to request data from the Flight server.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        return self.get_stream_reader(params).read_all().to_pandas()

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
