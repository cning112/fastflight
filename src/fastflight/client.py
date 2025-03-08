import asyncio
import contextlib
import inspect
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterable, Callable, Dict, Generator, Optional, Type, TypeVar, Union

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from fastflight.data_services import BaseParams
from fastflight.utils.stream_utils import AsyncToSyncConverter, write_arrow_data_to_stream

logger = logging.getLogger(__name__)


class FlightClientPool:
    """
    Manages a pool of clients to connect to an Arrow Flight server.

    Attributes:
        flight_server_location (str): The URI of the Flight server.
        queue (asyncio.Queue): A queue to manage the FlightClient instances.
    """

    def __init__(self, flight_server_location: str, size: int = 5) -> None:
        """
        Initializes the FlightClientPool with a specified number of FlightClient instances.

        Args:
            flight_server_location (str): The URI of the Flight server.
            size (int): The number of FlightClient instances to maintain in the pool.
        """
        self.flight_server_location = flight_server_location
        self.queue: asyncio.Queue[flight.FlightClient] = asyncio.Queue(maxsize=size)
        for _ in range(size):
            self.queue.put_nowait(flight.FlightClient(flight_server_location))
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
            client = asyncio.run(asyncio.wait_for(self.queue.get(), timeout=timeout))
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

TicketData = Union[bytes, BaseParams]


def to_flight_ticket(ticket_data: TicketData) -> flight.Ticket:
    if isinstance(ticket_data, bytes):
        return flight.Ticket(ticket_data)

    params_cls = ticket_data.__class__
    service_cls = ticket_data.default_service_class()
    ticket = {
        **ticket_data.to_json(),
        "_service_cls": f"{service_cls.__module__}.{service_cls.__name__}",
        "_params_cls": f"{params_cls.__module__}.{params_cls.__name__}",
    }
    return flight.Ticket(json.dumps(ticket).encode("utf-8"))


class FastFlightClient:
    """
    A helper class to get data from the Flight server using a pool of `FlightClient`s.
    """

    def __init__(
        self,
        flight_server_location: str,
        registered_data_types: Dict[str, Type[BaseParams]] | None = None,
        client_pool_size: int = 5,
    ):
        """
        Initializes the FlightClient.

        Args:
            flight_server_location (str): The URI of the Flight server.
            registered_data_types (Dict[str, Type[BaseParams]] | None): A dictionary of registered data types.
            client_pool_size (int): The number of FlightClient instances to maintain in the pool.
        """
        self._client_pool = FlightClientPool(flight_server_location, client_pool_size)
        self._converter = AsyncToSyncConverter()
        self._registered_data_types = dict(registered_data_types or {})

    def get_data_types(self) -> Dict[str, Type[BaseParams]]:
        return dict(self._registered_data_types)

    async def aget_stream_reader_with_callback(
        self, ticket: TicketData, callback: Callable[[flight.FlightStreamReader], R]
    ) -> R:
        """
        Retrieves a `FlightStreamReader` from the Flight server asynchronously and processes it with a callback.

        This method ensures that:
        - The data service for the given `data_type` is registered before making a request.
        - If the data type is not registered, it triggers a preflight request.
        - If a callback is provided, it processes the `FlightStreamReader` accordingly.

        Args:
            ticket (BaseParams): The params used to request data.
            callback (Callable[[flight.FlightStreamReader], R]): A function to process the stream.

        Returns:
            R: The result of the callback function applied to the FlightStreamReader.

        Raises:
            RuntimeError: If the preflight request fails.
        """

        try:
            flight_ticket = to_flight_ticket(ticket)
            async with self._client_pool.acquire_async() as client:
                reader = client.do_get(flight_ticket)
                if inspect.iscoroutinefunction(callback):
                    return await callback(reader)
                else:
                    return await asyncio.to_thread(lambda: callback(reader))

        except Exception as e:
            logger.error(f"Error fetching data: {e}", exc_info=True)
            raise

    async def aget_stream_reader(self, ticket: TicketData) -> flight.FlightStreamReader:
        """
        Returns a `FlightStreamReader` from the Flight server using the provided flight ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        return await self.aget_stream_reader_with_callback(ticket, callback=lambda x: x)

    async def aget_pa_table(self, ticket: TicketData) -> pa.Table:
        """
        Returns a pyarrow table from the Flight server using the provided flight ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return await self.aget_stream_reader_with_callback(ticket, callback=lambda reader: reader.read_all())

    async def aget_pd_dataframe(self, ticket: TicketData) -> pd.DataFrame:
        """
        Returns a pandas dataframe from the Flight server using the provided flight ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        return await self.aget_stream_reader_with_callback(
            ticket, callback=lambda reader: reader.read_all().to_pandas()
        )

    async def aget_stream(self, ticket: TicketData) -> AsyncIterable[bytes]:
        """
        Generates a stream of bytes of arrow data from a Flight server ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Yields:
            bytes: A stream of bytes from the Flight server.
        """
        reader = await self.aget_stream_reader(ticket)
        async for chunk in await write_arrow_data_to_stream(reader):
            yield chunk

    def get_stream_reader(self, ticket: TicketData) -> flight.FlightStreamReader:
        """
        Returns a `FlightStreamReader` from the Flight server using the provided flight ticket data synchronously.
        This method ensures that the data service is registered via a preflight request before proceeding.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        # return self._converter.run_coroutine(self.aget_stream_reader(ticket))
        try:
            flight_ticket = to_flight_ticket(ticket)
            with self._client_pool.acquire() as client:
                return client.do_get(flight_ticket)
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise

    def get_pa_table(self, ticket: TicketData) -> pa.Table:
        """
        Returns an Arrow Table from the Flight server using the provided flight ticket data synchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return self.get_stream_reader(ticket).read_all()

    def get_pd_dataframe(self, ticket: TicketData) -> pd.DataFrame:
        """
        Returns a pandas dataframe from the Flight server using the provided flight ticket data synchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        return self.get_stream_reader(ticket).read_all().to_pandas()

    async def close_async(self) -> None:
        """
        Closes the client asynchronously.
        """
        await self._client_pool.close_async()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.close_async())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_async()
