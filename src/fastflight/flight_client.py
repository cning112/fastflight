import asyncio
import contextlib
import inspect
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterable, Callable, Generator, Optional, TypeVar, Union

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from fastflight.data_service_base import BaseParams
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


TicketType = Union[bytes, BaseParams, flight.Ticket]

R = TypeVar("R")


def to_flight_ticket(ticket: TicketType) -> flight.Ticket:
    if isinstance(ticket, BaseParams):
        return flight.Ticket(ticket.to_bytes())

    if isinstance(ticket, bytes):
        return flight.Ticket(ticket)

    if isinstance(ticket, flight.Ticket):
        return ticket

    raise ValueError(f"Invalid ticket type: {type(ticket)}")


class FlightClientManager:
    """
    A helper class to get data from flight server using a pool of `FlightClient`s.
    """

    def __init__(self, flight_server_location: str, client_pool_size: int = 5):
        """
        Initializes the FlightClientHelper.

        Args:
            flight_server_location (str): The URI of the Flight server.
            client_pool_size (int): The number of FlightClient instances to maintain in the pool.
        """
        self._client_pool = FlightClientPool(flight_server_location, client_pool_size)
        self._converter = AsyncToSyncConverter()

    async def aget_stream_reader_with_callback(
        self, ticket: TicketType, callback: Callable[[flight.FlightStreamReader], R]
    ) -> R:
        try:
            flight_ticket = to_flight_ticket(ticket)
            async with self._client_pool.acquire_async() as client:
                # client.do_get is a synchronous call, so we call it directly.
                reader = client.do_get(flight_ticket)
                if inspect.iscoroutinefunction(callback):
                    # If the callback is an async function, call it directly.
                    result = await callback(reader)
                else:
                    # Otherwise, run the callback in a background thread.
                    result = await asyncio.to_thread(lambda: callback(reader))

                # If the result is awaitable (e.g. a coroutine), await it.
                if asyncio.iscoroutine(result):
                    return await result
                else:
                    return result
        except Exception as e:
            logger.error(f"Error fetching data: {e}", exc_info=True)
            raise

    async def aget_stream_reader(self, ticket: TicketType) -> flight.FlightStreamReader:
        """
        Gets a reader to a stream of Arrow data in bytes format using the provided flight ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        return await self.aget_stream_reader_with_callback(ticket, callback=lambda x: x)

    async def aread_pa_table(self, ticket: TicketType) -> pa.Table:
        """
        Returns a pyarrow table from the Flight server using the provided flight ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return await self.aget_stream_reader_with_callback(ticket, callback=lambda reader: reader.read_all())

    async def aread_pd_dataframe(self, ticket: TicketType) -> pd.DataFrame:
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

    async def aget_stream(self, ticket: TicketType) -> AsyncIterable[bytes]:
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

    def get_stream_reader(self, ticket: TicketType) -> flight.FlightStreamReader:
        """
        A synchronous version of :meth:`FlightClientHelper.aget_stream_reader`.

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

    def read_pa_table(self, ticket: TicketType) -> pa.Table:
        """
        A synchronous version of :meth:`FlightClientHelper.aread_pa_table`.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return self.get_stream_reader(ticket).read_all()

    def read_pd_dataframe(self, ticket: TicketType) -> pd.DataFrame:
        """
        A synchronous version of :meth:`FlightClientHelper.aread_pd_df`.

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
