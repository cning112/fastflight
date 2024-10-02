import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterable, Union

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from fastflight.data_service_base import BaseParams
from fastflight.utils.stream_utils import AsyncToSyncConverter, stream_arrow_data

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
    async def acquire_async(self):
        """
        Acquires a FlightClient instance from the pool.

        Yields:
            flight.FlightClient: An instance of FlightClient from the pool.
        """
        client = await self.queue.get()
        try:
            yield client
        finally:
            await self.queue.put(client)

    async def close_async(self):
        while not self.queue.empty():
            client = await self.queue.get()
            client.close()


TicketType = Union[bytes, BaseParams, flight.Ticket]


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

    async def aget_stream_reader(self, ticket: TicketType) -> flight.FlightStreamReader:
        """
        Gets a reader to a stream of Arrow data in bytes format using the provided flight ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        async with self._client_pool.acquire_async() as client:
            flight_ticket = to_flight_ticket(ticket)
            try:
                reader = await asyncio.to_thread(client.do_get, flight_ticket)
                return reader
            except Exception as e:
                logger.error(f"Error fetching data: {e}")
                raise

    async def aget_stream(self, ticket: TicketType) -> AsyncIterable[bytes]:
        """
        Generates a stream of bytes of arrow data from a Flight server ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Yields:
            bytes: A stream of bytes from the Flight server.
        """
        reader = await self.aget_stream_reader(ticket)
        return await stream_arrow_data(reader)

    async def aread_pa_table(self, ticket: TicketType) -> pa.Table:
        """
        Returns a pyarrow table from the Flight server using the provided flight ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        reader = await self.aget_stream_reader(ticket)
        return reader.read_all()

    async def aread_pd_df(self, ticket: TicketType) -> pd.DataFrame:
        """
        Returns a pandas dataframe from the Flight server using the provided flight ticket data asynchronously.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        table = await self.aread_pa_table(ticket)
        return table.to_pandas()

    def get_stream_reader(self, ticket: TicketType) -> flight.FlightStreamReader:
        """
        A synchronous version of :meth:`FlightClientHelper.aget_stream_reader`.

        Args:
            ticket: The ticket data to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        return self._converter.run_coroutine(self.aget_stream_reader(ticket))

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
