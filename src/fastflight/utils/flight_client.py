import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import AsyncIterable

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

logger = logging.getLogger(__name__)


class ClientPool:
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


class PooledClient:
    """
    A helper class to get data from flight server using a pool of `FlightClient`s.

    Attributes:
        client_pool (ClientPool): The pool of FlightClient instances.
        executor (concurrent.futures.ThreadPoolExecutor | None): The executor to run blocking calls in a separate thread.
        loop (asyncio.AbstractEventLoop): The current event loop.
    """

    def __init__(
        self, flight_server_location: str, client_pool_size: int = 5, executor: ThreadPoolExecutor | None = None
    ):
        """
        Initializes the FlightClientHelper.

        Args:
            flight_server_location (str): The URI of the Flight server.
            client_pool_size (int): The number of FlightClient instances to maintain in the pool.
            executor (ThreadPoolExecutor | None): An optional executor to use in event loops. If not provided, will use the default executor.
        """
        self.client_pool = ClientPool(flight_server_location, client_pool_size)
        self.loop = asyncio.get_running_loop()
        self.executor = executor

    async def aget_stream_reader(self, ticket_bytes: bytes) -> flight.FlightStreamReader:
        """
        Gets a reader to a stream of Arrow data in bytes format using the provided flight ticket data asynchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        async with self.client_pool.acquire_async() as client:
            flight_ticket = flight.Ticket(ticket_bytes)
            try:
                reader = await self.loop.run_in_executor(self.executor, client.do_get, flight_ticket)
                return reader
            except Exception as e:
                logger.error(f"Error fetching data: {e}")
                raise

    async def aget_bytes_stream(self, ticket_bytes: bytes) -> AsyncIterable[bytes]:
        """
        Generates a stream of bytes of arrow data from a Flight server ticket data asynchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Yields:
            bytes: A stream of bytes from the Flight server.
        """
        reader = await self.aget_stream_reader(ticket_bytes)
        async for data in aread_stream_bytes(reader):
            yield data

    async def aread_pd_df(self, ticket_bytes: bytes) -> pd.DataFrame:
        """
        Returns a pandas dataframe from the Flight server using the provided flight ticket data asynchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        reader = await self.aget_stream_reader(ticket_bytes)
        batches = [await self.loop.run_in_executor(self.executor, batch.data.to_pandas) for batch in reader]
        return pd.concat(batches, ignore_index=True)

    async def aread_pa_table(self, ticket_bytes: bytes) -> pa.Table:
        """
        Returns a pyarrow table from the Flight server using the provided flight ticket data asynchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        reader = await self.aget_stream_reader(ticket_bytes)
        batches = [await self.loop.run_in_executor(self.executor, batch.data) for batch in reader]
        return pa.Table.from_batches(batches)

    def get_stream_reader(self, ticket_bytes: bytes) -> flight.FlightStreamReader:
        """
        A synchronous version of :meth:`FlightClientHelper.aget_stream_reader`.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        return asyncio.run(self.aget_stream_reader(ticket_bytes))

    def read_pd_df(self, ticket_bytes: bytes) -> pd.DataFrame:
        """
        A synchronous version of :meth:`FlightClientHelper.aread_pd_df`.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        return asyncio.run(self.aread_pd_df(ticket_bytes))

    def read_pd_table(self, ticket_bytes: bytes) -> pa.Table:
        """
        A synchronous version of :meth:`FlightClientHelper.aread_pa_table`.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return asyncio.run(self.aread_pa_table(ticket_bytes))

    async def close_async(self, close_executor: bool = True) -> None:
        """
        Closes the client asynchronously.

        Args:
            close_executor (bool, optional): Whether to close the executor if it is not None. Defaults to True.

        Returns:
            None
        """
        await self.client_pool.close_async()
        if close_executor and self.executor:
            self.executor.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.close_async())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_async()


async def aread_stream_bytes(reader: flight.FlightStreamReader, timeout: float = 10.0) -> AsyncIterable[bytes]:
    """
    Yields a stream of Arrow data in bytes format from the given FlightStreamReader asynchronously.

    This function reads chunks from a FlightStreamReader, converts each chunk into
    a stream of bytes using Arrow IPC (Inter-Process Communication) format, and
    yields these bytes asynchronously.

    Args:
        reader (flight.FlightStreamReader): A FlightStreamReader object to read data chunks from.
        timeout (float): Timeout in seconds. Used in waiting for reading a chunk from the given FlightStreamReader.

    Yields:
        bytes: Serialized Arrow data in bytes format.
    """
    schema = reader.schema

    def next_chunk() -> flight.FlightStreamChunk | None:
        try:
            return reader.read_chunk()
        except StopIteration:
            pass

    while True:
        try:
            chunk = await asyncio.wait_for(asyncio.to_thread(next_chunk), timeout=timeout)
            if chunk is None or chunk.data is None:
                logger.info("Read empty chunk. Stopping the reader.")
                break
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, schema) as writer:
                writer.write_batch(chunk.data)
            buffer: pa.Buffer = sink.getvalue()
            yield buffer.to_pybytes()
        except flight.FlightCancelledError:
            logger.info("Flight cancelled, stopping the reader.")
            break
        except asyncio.TimeoutError:
            logger.info("Read chunk timeout, stopping the reader.")
            break
        except Exception as e:
            logger.info(f"An error occurred: {e}")
            break


if __name__ == "__main__":

    async def main():
        client = PooledClient("grpc://localhost:8815")
        from demo.internal.flight_service.models.params import SqlParams

        query_params = SqlParams(query="select 1 as a")
        logger.info(query_params.to_bytes())

        async for arrow_bytes in client.aget_bytes_stream(query_params.to_bytes()):
            logger.info(f"Received {len(arrow_bytes)} bytes")

        df = await client.aread_pd_df(query_params.to_bytes())
        print(df)

    asyncio.run(main())
