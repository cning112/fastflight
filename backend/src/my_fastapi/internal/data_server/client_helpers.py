import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import AsyncIterable

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

logger = logging.getLogger(__name__)


class FlightClientPool:
    """
    A pool to manage multiple FlightClient instances for efficient reuse.

    Attributes:
        uri (str): The URI of the Flight server.
        queue (asyncio.Queue): A queue to manage the FlightClient instances.
    """

    def __init__(self, uri: str, size: int = 5) -> None:
        """
        Initializes the FlightClientPool with a specified number of FlightClient instances.

        Args:
            uri (str): The URI of the Flight server.
            size (int): The number of FlightClient instances to maintain in the pool.
        """
        self.uri = uri
        self.queue: asyncio.Queue[flight.FlightClient] = asyncio.Queue(maxsize=size)
        for _ in range(size):
            self.queue.put_nowait(flight.FlightClient(uri))
        logger.info(f"Created FlightClientPool with {size} clients at {uri}")

    @asynccontextmanager
    async def acquire(self):
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

    async def close(self):
        while not self.queue.empty():
            client = await self.queue.get()
            client.close()


class FlightClientHelper:
    """
    A helper class to manage FlightClient operations using a connection pool.

    Attributes:
        client_pool (FlightClientPool): The pool of FlightClient instances.
        executor (concurrent.futures.ThreadPoolExecutor | None): The executor to run blocking calls in a separate thread.
        loop (asyncio.AbstractEventLoop): The current event loop.
    """

    def __init__(self, uri: str, client_pool_size: int = 5, executor: ThreadPoolExecutor | None = None) -> None:
        """
        Initializes the FlightClientHelper with a specified connection pool size and thread pool size.

        Args:
            uri (str): The URI of the Flight server.
            client_pool_size (int): The number of FlightClient instances to maintain in the pool.
            executor (ThreadPoolExecutor | None): An executor to use in event loops
        """
        self.client_pool = FlightClientPool(uri, client_pool_size)
        self.loop = asyncio.get_running_loop()
        self.executor = executor

    async def fetch_arrow_stream_async(self, ticket_bytes: bytes) -> AsyncIterable[bytes]:
        reader = await self.fetch_data_reader_async(ticket_bytes)
        async for data in flight_reader_to_arrow_stream(reader):
            yield data

    async def fetch_data_reader_async(self, ticket_bytes: bytes) -> flight.FlightStreamReader:
        """
        Fetches arrow data from the Flight server using the provided ticket asynchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        async with self.client_pool.acquire() as client:
            flight_ticket = flight.Ticket(ticket_bytes)
            try:
                reader = await self.loop.run_in_executor(self.executor, client.do_get, flight_ticket)
                return reader
            except Exception as e:
                logger.error(f"Error fetching data: {e}")
                raise

    async def fetch_data_as_pandas_async(self, ticket_bytes: bytes) -> pd.DataFrame:
        """
        Fetches data from the Flight server and converts it to a Pandas DataFrame asynchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        reader = await self.fetch_data_reader_async(ticket_bytes)
        batches = [await self.loop.run_in_executor(self.executor, batch.data.to_pandas) for batch in reader]
        return pd.concat(batches, ignore_index=True)

    async def fetch_data_as_arrow_async(self, ticket_bytes: bytes) -> pa.Table:
        """
        Fetches data from the Flight server and keeps it in Arrow format asynchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        reader = await self.fetch_data_reader_async(ticket_bytes)
        batches = [await self.loop.run_in_executor(self.executor, batch.data) for batch in reader]
        return pa.Table.from_batches(batches)

    def fetch_data_reader(self, ticket_bytes: bytes) -> flight.FlightStreamReader:
        """
        Fetches data from the Flight server using the provided ticket synchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        return asyncio.run(self.fetch_data_reader_async(ticket_bytes))

    def fetch_data_as_pandas(self, ticket_bytes: bytes) -> pd.DataFrame:
        """
        Fetches data from the Flight server and converts it to a Pandas DataFrame synchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            pd.DataFrame: The data from the Flight server as a Pandas DataFrame.
        """
        return asyncio.run(self.fetch_data_as_pandas_async(ticket_bytes))

    def fetch_data_as_arrow(self, ticket_bytes: bytes) -> pa.Table:
        """
        Fetches data from the Flight server and keeps it in Arrow format synchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            pa.Table: The data from the Flight server as an Arrow Table.
        """
        return asyncio.run(self.fetch_data_as_arrow_async(ticket_bytes))

    async def close(self):
        await self.client_pool.close()
        if self.executor:
            self.executor.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.close())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


async def flight_reader_to_arrow_stream(reader: flight.FlightStreamReader) -> AsyncIterable[bytes]:
    """
    Convert a FlightStreamReader into an asynchronous generator of bytes.

    This function reads chunks from a FlightStreamReader, converts each chunk into
    a stream of bytes using Arrow IPC (Inter-Process Communication) format, and
    yields these bytes asynchronously.

    Parameters
    ----------
    reader : flight.FlightStreamReader
        A FlightStreamReader object to read data chunks from.

    Yields
    ------
    bytes
        Byte streams representing each data chunk in Arrow IPC format.
    """
    schema = reader.schema

    def read_chunk():
        try:
            return reader.read_chunk()
        except StopIteration:
            pass

    while True:
        try:
            chunk = await asyncio.wait_for(asyncio.to_thread(read_chunk), timeout=10.0)
            if chunk is None or chunk.data is None:
                break
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, schema) as writer:  # 使用获取的 schema
                writer.write_batch(chunk.data)  # 从 chunk 中提取 data
            buffer = sink.getvalue()
            yield buffer.to_pybytes()  # 将 pyarrow.Buffer 转换为 bytes
        except flight.FlightCancelledError:
            print("Flight cancelled, stopping the reader.")
            break
        except asyncio.TimeoutError:
            print("Read chunk timeout, stopping the reader.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            break


if __name__ == "__main__":

    async def main():
        client = FlightClientHelper("grpc://localhost:8815")
        ticket_bytes = b'{"kind": "sql", "query": "select 1 as a"}'

        async for arrow_bytes in client.fetch_arrow_stream_async(ticket_bytes):
            print(f"Received {len(arrow_bytes)} bytes")

    asyncio.run(main())
