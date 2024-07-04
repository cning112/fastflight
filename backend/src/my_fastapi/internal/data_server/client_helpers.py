import asyncio
import concurrent.futures
import logging
from contextlib import asynccontextmanager

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
        executor (concurrent.futures.ThreadPoolExecutor): The executor to run blocking calls in a separate thread.
        loop (asyncio.AbstractEventLoop): The current event loop.
    """

    def __init__(self, uri: str, client_pool_size: int = 5, max_workers: int = 10) -> None:
        """
        Initializes the FlightClientHelper with a specified connection pool size and thread pool size.

        Args:
            uri (str): The URI of the Flight server.
            client_pool_size (int): The number of FlightClient instances to maintain in the pool.
            max_workers (int): The maximum number of worker threads in the thread pool.
        """
        self.client_pool = FlightClientPool(uri, client_pool_size)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.loop = asyncio.get_event_loop()
        self.loop.set_default_executor(self.executor)

    async def fetch_data_async(self, ticket_bytes: bytes) -> flight.FlightStreamReader:
        """
        Fetches data from the Flight server using the provided ticket asynchronously.

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
        reader = await self.fetch_data_async(ticket_bytes)
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
        reader = await self.fetch_data_async(ticket_bytes)
        batches = [await self.loop.run_in_executor(self.executor, batch.data) for batch in reader]
        return pa.Table.from_batches(batches)

    def fetch_data(self, ticket_bytes: bytes) -> flight.FlightStreamReader:
        """
        Fetches data from the Flight server using the provided ticket synchronously.

        Args:
            ticket_bytes (bytes): The ticket bytes to request data from the Flight server.

        Returns:
            flight.FlightStreamReader: A reader to stream data from the Flight server.
        """
        return asyncio.run(self.fetch_data_async(ticket_bytes))

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
        self.executor.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.close())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


# 使用示例
# uri = "grpc://localhost:8815"
# helper = FlightClientHelper(uri, pool_size=5, max_workers=10)

# # 获取数据并转换为 Pandas DataFrame
# ticket_bytes = ...  # 从服务器接收的票据字节数据
# df = helper.fetch_data_as_pandas(ticket_bytes)
# print(df)

# # 获取数据并保持 Arrow 格式
# table = helper.fetch_data_as_arrow(ticket_bytes)
# print(table)
