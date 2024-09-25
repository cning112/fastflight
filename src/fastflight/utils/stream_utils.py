import asyncio
import logging
import threading
from typing import AsyncGenerator, AsyncIterable, Awaitable, Iterable, Iterator, List, Optional, TypeVar, Union

import pandas as pd
import pyarrow as pa
from pyarrow import RecordBatch, flight

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AsyncToSyncConverter:
    """
    A utility class to convert asynchronous iterables into synchronous ones.
    It manages an asyncio event loop and allows synchronous code to consume async iterables.

    This class can either use a provided event loop or create its own in a separate thread.
    It provides methods to submit coroutines and convert async iterators to sync iterators.

    Example usage:
        async def async_gen():
            for i in range(5):
                await asyncio.sleep(0.5)
                yield i

        with AsyncToSyncConverter() as converter:
            for value in converter.syncify_async_iter(async_gen()):
                print(value)

    Compatibility:
        - Python 3.7 and later:
            - This code is designed to work with Python 3.7 and later versions.
            - It leverages features from Python 3.7 such as `asyncio.run_coroutine_threadsafe`,
              and the stable `async`/`await` syntax, which was fully optimized in Python 3.7+.
            - The `asyncio.Queue`, `async for`, and `await` used in this code are well supported and stable from Python 3.7 onwards.
    """

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """
        Initializes the AsyncToSyncConverter.

        Args:
            loop (Optional[asyncio.AbstractEventLoop]): An existing event loop.
                If not provided, a new loop will be created and run in a separate thread.
        """
        if loop:
            self.loop: asyncio.AbstractEventLoop = loop
            # If an existing event loop is passed, we do not need a separate thread.
            self.loop_thread: Optional[threading.Thread] = None
            logger.info("Using the provided event loop.")
        else:
            # Create a new event loop and run it in a separate thread.
            self.loop = asyncio.new_event_loop()
            self.loop_thread = threading.Thread(target=self._start_loop, daemon=True)
            self.loop_thread.start()
            logger.info("Created a new event loop and started a new thread.")

    def _start_loop(self) -> None:
        """
        Starts the event loop in a separate thread if a new loop was created.
        """
        logger.debug("Starting event loop in a separate thread.")
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def close(self) -> None:
        """
        Safely stops the event loop and waits for the thread to join.
        This method is only needed if a new event loop and thread were created.
        """
        # Stop the event loop and join the thread only if a new loop was created in a separate thread.
        if self.loop_thread:
            logger.info("Stopping the event loop and joining the thread.")
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.loop_thread.join()  # Ensure the thread is joined after stopping the loop.
            logger.info("Event loop stopped, and thread joined.")

    def run_coroutine(self, coro: Awaitable[T]) -> T:
        """
        Submits a coroutine to the event loop and waits for the result synchronously.

        Args:
            coro (Awaitable[T]): The coroutine to run.

        Returns:
            T: The result of the coroutine.
        """
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        result = future.result()
        return result

    def syncify_async_iter(self, aiter: Union[AsyncIterable[T], Awaitable[AsyncIterable[T]]]) -> Iterator[T]:
        """
        Converts an asynchronous iterable into a synchronous iterator.

        Args:
            aiter (Union[AsyncIterable[T], Awaitable[AsyncIterable[T]]]): The async iterable or awaitable returning an async iterable.

        Returns:
            Iterator[T]: A synchronous iterator that can be used in a for loop.
        """

        sentinel = object()  # Unique sentinel object to mark the end of the iteration.
        queue: asyncio.Queue = asyncio.Queue()

        async def _iterate() -> None:
            """
            Internal function to iterate over the async iterable and place results into the queue.
            Runs within the event loop.
            """
            try:
                async for item in aiter:
                    await queue.put((False, item))
            except Exception as e:
                logger.error("Error during iteration: %s", e)
                await queue.put((True, e))
            finally:
                logger.debug("Queueing sentinel to indicate end of iteration.")
                await queue.put(sentinel)  # Put sentinel to signal the end of the iteration.

        logger.debug("Scheduling the async iterable to run in the event loop.")
        # Schedule the async iterable to run in the event loop.
        self.loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_iterate()))

        # Synchronously retrieve results from the queue.
        while True:
            result = self.run_coroutine(queue.get())  # Fetch the next result from the queue.
            if result is sentinel:
                logger.info("End of iteration reached.")
                break
            if isinstance(result, tuple):
                is_exception, item = result
                if is_exception:
                    logger.error(f"Reraising exception from async iterable: {item}")
                    raise item
                else:
                    yield item

    def __enter__(self) -> "AsyncToSyncConverter":
        """
        Context manager entry point.
        Returns:
            AsyncToSyncConverter: The instance itself for use in a 'with' block.
        """
        logger.info("Entering context manager for AsyncToSyncConverter.")
        return self

    def __exit__(
        self, exc_type: Optional[type], exc_value: Optional[BaseException], traceback: Optional[object]
    ) -> None:
        """
        Context manager exit point. Closes the event loop if necessary and joins the thread.
        """
        logger.info("Exiting context manager for AsyncToSyncConverter.")
        self.close()


async def record_batches_from_stream(
    stream: AsyncIterable[T] | Awaitable[AsyncIterable[T]], schema: pa.Schema | None = None, batch_size: int = 100
) -> AsyncIterable[pa.RecordBatch]:
    """
    Similar to `more_itertools.chunked`, but returns an async iterable of Arrow RecordBatch.
    Args:
        stream (AsyncIterable[T]): An async iterable of data of type T. A list of T must be used to create a pd.DataFrame
        schema (pa.Schema | None, optional): The schema of the stream. Defaults to None and will be inferred.
        batch_size (int): The maximum size of each batch. Defaults to 100.

    Yields:
        pa.RecordBatch:  An async iterable of Arrow RecordBatch.
    """
    buffer = []
    async for row in stream:
        buffer.append(row)
        if len(buffer) >= batch_size:
            df = pd.DataFrame(buffer)
            batch = pa.RecordBatch.from_pandas(df, schema=schema)
            yield batch
            buffer.clear()

    if buffer:
        df = pd.DataFrame(buffer)
        batch = pa.RecordBatch.from_pandas(df, schema=schema)
        yield batch


async def stream_arrow_data(reader: flight.FlightStreamReader, *, buffer_size=10) -> AsyncGenerator[bytes, None]:
    """
    Convert a FlightStreamReader into an AsyncGenerator of bytes in Arrow IPC format

    :param reader: a FlightStreamReader
    :buffer_size: max size of the queue. WHen the queue is full, the producer will block.
    :return: AsyncGenerator of bytes
    """
    queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=buffer_size)
    end_of_stream = object()

    def next_chunk():
        try:
            return reader.read_chunk()
        except StopIteration:
            return end_of_stream

    async def produce() -> None:
        try:
            logger.debug("Start producing Arrow IPC bytes from FlightStreamReader %s", id(reader))
            while True:
                # Not using reader.read_chunk() directly as it throws StopIteration
                chunk = await asyncio.to_thread(next_chunk)

                if chunk is end_of_stream:
                    break

                sink = pa.BufferOutputStream()
                with pa.ipc.new_stream(sink, chunk.data.schema) as writer:
                    writer.write_batch(chunk.data)
                buffer: pa.Buffer = sink.getvalue()
                await queue.put(buffer.to_pybytes())
        except Exception as e:
            logger.error("Error during producing Arrow IPC bytes", e)
        finally:
            await queue.put(end_of_stream)
            logger.debug("End producing Arrow IPC bytes from FlightStreamReader %s", id(reader))

    async def consume() -> AsyncGenerator[bytes, None]:
        while True:
            data: Optional[bytes] = await queue.get()
            if data is end_of_stream:
                break
            yield data

    asyncio.create_task(produce())

    return consume()


def read_table_from_arrow_stream(stream: Iterable[bytes]) -> Optional[pa.Table]:
    """
    Reads Arrow IPC data from the given iterable of byte streams and returns a pyarrow.Table.

    :param stream: Iterable of byte streams.
    :return: pyarrow.Table if data is available; otherwise, returns None.
    """
    all_batches: List[RecordBatch] = []
    buffer = b""  # Buffer to accumulate byte data

    for data in stream:
        buffer += data

        # Attempt to create a RecordBatchStreamReader from the read data
        buffer_reader = pa.BufferReader(buffer)
        try:
            stream_reader = pa.ipc.RecordBatchStreamReader(buffer_reader)
        except pa.ArrowInvalid:
            # If there is not enough data to create a RecordBatch, continue reading more data from the stream
            continue

        while True:
            # Read each record batch
            try:
                batch = stream_reader.read_next_batch()
                all_batches.append(batch)
            except StopIteration:
                # Exit the loop if there are no more record batches
                break
            except pa.ArrowInvalid:
                # If the remaining buffer does not contain enough data to parse a RecordBatch, continue to read more data from the stream
                break

        # Update buffer to remove processed data
        if buffer_reader is not None:
            buffer = buffer[buffer_reader.tell() :]  # Remaining unprocessed data

    if all_batches:
        return pa.Table.from_batches(all_batches)


def read_dataframe_from_arrow_stream(stream: Iterable[bytes]) -> Optional[pa.Table]:
    table = read_table_from_arrow_stream(stream)
    if table is not None:
        return table.to_pandas()
