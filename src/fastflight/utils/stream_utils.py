import asyncio
import io
import logging
import threading
from typing import Any, AsyncIterable, Awaitable, Iterable, Iterator, Optional, TypeVar, Union

import pandas as pd
import pyarrow as pa
from pyarrow import flight
from pyarrow._flight import FlightStreamChunk

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

    async def _iterate(
        self, queue: asyncio.Queue, ait: Union[AsyncIterable[T], Awaitable[AsyncIterable[T]]], sentinel: Any
    ) -> None:
        """
        Internal function to iterate over the async iterable and place results into the queue.
        Runs within the event loop.
        """
        try:
            if not hasattr(ait, "__aiter__"):
                ait = await ait

            async for item in ait:
                await queue.put((False, item))
        except Exception as e:
            logger.error("Error during iteration: %s", e)
            await queue.put((True, e))
        finally:
            logger.debug("Queueing sentinel to indicate end of iteration.")
            await queue.put(sentinel)  # Put sentinel to signal the end of the iteration.

    def syncify_async_iter(self, ait: Union[AsyncIterable[T], Awaitable[AsyncIterable[T]]]) -> Iterator[T]:
        """
        Converts an asynchronous iterable into a synchronous iterator.
        Note that this method doesn't load the entire async iterable into memory and then iterates over it.

        Args:
            ait (Union[AsyncIterable[T], Awaitable[AsyncIterable[T]]]): The async iterable or awaitable returning an async iterable.

        Returns:
            Iterator[T]: A synchronous iterator that can be used in a for loop.
        """

        sentinel = object()  # Unique sentinel object to mark the end of the iteration.
        queue: asyncio.Queue = asyncio.Queue()

        logger.debug("Scheduling the async iterable to run in the event loop.")
        # Schedule the async iterable to run in the event loop.
        self.loop.call_soon_threadsafe(lambda: asyncio.ensure_future(self._iterate(queue, ait, sentinel)))

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


async def read_record_batches_from_stream(
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

    if not hasattr(stream, "__aiter__"):
        stream = await stream

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


async def write_arrow_data_to_stream(reader: flight.FlightStreamReader, *, buffer_size=10) -> AsyncIterable[bytes]:
    """
    Convert a FlightStreamReader into an AsyncGenerator of bytes in Arrow IPC format.

    This function employs a producer-consumer pattern:
    - The producer reads data from the FlightStreamReader by calling its blocking `read_chunk` method.
    - To avoid blocking the event loop, the blocking call is wrapped in `asyncio.to_thread`, which
      runs it in a background thread.
    - The producer converts each chunk into Arrow IPC formatted bytes and puts them into an async queue.
    - The consumer asynchronously yields bytes from the queue.

    :param reader: A FlightStreamReader instance.
    :param buffer_size: Maximum size of the internal queue. When full, the producer will block.
    :return: An AsyncGenerator that yields bytes in Arrow IPC format.
    """
    # Create an async queue to hold produced byte chunks.
    queue: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=buffer_size)
    # Sentinel object to signal the end of the stream.
    end_of_stream = object()

    def next_chunk() -> FlightStreamChunk:
        """
        Wrap the synchronous read_chunk call and handle StopIteration.

        Since reader.read_chunk() is a blocking call, this helper function allows us to run it in a
        background thread using asyncio.to_thread.
        """
        try:
            return reader.read_chunk()
        except StopIteration:
            return end_of_stream  # type: ignore[return-value]

    async def produce() -> None:
        """
        Producer coroutine that continuously retrieves data chunks from the reader,
        converts them into Arrow IPC formatted bytes, and puts them into the queue.

        The blocking call to read_chunk is executed in a background thread using asyncio.to_thread
        to ensure the event loop remains responsive.
        """
        try:
            logger.debug("Start producing Arrow IPC bytes from FlightStreamReader %s", id(reader))
            while True:
                # Wrap the blocking next_chunk() call in asyncio.to_thread to run it without blocking the event loop.
                chunk = await asyncio.to_thread(next_chunk)
                if chunk is end_of_stream:
                    # If the sentinel is received, break the loop.
                    break

                if chunk.data is None:
                    # TODO: when can this happen?
                    continue

                # Convert the chunk's data into Arrow IPC format.
                sink = pa.BufferOutputStream()
                # Using the new_stream context manager to write IPC data based on the chunk's schema.
                with pa.ipc.new_stream(sink, chunk.data.schema) as writer:
                    writer.write_batch(chunk.data)
                # Retrieve the bytes from the output buffer.
                buffer_value: pa.Buffer = sink.getvalue()
                await queue.put(buffer_value.to_pybytes())
        except Exception as e:
            logger.error("Error during producing Arrow IPC bytes", exc_info=True)
            await queue.put(e)  # type: ignore[arg-type]
        finally:
            # Signal the consumer that production is complete.
            await queue.put(end_of_stream)  # type: ignore[arg-type]
            logger.debug("End producing Arrow IPC bytes from FlightStreamReader %s", id(reader))

    async def consume() -> AsyncIterable[bytes]:
        """
        Consumer coroutine that yields bytes from the queue.

        Iteration stops when the end-of-stream sentinel is encountered, or an exception is raised.
        """
        while True:
            data: Optional[bytes] = await queue.get()
            if data is None:
                # TODO: can this happen?
                continue
            if data is end_of_stream:
                break
            elif isinstance(data, Exception):
                raise data
            yield data

    # Launch the producer task in the background.
    asyncio.create_task(produce())
    # Return the consumer async generator.
    return consume()


class IterableBytesIO(io.RawIOBase):
    def __init__(self, iterable: Iterable[bytes]):
        self.iterable = iter(iterable)
        self.buffer = b""

    def read(self, size=-1) -> bytes:
        if size == -1:
            return b"".join(self.iterable)

        while len(self.buffer) < size:
            try:
                self.buffer += next(self.iterable)
            except StopIteration:
                break

        result, self.buffer = self.buffer[:size], self.buffer[size:]
        return result

    def readable(self) -> bool:
        return True


def read_table_from_arrow_stream(stream: Iterable[bytes]) -> pa.Table:
    stream_io = IterableBytesIO(stream)
    return pa.ipc.RecordBatchStreamReader(stream_io).read_all()


def read_dataframe_from_arrow_stream(stream: Iterable[bytes]) -> pd.DataFrame:
    table = read_table_from_arrow_stream(stream)
    return table.to_pandas()
