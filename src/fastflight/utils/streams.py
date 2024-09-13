import asyncio
import logging
from typing import AsyncIterable, Awaitable, Iterator, TypeVar

import pandas as pd
import pyarrow as pa

T = TypeVar("T")
logger = logging.getLogger(__name__)


def syncify_async_iter(aiter: AsyncIterable[T] | Awaitable[AsyncIterable[T]]) -> Iterator[T]:
    """
    Convert an async iterable to a sync iterator.
    :param aiter: An async iterable or an awaitable that returns an async iterable
    :return: A synchronous iterator
    """

    async def _iterate():
        async for item in aiter:
            yield item

    ait = _iterate()
    with EventLoopContext() as loop:
        while True:
            try:
                yield loop.run_until_complete(ait.__anext__())
                # yield asyncio.run(ait.__anext__())
            except StopAsyncIteration:
                break


class EventLoopContext:
    def __enter__(self):
        try:
            self.loop = asyncio.get_event_loop()
            if self.loop.is_closed():
                raise RuntimeError("Event loop is closed")
        except RuntimeError:
            # from python 3.10, asyncio.get_event_loop() raises RuntimeError if there is no running event loop
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.new_loop = True
        else:
            self.new_loop = False
        return self.loop

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.new_loop:
            self.loop.close()


async def stream_to_batches(
    stream: AsyncIterable[T] | Awaitable[AsyncIterable[T]], schema: pa.Schema | None = None, batch_size: int = 100
) -> AsyncIterable[pa.RecordBatch]:
    """
    Similar to `more_itertools.chunked`, but returns an async iterable of Arrow RecordBatch.
    Args:
        stream (AsyncIterable[T]): An async iterable.
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
