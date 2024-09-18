import asyncio
import concurrent.futures
import logging
import threading
from typing import AsyncIterable, Awaitable, Iterator, TypeVar, Union

import pandas as pd
import pyarrow as pa

T = TypeVar("T")
logger = logging.getLogger(__name__)


class AsyncToSyncConverter:
    def __init__(self):
        # 创建一个独立的事件循环并运行它
        self.loop = asyncio.new_event_loop()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.loop_thread = threading.Thread(target=self._start_loop, daemon=True)
        self.loop_thread.start()

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def stop_loop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.executor.shutdown()

    def run_coroutine(self, coro):
        """
        将协程提交到独立线程中的事件循环，并同步等待其结果。
        """
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result()


converter = AsyncToSyncConverter()


def syncify_async_iter(aiter: Union[AsyncIterable[T], Awaitable[AsyncIterable[T]]]) -> Iterator[T]:
    """
    将异步生成器转换为同步迭代器。
    :param aiter: 一个异步可迭代对象或一个可等待的返回异步可迭代对象的对象
    :param converter: AsyncToSyncConverter 用于在线程中管理事件循环
    :return: 一个同步迭代器
    """

    async def _iterate(queue: asyncio.Queue):
        # 处理可能是 Awaitable 的情况
        if asyncio.iscoroutine(aiter):
            aiter_resolved = await aiter
        else:
            aiter_resolved = aiter
        async for item in aiter_resolved:
            await queue.put(item)
        await queue.put(None)  # 标记结束

    # 创建一个队列来管理生成器的值
    queue = asyncio.Queue()

    # 将生成器提交到事件循环中异步执行
    converter.loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_iterate(queue)))

    # 从队列中同步获取结果
    while True:
        result = converter.run_coroutine(queue.get())
        if result is None:
            break
        yield result


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
