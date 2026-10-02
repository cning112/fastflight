import asyncio
import logging
from collections.abc import AsyncIterator, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pyarrow as pa

from fastflight.core.base import BaseDataService, BaseParams

logger = logging.getLogger(__name__)


class DuckDBParams(BaseParams):
    """
    Parameters for DuckDB-based data services, supporting both file and in-memory queries.
    """

    database_path: str | None = None
    query: str
    parameters: dict[str, Any] | Sequence[Any] | None = None


class DuckDBDataService(BaseDataService[DuckDBParams]):
    _executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="duckdb_data_service")

    @staticmethod
    def _iterate_duckdb_batches(params: DuckDBParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        """Execute DuckDB query and yield record batches incrementally."""
        try:
            import duckdb
        except ImportError:
            raise ImportError("DuckDB not installed. Install with 'pip install duckdb' or 'uv add duckdb'") from None

        db_path = params.database_path or ":memory:"
        query_parameters = params.parameters or {}

        with duckdb.connect(db_path) as conn:
            logger.debug(f"Executing query: {params.query}")
            arrow_obj: Any = conn.execute(params.query, query_parameters).arrow()

            reader: pa.RecordBatchReader
            if isinstance(arrow_obj, pa.RecordBatchReader):  # type: ignore[unreachable, unused-ignore]
                reader = arrow_obj
            elif isinstance(arrow_obj, pa.RecordBatch):  # type: ignore[unreachable, unused-ignore]
                reader = pa.RecordBatchReader.from_batches(arrow_obj.schema, [arrow_obj])
            elif isinstance(arrow_obj, pa.Table):  # type: ignore[unreachable, unused-ignore]
                reader = arrow_obj.to_reader()
            else:
                raise TypeError(f"Unexpected DuckDB Arrow result type: {type(arrow_obj)!r}")

            for batch in reader:
                if batch.num_rows == 0:
                    continue
                if batch_size and batch.num_rows > batch_size:
                    for offset in range(0, batch.num_rows, batch_size):
                        yield batch.slice(offset, min(batch_size, batch.num_rows - offset))
                else:
                    yield batch

    @staticmethod
    def _submit_batch_producer(params: DuckDBParams, batch_size: int | None, put_item) -> None:
        """Run in thread pool: stream batches and push them via callback."""
        try:
            for batch in DuckDBDataService._iterate_duckdb_batches(params, batch_size):
                put_item(batch)
        except Exception as exc:  # propagate to consumer
            put_item(exc)
        finally:
            put_item(None)  # sentinel

    def get_batches(self, params: DuckDBParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        try:
            logger.info(f"SYNC: Processing request for {params.database_path or ':memory:'}")

            import queue

            batches_queue: queue.Queue[pa.RecordBatch | Exception | None] = queue.Queue(maxsize=4)

            def put_item(item):
                batches_queue.put(item)

            self._executor.submit(self._submit_batch_producer, params, batch_size, put_item)

            total_rows = 0
            while True:
                item = batches_queue.get()
                if item is None:
                    break
                if isinstance(item, Exception):
                    raise item
                batch = item
                total_rows += batch.num_rows
                yield batch

            logger.debug(f"SYNC: Successfully yielded {total_rows} rows")

        except Exception as e:
            logger.error(f"SYNC: Service error: {e}", exc_info=True)
            raise

    async def aget_batches(self, params: DuckDBParams, batch_size: int | None = None) -> AsyncIterator[pa.RecordBatch]:
        logger.info(f"ASYNC: Processing request for {params.database_path or ':memory:'}")

        try:
            loop = asyncio.get_running_loop()
            executor = self._executor  # can be None meaning to use a default ThreadPoolExecutor

            async_queue: asyncio.Queue[pa.RecordBatch | Exception | None] = asyncio.Queue(maxsize=4)

            def put_item(item):
                asyncio.run_coroutine_threadsafe(async_queue.put(item), loop)

            executor.submit(self._submit_batch_producer, params, batch_size, put_item)

            total_rows = 0
            batch_count = 0
            while True:
                item = await async_queue.get()
                if item is None:
                    break
                if isinstance(item, Exception):
                    raise item
                batch = item
                total_rows += batch.num_rows
                batch_count += 1
                yield batch
                if batch_count % 5 == 0:
                    await asyncio.sleep(0)

            logger.debug(f"ASYNC: Successfully yielded {total_rows} rows")

        except Exception as e:
            logger.error(f"ASYNC: Service error: {e}", exc_info=True)
            raise
