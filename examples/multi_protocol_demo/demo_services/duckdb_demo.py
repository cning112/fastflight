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
    def _execute_duckdb_query(params: DuckDBParams) -> pa.Table:
        """Execute DuckDB query in isolation to prevent segfaults."""
        try:
            import duckdb
        except ImportError:
            raise ImportError("DuckDB not installed. Install with 'pip install duckdb' or 'uv add duckdb'") from None

        db_path = params.database_path or ":memory:"
        query_parameters = params.parameters or {}

        with duckdb.connect(db_path) as conn:
            logger.debug(f"Executing query: {params.query}")
            arrow_table = conn.execute(params.query, query_parameters).arrow()
            logger.debug(f"Fetched arrow table with {arrow_table.num_rows} rows")
            return arrow_table

    def get_batches(self, params: DuckDBParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        try:
            logger.info(f"SYNC: Processing request for {params.database_path or ':memory:'}")

            # !!! Execute query in a separate thread to isolate DuckDB from Flight server !!!
            future = self._executor.submit(self._execute_duckdb_query, params)
            table = future.result(timeout=60)  # 60 second timeout

            # Convert to batches after DuckDB connection is closed
            yield from table.to_batches(max_chunksize=batch_size or 10000)

            logger.debug(f"SYNC: Successfully yielded {table.num_rows} rows")

        except Exception as e:
            logger.error(f"SYNC: Service error: {e}", exc_info=True)
            raise

    async def aget_batches(self, params: DuckDBParams, batch_size: int | None = None) -> AsyncIterator[pa.RecordBatch]:
        logger.info(f"ASYNC: Processing request for {params.database_path or ':memory:'}")

        try:
            loop = asyncio.get_running_loop()
            executor = self._executor  # can be None meaning to use a default ThreadPoolExecutor
            table = await loop.run_in_executor(executor, self._execute_duckdb_query, params)

            # Convert to batches and yield with cooperative multitasking
            batches = table.to_batches(max_chunksize=batch_size or 10000)

            for i, batch in enumerate(batches):
                # Yield control to event loop periodically
                if i % 5 == 0:
                    await asyncio.sleep(0)
                yield batch

            logger.debug(f"ASYNC: Successfully yielded {table.num_rows} rows")

        except Exception as e:
            logger.error(f"ASYNC: Service error: {e}", exc_info=True)
            raise
