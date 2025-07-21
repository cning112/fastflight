"""
Demo services for FastFlight. These are not imported by default and are only used in CLI/testing scenarios.

CRITICAL FIX: Addressing segmentation fault issues with DuckDB + PyArrow Flight
"""

import gc
import logging
import threading
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
    # Thread-local storage for DuckDB connections to avoid segfaults
    _thread_local = threading.local()

    @classmethod
    def _get_thread_connection(cls, db_path: str):
        """Get or create a thread-local DuckDB connection."""
        try:
            import duckdb
        except ImportError:
            raise ImportError("DuckDB not installed. Install with 'pip install duckdb' or 'uv add duckdb'") from None

        if not hasattr(cls._thread_local, "connections"):
            cls._thread_local.connections = {}

        if db_path not in cls._thread_local.connections:
            cls._thread_local.connections[db_path] = duckdb.connect(db_path)

        return cls._thread_local.connections[db_path]

    @staticmethod
    def _execute_duckdb_query(params: DuckDBParams) -> pa.Table:
        """Execute DuckDB query in isolation to prevent segfaults."""
        try:
            import duckdb
        except ImportError:
            raise ImportError("DuckDB not installed. Install with 'pip install duckdb' or 'uv add duckdb'") from None

        db_path = params.database_path or ":memory:"
        query_parameters = params.parameters or {}

        try:
            # Create a fresh connection for each query to avoid memory conflicts
            # This is less efficient but more stable with PyArrow Flight
            conn = duckdb.connect(db_path)

            try:
                logger.debug(f"Executing query: {params.query}")

                # Execute and immediately fetch as Arrow to minimize DuckDB/Arrow interaction time
                arrow_table = conn.execute(params.query, query_parameters).arrow()
                logger.debug(f"Fetched arrow table with {arrow_table.num_rows} rows")

                # Make a deep copy of the table to ensure it's fully materialized
                # and independent of DuckDB's memory management
                arrow_table = pa.Table.from_arrays(
                    [pa.array(col.to_pylist()) for col in arrow_table.columns], names=arrow_table.column_names
                )

                logger.debug("Created independent arrow table")

                return arrow_table

            finally:
                # Explicitly close the connection and force garbage collection
                conn.close()
                gc.collect()

        except Exception as e:
            logger.error(f"Error executing DuckDB query: {e}", exc_info=True)
            raise

    def get_batches(self, params: DuckDBParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        try:
            logger.info(f"SYNC: Processing request for {params.database_path or ':memory:'}")

            # Execute query in a separate thread to isolate DuckDB from Flight server
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._execute_duckdb_query, params)
                table = future.result(timeout=60)  # 60 second timeout

            # Convert to batches after DuckDB connection is closed
            yield from table.to_batches(max_chunksize=batch_size or 10000)

            logger.debug(f"SYNC: Successfully yielded {table.num_rows} rows")

        except Exception as e:
            logger.error(f"SYNC: Service error: {e}", exc_info=True)
            raise

    async def aget_batches(self, params: DuckDBParams, batch_size: int | None = None) -> AsyncIterator[pa.RecordBatch]:
        """
        Async batch retrieval with segfault protection.
        """
        import asyncio

        logger.info(f"ASYNC: Processing request for {params.database_path or ':memory:'}")

        try:
            # Execute the DuckDB query in a thread executor for safety
            loop = asyncio.get_running_loop()

            # Use a dedicated thread pool executor with a single worker
            # to ensure DuckDB operations are isolated
            with ThreadPoolExecutor(max_workers=1) as executor:
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
