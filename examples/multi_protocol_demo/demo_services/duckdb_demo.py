"""
Demo services for FastFlight. These are not imported by default and are only used in CLI/testing scenarios.

CRITICAL FIX: Addressing segmentation fault issues with DuckDB + PyArrow Flight
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable, Sequence
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
    @staticmethod
    def _execute_duckdb_query(params: DuckDBParams) -> pa.Table:
        try:
            import duckdb
        except ImportError:
            raise ImportError("DuckDB not installed. Install with 'pip install duckdb' or 'uv add duckdb'") from None

        db_path = params.database_path or ":memory:"
        query_parameters = params.parameters or {}

        # Create connection with minimal scope
        with duckdb.connect(db_path) as conn:
            logger.debug(f"Executing query: {params.query}")

            # Execute query and get Arrow table
            arrow_table: pa.Table = conn.execute(params.query, query_parameters).arrow()
            logger.debug(f"Got query data of {arrow_table.num_rows} rows in arrow format")
            return arrow_table

    def get_batches(self, params: DuckDBParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        try:
            logger.info(f"SYNC: Processing request for {params.database_path or ':memory:'}")

            # Execute query safely
            table = self._execute_duckdb_query(params)

            # Convert to batches
            yield from table.to_batches(max_chunksize=batch_size or 10000)

            logger.debug(f"SYNC: Successfully yielded {table.num_rows} rows")

        except Exception as e:
            logger.error(f"SYNC: Service error: {e}", exc_info=True)
            raise

    async def aget_batches(self, params: DuckDBParams, batch_size: int | None = None) -> AsyncIterator[pa.RecordBatch]:
        """
        Async batch retrieval with segfault protection.
        """
        logger.info(f"ASYNC: Processing request for {params.database_path or ':memory:'}")

        try:
            # Execute the DuckDB query in a thread executor for safety
            loop = asyncio.get_running_loop()
            table = await loop.run_in_executor(None, self._execute_duckdb_query, params)

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
