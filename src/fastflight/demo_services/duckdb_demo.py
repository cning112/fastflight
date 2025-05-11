"""
Demo services for FastFlight. These are not imported by default and are only used in CLI/testing scenarios.
"""

import asyncio
import logging
from typing import Any, AsyncIterable, Iterable, Sequence

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
    """
    Data service for DuckDB, supporting both file-based and in-memory databases.
    Executes SQL queries and returns results in Arrow format.
    """

    def get_batches(self, params: DuckDBParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        try:
            import duckdb
        except ImportError:
            logger.error("DuckDB not installed")
            raise ImportError("DuckDB not installed. Install with 'pip install duckdb' or 'uv add duckdb'")

        try:
            db_path = params.database_path or ":memory:"
            logger.info(f"Connecting to DuckDB at {db_path}")

            conn = duckdb.connect(db_path)
            try:
                query_parameters = params.parameters or {}
                logger.info(f"Executing query: {params.query}")
                logger.debug(f"With parameters: {query_parameters}")

                result = conn.execute(params.query, query_parameters)
                arrow_table = result.arrow()

                for batch in arrow_table.to_batches(max_chunksize=batch_size):
                    yield batch

            except Exception as e:
                logger.error(f"DuckDB execution error at {db_path}: {str(e)}", exc_info=True)
                raise
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"DuckDB service error at {params.database_path or ':memory:'}: {str(e)}", exc_info=True)
            raise

    async def aget_batches(self, params: DuckDBParams, batch_size: int | None = None) -> AsyncIterable[pa.RecordBatch]:  # type: ignore
        """
        Asynchronously get batches by wrapping the blocking get_batches method
        in a thread executor to allow async usage.
        """

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError("aget_batches() must be called within an async context with an active event loop.")

        def blocking_get_batches():
            return list(self.get_batches(params, batch_size))

        batches = await loop.run_in_executor(None, blocking_get_batches)
        for batch in batches:
            yield batch


# Example JSON payload for DuckDBParams via REST endpoint:
# {
#   "param_type": "fastflight.demo_services.duckdb_demo.DuckDBParams",
#   "database_path": "example.duckdb",
#   "query": "SELECT * FROM flights WHERE origin = ?",
#   "parameters": ["JFK"]
# }
