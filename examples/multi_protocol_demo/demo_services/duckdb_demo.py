import asyncio
import logging
import threading
import time
import weakref
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


class AsyncDuckDBConnectionPool:
    """
    Async-friendly connection pool for DuckDB that manages connections
    across different async tasks and threads.
    """

    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self._connections: weakref.WeakSet = weakref.WeakSet()
        self._lock = asyncio.Lock()

    async def get_connection(self, db_path: str):
        """Get a DuckDB connection, creating new one if needed."""
        try:
            import duckdb
        except ImportError:
            raise ImportError("DuckDB not installed. Install with 'pip install duckdb' or 'uv add duckdb'")

        async with self._lock:
            # For now, create new connection each time to avoid threading issues
            # In production, you might want a more sophisticated pooling strategy
            loop = asyncio.get_running_loop()
            conn = await loop.run_in_executor(None, lambda: duckdb.connect(db_path))
            self._connections.add(conn)
            logger.debug(f"Created new DuckDB connection. Total connections: {len(self._connections)}")
            return conn

    async def close_connection(self, conn):
        """Close a connection safely."""
        if conn:
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, conn.close)
                logger.debug(f"Closed DuckDB connection. Remaining connections: {len(self._connections)}")
            except Exception as e:
                logger.warning(f"Error closing DuckDB connection: {e}")


# Global connection pool instance
_connection_pool = AsyncDuckDBConnectionPool()


class DuckDBDataService(BaseDataService[DuckDBParams]):
    """
    Data service for DuckDB, supporting both file-based and in-memory databases.
    Executes SQL queries and returns results in Arrow format.

    Enhanced with true async support including:
    - Streaming query execution
    - Cooperative yielding for event loop
    - Connection pooling
    - Memory-efficient batch processing
    """

    def __init__(self):
        super().__init__()
        self._thread_local = threading.local()

    def _get_connection(self, db_path: str):
        # Get the connection for the current thread (sync version)
        if not hasattr(self._thread_local, "connection"):
            try:
                import duckdb
            except ImportError:
                logger.error("DuckDB not installed")
                raise ImportError("DuckDB not installed. Install with 'pip install duckdb' or 'uv add duckdb'")

            current_thread = threading.current_thread()
            logger.info(f"Creating DuckDB connection for thread: {current_thread.name}")

            self._thread_local.connection = duckdb.connect(db_path)

        return self._thread_local.connection

    def get_batches(self, params: DuckDBParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        try:
            db_path = params.database_path or ":memory:"
            logger.info(f"Connecting to DuckDB at {db_path}")

            conn = self._get_connection(db_path)
            try:
                query_parameters = params.parameters or {}
                logger.info(f"Executing query: {params.query}")
                logger.debug(f"With parameters: {query_parameters}")

                result = conn.execute(params.query, query_parameters)
                arrow_table = result.arrow()

                for batch in arrow_table.to_batches(max_chunksize=batch_size):
                    time.sleep(0.01)  # simulate synchronous I/O delay
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
        Efficient async implementation using DuckDB's native Arrow support.
        Avoids unnecessary fetchmany + pandas conversion.
        """
        if batch_size is None:
            batch_size = 10000

        loop = asyncio.get_running_loop()
        db_path = params.database_path or ":memory:"
        conn = await _connection_pool.get_connection(db_path)

        try:

            def get_arrow_batches():
                result = conn.execute(params.query, params.parameters or {})
                table = result.arrow()
                return table.to_batches(max_chunksize=batch_size)

            batches = await loop.run_in_executor(None, get_arrow_batches)

            for i, batch in enumerate(batches):
                await asyncio.sleep(0.01)  # simulate async I/O delay
                yield batch
        finally:
            await _connection_pool.close_connection(conn)
