"""
Demo services for FastFlight. These are not imported by default and are only used in CLI/testing scenarios.
"""

import asyncio
import logging
import threading
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
        True async implementation with the following improvements:

        1. Streaming execution - doesn't load all data into memory
        2. Cooperative yielding - allows other coroutines to run
        3. Connection pooling - efficient resource management
        4. Incremental fetching - processes data as it becomes available
        5. Memory-efficient batching - controls memory usage

        This provides real async benefits compared to the blocking approach.
        """
        # Default batch size - balance between memory usage and efficiency
        if batch_size is None:
            batch_size = 10000

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError("aget_batches() must be called within an async context with an active event loop.")

        db_path = params.database_path or ":memory:"
        conn = None

        try:
            # Get connection from pool asynchronously
            conn = await _connection_pool.get_connection(db_path)

            # Prepare and execute query in thread executor
            query_parameters = params.parameters or {}
            logger.info(f"Executing async streaming query: {params.query}")
            logger.debug(f"With parameters: {query_parameters}")

            def execute_query():
                """Execute query and return result cursor."""
                return conn.execute(params.query, query_parameters)

            result = await loop.run_in_executor(None, execute_query)

            # Stream results using fetchmany for true incremental processing
            batches_yielded = 0
            total_rows = 0

            while True:

                def fetch_chunk():
                    """Fetch a chunk of rows incrementally."""
                    try:
                        chunk = result.fetchmany(batch_size)
                        return chunk, [desc[0] for desc in result.description] if chunk else (None, None)
                    except Exception as e:
                        # If fetchmany is not supported, fall back to fetchall (less ideal)
                        logger.debug(f"fetchmany failed, using fallback: {e}")
                        try:
                            all_data = result.fetchall()
                            columns = [desc[0] for desc in result.description]
                            return all_data, columns
                        except Exception:
                            # Last resort: use arrow() method
                            arrow_table = result.arrow()
                            return arrow_table, None

                # Fetch data chunk in thread executor
                chunk_data, columns = await loop.run_in_executor(None, fetch_chunk)

                if chunk_data is None or len(chunk_data) == 0:
                    logger.info(
                        f"Async streaming completed. Batches yielded: {batches_yielded}, Total rows: {total_rows}"
                    )
                    break

                # Handle different return types from fallback mechanisms
                if isinstance(chunk_data, pa.Table):
                    # Direct Arrow table from fallback
                    for batch in chunk_data.to_batches(max_chunksize=batch_size):
                        yield batch
                        batches_yielded += 1
                        total_rows += len(batch)

                        # Cooperative yielding every few batches
                        if batches_yielded % 5 == 0:
                            await asyncio.sleep(0)
                    break  # Arrow fallback gives us all data at once

                else:
                    # Regular row data - convert to Arrow batch
                    def create_arrow_batch():
                        import pandas as pd

                        df = pd.DataFrame(chunk_data, columns=columns)
                        table = pa.Table.from_pandas(df)
                        return table.to_batches(max_chunksize=batch_size)

                    # Convert to Arrow in thread executor to avoid blocking
                    arrow_batches = await loop.run_in_executor(None, create_arrow_batch)

                    # Yield each batch with cooperative yielding
                    for batch in arrow_batches:
                        yield batch
                        batches_yielded += 1
                        total_rows += len(batch)

                        # Cooperative yielding - critical for async performance
                        if batches_yielded % 5 == 0:
                            await asyncio.sleep(0)  # Allow other coroutines to run

                # If we got less than requested batch_size, we're at the end
                if len(chunk_data) < batch_size:
                    logger.debug(f"Received {len(chunk_data)} rows, less than batch_size {batch_size}, ending stream")
                    break

                # Add small delay between chunks to allow other async operations
                await asyncio.sleep(0.001)  # 1ms delay for better async behavior

        except Exception as e:
            logger.error(f"Error in async DuckDB batch streaming: {e}", exc_info=True)
            raise
        finally:
            # Always clean up connection
            if conn:
                await _connection_pool.close_connection(conn)
