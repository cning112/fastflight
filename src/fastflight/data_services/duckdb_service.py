import logging
from typing import Any, AsyncIterable, Dict, Optional, Sequence

import pyarrow as pa

from fastflight.data_service_base import BaseDataService, BaseParams

logger = logging.getLogger(__name__)


class DuckDBParams(BaseParams):
    """
    Parameters for DuckDB-based data services, supporting both file and in-memory queries.
    """

    database_path: Optional[str] = None
    query: str
    parameters: Optional[Dict[str, Any] | Sequence[Any]] = None


@BaseDataService.register(DuckDBParams)
class DuckDBDataService(BaseDataService[DuckDBParams]):
    """
    Data service for DuckDB, supporting both file-based and in-memory databases.
    Executes SQL queries and returns results in Arrow format.
    """

    async def aget_batches(
        self, params: DuckDBParams, batch_size: Optional[int] = None
    ) -> AsyncIterable[pa.RecordBatch]:
        try:
            import duckdb
        except ImportError:
            logger.error("DuckDB not installed")
            raise ImportError("DuckDB not installed. Install with 'pip install duckdb'")

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

                def gen():
                    for batch in arrow_table.to_batches(max_chunksize=batch_size):
                        yield batch

                return gen()

            except Exception as e:
                logger.error(f"DuckDB execution error: {str(e)}", exc_info=True)
                raise
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"DuckDB service error: {str(e)}", exc_info=True)
            raise
