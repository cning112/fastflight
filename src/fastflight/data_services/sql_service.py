from typing import AsyncIterable

import pyarrow as pa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Result

from fastflight.data_service_base import BaseDataService, BaseParams


@BaseParams.register("SQL")
class SQLParams(BaseParams):
    """
    Parameters for SQL-based data data_services, including connection string and query details.
    """

    connection_string: str  # SQLAlchemy connection string
    query: str  # SQL query
    parameters: dict | None = None  # Optional query parameters


@BaseDataService.register("SQL")
class SQLDataService(BaseDataService[SQLParams]):
    """
    Data service for SQL-based sources using SQLAlchemy for flexible database connectivity.
    Executes the SQL query and returns data in Arrow batches.
    """

    async def aget_batches(self, params: SQLParams, batch_size: int = 100) -> AsyncIterable[pa.RecordBatch]:
        engine = create_engine(params.connection_string)
        with engine.connect() as connection:
            result: Result = connection.execute(text(params.query), params.parameters or {})

            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break

                # Create a PyArrow Table from rows
                arrays = [pa.array([row[i] for row in rows]) for i in range(len(result.keys()))]
                table = pa.Table.from_arrays(arrays, list(result.keys()))
                for batch in table.to_batches():
                    yield batch
