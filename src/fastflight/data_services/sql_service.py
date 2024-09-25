from typing import AsyncIterable

import pandas as pd
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
        # Create an SQLAlchemy engine
        engine = create_engine(params.connection_string)

        with engine.connect() as connection:
            # Execute the query with optional parameters
            result: Result = connection.execute(text(params.query), params.parameters or {})

            # Fetch rows in batches
            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break

                # Convert rows to Pandas DataFrame and then to Arrow Table
                df = pd.DataFrame(rows, columns=result.keys())
                table = pa.Table.from_pandas(df)

                # Yield each batch of Arrow RecordBatch
                for batch in table.to_batches():
                    yield batch
