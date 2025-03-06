from typing import AsyncIterable

import pyarrow as pa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Result

from fastflight.data_service_base import BaseDataService, BaseParams


class SQLParams(BaseParams):
    """
    Parameters for SQL-based data data_services, including connection string and query details.
    """

    conn_str: str  # SQLAlchemy connection string
    query: str  # SQL query
    parameters: dict | list | None = None  # Optional query parameters


@BaseDataService.register(SQLParams)
class SQLService(BaseDataService[SQLParams]):
    """
    Data service for SQL-based sources using SQLAlchemy for flexible database connectivity.
    Executes the SQL query and returns data in Arrow batches.
    """

    async def aget_batches(self, params: SQLParams, batch_size: int | None = None) -> AsyncIterable[pa.RecordBatch]:
        engine = create_engine(params.conn_str)
        with engine.connect() as connection:
            result: Result = connection.execute(text(params.query), params.parameters or {})

            def gen():
                while True:
                    rows = result.fetchmany(batch_size)
                    if not rows:
                        break

                    # Create a PyArrow Table from rows
                    columns = list(result.keys())
                    arrays = [pa.array([row[i] for row in rows]) for i in range(len(columns))]
                    table = pa.Table.from_arrays(arrays, columns)
                    for batch in table.to_batches():
                        yield batch

            return gen()


if __name__ == "__main__":
    ticket = SQLParams(
        conn_str="sqlite:///example.db",
        query="select * from financial_data where date >= ? and date <= ?",
        parameters=["2024-01-01T00:00:00Z", "2024-01-31T00:00:00Z"],
    )
    import json

    print(json.dumps(ticket.to_json(), indent=2))
