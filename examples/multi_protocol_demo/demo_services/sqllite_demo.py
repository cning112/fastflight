from collections.abc import Iterable

import pyarrow as pa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Result

from fastflight.core.base import BaseDataService, BaseParams


class SQLParams(BaseParams):
    conn_str: str  # SQLAlchemy connection string
    query: str  # SQL query
    parameters: dict | list | None = None  # Optional query parameters


class SQLService(BaseDataService[SQLParams]):
    def get_batches(self, params: SQLParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        engine = create_engine(params.conn_str)
        with engine.connect() as connection:
            result: Result = connection.execute(text(params.query), params.parameters or {})

            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break

                # Create a PyArrow Table from rows
                columns = list(result.keys())
                arrays = [pa.array([row[i] for row in rows]) for i in range(len(columns))]
                table = pa.table(arrays, columns)  # type: ignore[arg-type]
                yield from table.to_batches()
