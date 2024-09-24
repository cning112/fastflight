import logging
from typing import AsyncGenerator, AsyncIterable

import pyarrow as pa
from databases import Database
from pydantic import Field

from fastflight.services.base import BaseDataService, BaseParams
from fastflight.services.data_sources import DataSource
from fastflight.utils.stream_utils import record_batches_from_stream

logger = logging.getLogger(__name__)


@BaseParams.register(DataSource.PostgresSQL)
class PostgresSqlParams(BaseParams):
    connection_string: str = Field(...)
    query: str = Field(...)
    batch_size: int = Field(default=1000)


T = PostgresSqlParams


# engines: dict[str, Engine] = {}
#
#
# async def get_engine(connection_string: str) -> Engine:
#     if connection_string not in engines:
#         engines[connection_string] = await asyncpg.connect(dsn=connection_string)
#     return engines[connection_string]
#
#
# async_sessions: dict[str, sessionmaker[AsyncSession]] = {}
#
#
# async def get_session(connection_string: str) -> AsyncSession:
#     if connection_string not in async_sessions:
#         engine = await get_engine(connection_string)
#         sm = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
#         async_sessions[connection_string] = sm
#     return async_sessions[connection_string]()
#


async def run_query_stream(conn_string: str, query: str) -> AsyncGenerator:
    """
    Run a query and return results in a streaming fashion, based on the given connection string.
    :param conn_string: The connection string to the database (supports PostgreSQL, SQLite, MySQL).
    :param query: The SQL query string to run.
    :return: An async generator yielding query results row by row.
    """
    # Create a Database instance from the connection string
    database = Database(conn_string)

    # Connect to the database
    await database.connect()

    # Execute the query and fetch results in stream
    async with database.connection() as connection:
        async for row in connection.iterate(query):
            yield row

    # Disconnect from the database
    await database.disconnect()


@BaseDataService.register(DataSource.PostgresSQL)
class PostgresSQLDataService(BaseDataService[T]):
    """
    A data source class for SQL queries.
    """

    async def aget_batches(self, params: T, batch_size: int = 100) -> AsyncIterable[pa.RecordBatch]:
        # FIXME: this method doesn't return an AsyncIterable
        logger.info("Running query: %s", params.query)
        stream = run_query_stream(params.connection_string, params.query)
        logger.info("Got Stream")
        return record_batches_from_stream(stream, batch_size=params.batch_size or batch_size)
