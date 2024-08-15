from pydantic import Field

from .base_ticket import BaseTicket
from .data_source import DataSource


@BaseTicket.register(DataSource.SQL)
class SQLQueryTicket(BaseTicket):
    query: str = Field(..., min_length=1)


@BaseTicket.register(DataSource.NoSQL)
class NoSQLQueryTicket(BaseTicket):
    collection: str = Field(...)
    filter: dict = Field(default={})
