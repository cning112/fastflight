from pydantic import Field

from .base_params import BaseParams
from .data_source import DataSourceKind


@BaseParams.register(DataSourceKind.SQL)
class SqlParams(BaseParams):
    query: str = Field(..., min_length=1)


@BaseParams.register(DataSourceKind.NoSQL)
class NoSqlParams(BaseParams):
    collection: str = Field(...)
    filter: dict = Field(default={})
