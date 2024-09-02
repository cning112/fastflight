from pathlib import Path

from pydantic import Field

from fastflight.models.base_params import BaseParams
from fastflight.models.data_source_kind import DataSourceKind


@BaseParams.register(DataSourceKind.SQL)
class SqlParams(BaseParams):
    query: str = Field(..., min_length=1)


@BaseParams.register(DataSourceKind.NoSQL)
class NoSqlParams(BaseParams):
    collection: str = Field(...)
    filter: dict = Field(default={})


@BaseParams.register(DataSourceKind.CSV)
class CsvFileParams(BaseParams):
    path: Path = Field(...)
