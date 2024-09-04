from pathlib import Path

from pydantic import Field

from demo.flight_service.models.data_kinds import DataKind
from fastflight.services.base_params import BaseParams


@BaseParams.register(DataKind.SQL)
class SqlParams(BaseParams):
    query: str = Field(..., min_length=1)


@BaseParams.register(DataKind.NO_SQL)
class NoSqlParams(BaseParams):
    collection: str = Field(...)
    filter: dict = Field(default={})


@BaseParams.register(DataKind.CSV)
class CsvFileParams(BaseParams):
    path: Path = Field(...)
