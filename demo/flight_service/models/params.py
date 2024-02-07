from pathlib import Path

from pydantic import Field

from demo.flight_service.models.data_kinds import DataKind
from fastflight.services.base import BaseParams


@BaseParams.register(DataKind.CSV)
class CsvFileParams(BaseParams):
    path: Path = Field(...)
