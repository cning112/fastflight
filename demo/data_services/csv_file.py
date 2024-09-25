from pathlib import Path
from typing import AsyncIterable

import pyarrow as pa
import pyarrow.csv as csv
from pydantic import Field

from fastflight.data_service_base import BaseDataService, BaseParams

TYPE = "CSV"


@BaseParams.register(TYPE)
class CsvFileParams(BaseParams):
    path: Path = Field(...)


@BaseDataService.register(TYPE)
class CsvFileService(BaseDataService[CsvFileParams]):
    async def aget_batches(self, params: CsvFileParams, batch_size: int = 100) -> AsyncIterable[pa.RecordBatch]:
        with csv.open_csv(params.path, read_options=csv.ReadOptions(block_size=batch_size)) as reader:
            while (batch := reader.read_next_batch()).num_rows > 0:
                yield batch
