from typing import AsyncIterable

import pyarrow as pa
import pyarrow.csv as csv

from demo.flight_service.models.data_kinds import DataKind
from demo.flight_service.models.params import CsvFileParams
from fastflight.services.base import BaseDataService

T = CsvFileParams


@BaseDataService.register(DataKind.CSV)
class CsvFileService(BaseDataService[T]):
    async def aget_batches(self, params: T, batch_size: int = 100) -> AsyncIterable[pa.RecordBatch]:
        with csv.open_csv(params.path, read_options=csv.ReadOptions(block_size=batch_size)) as reader:
            while (batch := reader.read_next_batch()).num_rows > 0:
                yield batch
