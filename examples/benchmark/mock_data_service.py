import asyncio
import logging
from typing import AsyncIterable, TypeAlias

import numpy as np
import pyarrow as pa

from fastflight.data_service_base import BaseDataService, BaseParams

logger = logging.getLogger(__name__)

kind = "MockData"


@BaseParams.register(kind)
class MockDataParams(BaseParams):
    records_per_batch: int
    batch_generation_delay: float


T: TypeAlias = MockDataParams

# to generate the same data for the same dimension
np.random.seed(0)


# Pre-create a large pyarrow table
def create_large_pyarrow_table(total_rows, total_cols):
    column_names = [f"col{i + 1}" for i in range(total_cols)]
    columns = [pa.array(np.random.randint(0, 50_000, size=total_rows)) for _ in range(total_cols)]
    return pa.table(columns, names=column_names)


# Pre-create a large table 1 million rows and 50 columns
TABLE = create_large_pyarrow_table(total_rows=1_000_000, total_cols=50)


@BaseDataService.register(kind)
class MockDataService(BaseDataService[T]):
    async def aget_batches(self, params: T, batch_size: int | None = None) -> AsyncIterable[pa.RecordBatch]:
        for b in TABLE.to_batches(params.records_per_batch):
            # simulate an I/O wait time
            await asyncio.sleep(params.batch_generation_delay)
            yield b
