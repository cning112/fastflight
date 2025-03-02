import asyncio
import logging
from typing import AsyncIterable

import numpy as np
import pyarrow as pa

from fastflight.data_service_base import BaseDataService, BaseParams

logger = logging.getLogger(__name__)


class MockDataParams(BaseParams):
    rows_per_batch: int
    delay_per_row: float


# to generate the same data for the same dimension
np.random.seed(0)


# Pre-create a large pyarrow table
def create_large_pyarrow_table(total_rows, total_cols):
    column_names = [f"col{i + 1}" for i in range(total_cols)]
    columns = [pa.array(np.random.randint(0, 50_000, size=total_rows)) for _ in range(total_cols)]
    return pa.table(columns, names=column_names)


# Pre-create a large table 1 million rows and 50 columns
TABLE = create_large_pyarrow_table(total_rows=1_000_000, total_cols=50)


@BaseDataService.register(MockDataParams)
class MockDataService(BaseDataService[MockDataParams]):
    async def aget_batches(
        self, params: MockDataParams, batch_size: int | None = None
    ) -> AsyncIterable[pa.RecordBatch]:
        async def gen():
            for b in TABLE.to_batches(params.rows_per_batch):
                # simulate an I/O wait time
                await asyncio.sleep(params.delay_per_row * params.rows_per_batch)
                yield b

        return gen()
