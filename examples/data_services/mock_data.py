import logging
from typing import AsyncIterable

import numpy as np
import pyarrow as pa

from fastflight.data_service_base import BaseDataService, BaseParams

logger = logging.getLogger(__name__)

kind = "MockData"


@BaseParams.register(kind)
class MockDataParams(BaseParams):
    nrows: int
    ncols: int
    batch_size: int


T = MockDataParams

# to generate the same data for the same dimension
np.random.seed(0)


# Pre-create a large pyarrow table
def create_large_pyarrow_table(total_rows, total_cols):
    column_names = [f"col{i + 1}" for i in range(total_cols)]
    columns = [pa.array(np.random.randint(0, 50_000, size=total_rows)) for _ in range(total_cols)]
    return pa.table(columns, names=column_names)


# Function to return a slice of the large table
def get_table_slice(table, rows, cols):
    # Slice rows and columns
    sliced_table = table.slice(0, rows).select([f"col{i + 1}" for i in range(cols)])
    return sliced_table


# Pre-create a large table (for example, 1 million rows and 100 columns)
TABLE = create_large_pyarrow_table(total_rows=10_000_000, total_cols=200)


@BaseDataService.register(kind)
class MockDataService(BaseDataService[T]):
    async def aget_batches(self, params: T, batch_size: int | None = None) -> AsyncIterable[pa.RecordBatch]:
        table = get_table_slice(TABLE, params.nrows, params.ncols)
        for b in table.to_batches(params.batch_size or batch_size):
            yield b
