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


@BaseDataService.register(kind)
class MockDataService(BaseDataService[T]):
    async def aget_batches(self, params: T, batch_size: int | None = None) -> AsyncIterable[pa.RecordBatch]:
        table = create_pyarrow_table(params.nrows, params.ncols)
        for b in table.to_batches(params.batch_size or batch_size):
            yield b


# to generate the same data for the same dimension
np.random.seed(0)


def create_pyarrow_table(nrows: int, ncols: int) -> pa.Table:
    # Generate column names like 'col1', 'col2', ..., 'coln'
    column_names = [f"col{i + 1}" for i in range(ncols)]
    columns = [pa.array(np.random.randint(0, 50_000, size=nrows)) for _ in range(ncols)]
    table = pa.table(columns, names=column_names)
    return table
