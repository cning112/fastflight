"""
Improved Mock Data Service - Clean Version

Core improvements:
1. Simplified async implementation (removed unnecessary queues)
2. Better logging and debugging information
3. Support for data variant testing
"""

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Iterable

import numpy as np
import pyarrow as pa

from fastflight.core.base import BaseDataService, BaseParams

logger = logging.getLogger(__name__)


class MockDataParams(BaseParams):
    rows_per_batch: int
    delay_per_row: float
    data_variant: str = "default"


class MockDataParamsAsync(BaseParams):
    rows_per_batch: int
    delay_per_row: float
    data_variant: str = "default"


# Fixed random seed
np.random.seed(42)


def create_benchmark_table(total_rows: int, total_cols: int, variant: str = "default") -> pa.Table:
    """Create benchmark table"""
    column_names = [f"col_{i}" for i in range(total_cols)]

    # Explicitly type the columns list to allow mixed array types
    columns = []

    if variant == "simple":
        columns = [pa.array(np.arange(total_rows, dtype=np.int32)) for _ in range(total_cols)]
    elif variant == "complex":
        for i in range(total_cols):
            if i % 3 == 0:
                columns.append(pa.array(np.random.randint(0, 1_000_000, size=total_rows, dtype=np.int64)))
            elif i % 3 == 1:
                columns.append(pa.array(np.random.normal(0, 1000, size=total_rows).astype(np.float64)))
            else:
                strings = [f"data_{j}_{np.random.randint(0, 10000)}" for j in range(total_rows)]
                columns.append(pa.array(strings))
    else:
        columns = [pa.array(np.random.randint(0, 50_000, size=total_rows, dtype=np.int32)) for _ in range(total_cols)]

    return pa.table(columns, names=column_names)  # type: ignore[arg-type]


# Pre-built data tables
TABLES = {
    # "default": create_benchmark_table(1_000_000, 50),
    # "simple": create_benchmark_table(1_000_000, 50, "simple"),
    # "complex": create_benchmark_table(500_000, 50, "complex"),
}


def prepare_static_data():
    logger.info("Preparing pre-built data...")
    TABLES["default"] = create_benchmark_table(1_000_000, 50)
    TABLES["simple"] = create_benchmark_table(1_000_000, 50, "simple")
    TABLES["complex"] = create_benchmark_table(500_000, 50, "complex")
    logger.info("Pre-built test data:")
    for variant, table in TABLES.items():
        size_mb = table.nbytes / (1024 * 1024)
        logger.info(f"  {variant}: {table.num_rows:,} rows x {table.num_columns} columns = {size_mb:.2f} MB")


class MockDataService(BaseDataService[MockDataParams]):
    """Synchronous data service"""

    def get_batches(self, params: MockDataParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        table = TABLES[params.data_variant]
        delay = params.delay_per_row * params.rows_per_batch

        for batch in table.to_batches(params.rows_per_batch):
            if delay > 0:
                time.sleep(delay)
            yield batch


class MockDataServiceAsync(BaseDataService[MockDataParamsAsync]):
    """Asynchronous data service - simplified version"""

    def get_batches(self, params: MockDataParamsAsync, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        """Synchronous implementation for compatibility"""
        table = TABLES[params.data_variant]
        delay = params.delay_per_row * params.rows_per_batch

        for batch in table.to_batches(params.rows_per_batch):
            if delay > 0:
                time.sleep(delay)
            yield batch

    async def aget_batches(
        self, params: MockDataParamsAsync, batch_size: int | None = None
    ) -> AsyncIterator[pa.RecordBatch]:
        table = TABLES[params.data_variant]
        delay = params.delay_per_row * params.rows_per_batch

        for batch in table.to_batches(params.rows_per_batch):
            if delay > 0:
                await asyncio.sleep(delay)
            yield batch
