from typing import AsyncIterable

import duckdb
import pyarrow as pa

from fastflight.data_service_base import BaseDataService, BaseParams


class DuckDbAParquetParams(BaseParams):
    sql: str


@BaseDataService.register(DuckDbAParquetParams)
class DuckDbParquetService(BaseDataService[DuckDbAParquetParams]):
    async def aget_batches(
        self, params: DuckDbAParquetParams, batch_size: int | None = None
    ) -> AsyncIterable[pa.RecordBatch]:
        for batch in duckdb.sql(params.sql).fetch_record_batch(batch_size):
            yield batch
