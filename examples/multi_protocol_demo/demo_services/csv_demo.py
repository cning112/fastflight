import asyncio
from collections.abc import AsyncIterator, Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as csv
from pydantic import Field, field_serializer, field_validator

from fastflight.core.base import BaseDataService, BaseParams


class CsvFileParams(BaseParams):
    path: Path = Field(...)

    @field_serializer("path")
    def serialize_path(self, path: Path) -> str:
        return str(path)

    @field_validator("path", mode="before")
    @classmethod
    def parse_path(cls, v: str) -> Path:
        return Path(v)


# Synchronous version
class CsvFileService(BaseDataService[CsvFileParams]):
    def get_batches(self, params: CsvFileParams, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        with csv.open_csv(params.path, read_options=csv.ReadOptions(block_size=batch_size)) as reader:
            while True:
                try:
                    batch = reader.read_next_batch()
                except StopIteration:
                    break
                if batch.num_rows == 0:
                    break
                yield batch


class CsvFileParamsAsync(BaseParams):
    path: Path = Field(...)


# Async version
class CsvFileServiceAsync(BaseDataService[CsvFileParamsAsync]):
    async def aget_batches(
        self, params: CsvFileParamsAsync, batch_size: int | None = None
    ) -> AsyncIterator[pa.RecordBatch]:
        queue: asyncio.Queue[pa.RecordBatch | None] = asyncio.Queue()

        async def produce():
            for batch in self.get_batches(params, batch_size):
                await queue.put(batch)
            await queue.put(None)

        # Store task reference to prevent garbage collection
        _task = asyncio.create_task(produce())
        # Reference kept to prevent garbage collection during execution
        _ = _task

        while True:
            batch = await queue.get()
            if batch is None:
                break
            yield batch
