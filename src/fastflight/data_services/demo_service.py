import asyncio
import logging
from typing import AsyncIterable

import pandas as pd
import pyarrow as pa

from fastflight.data_service_base import BaseDataService, BaseParams

logger = logging.getLogger(__name__)


@BaseParams.register("Demo")
class DemoParams(BaseParams):
    value: int


T = DemoParams


@BaseDataService.register("Demo")
class DemoDataService(BaseDataService[T]):
    async def aget_batches(self, params: T, batch_size: int = 100) -> AsyncIterable[pa.RecordBatch]:
        for i in range(params.value):
            df = pd.DataFrame(data={"a": [i], "b": [i]})
            t = pa.Table.from_pandas(df)
            for b in t.to_batches():
                await asyncio.sleep(1)
                yield b
