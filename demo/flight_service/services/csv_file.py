import pandas as pd
import pyarrow as pa

from demo.flight_service.models.data_kinds import DataKind
from demo.flight_service.models.params import CsvFileParams
from fastflight.services.base_data_service import BaseDataService

T = CsvFileParams


@BaseDataService.register(DataKind.CSV)
class CsvFileService(BaseDataService[T]):
    async def aget_pa_table(self, params: T) -> pa.Table:
        if not (resolved := params.path.resolve()).exists():
            raise ValueError(f"File {resolved} does not exist.")
        return pa.Table.from_pandas(pd.read_csv(resolved))
