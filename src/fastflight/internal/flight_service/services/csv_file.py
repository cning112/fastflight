import pandas as pd
import pyarrow as pa

from ..models.data_source_kind import DataSourceKind
from ..models.params import CsvFileParams
from ..services.base_data_service import BaseDataService

T = CsvFileParams


@BaseDataService.register(kind=DataSourceKind.CSV)
class CsvFileService(BaseDataService[T]):
    async def aget_pa_table(self, params: T) -> pa.Table:
        if not (resolved := params.path.resolve()).exists():
            raise ValueError(f"File {resolved} does not exist.")
        return pa.Table.from_pandas(pd.read_csv(resolved))
