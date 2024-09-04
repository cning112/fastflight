import pandas as pd
import pyarrow as pa

from demo.flight_service.models.data_kinds import DataKind
from demo.flight_service.models.params import SqlParams
from fastflight.services.base_data_service import BaseDataService

T = SqlParams


@BaseDataService.register(DataKind.SQL)
class SQLDataService(BaseDataService[T]):
    """
    A data source class for SQL queries.
    """

    async def aget_pa_table(self, params: T) -> pa.Table:
        """
        Fetch the entire dataset for SQL queries based on the given parameters.

        Args:
            params (SqlParams): The parameters for fetching data.

        Returns:
            Table: The fetched data in the form of a PyArrow Table.
        """
        # Implement fetching logic for SQL
        data = pd.DataFrame(data={"a": [1, 2, 3], "b": [10, 20, 30]})
        return pa.Table.from_pandas(data)
