import pyarrow as pa

from demo.flight_service.models.data_kinds import DataKind
from demo.flight_service.models.params import NoSqlParams
from fastflight.services.base_data_service import BaseDataService

T = NoSqlParams


@BaseDataService.register(DataKind.NO_SQL)
class NoSQLDataService(BaseDataService[T]):
    """
    A data source class for NoSQL queries.
    """

    async def aget_pa_table(self, params: T) -> pa.Table:
        """
        Fetch the entire dataset for NoSQL queries based on the given parameters.

        Args:
            params (NoSqlParams): The parameters for fetching data.

        Returns:
            Table: The fetched data in the form of a PyArrow Table.
        """
        # Implement fetching logic for NoSQL
        data = ...
        return pa.Table.from_pandas(data)
