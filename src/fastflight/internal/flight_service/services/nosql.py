import pyarrow as pa

from ..models.data_source_kind import DataSourceKind
from ..models.params import NoSqlParams
from .base_data_service import BaseDataService

T = NoSqlParams


@BaseDataService.register(DataSourceKind.NoSQL)
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
