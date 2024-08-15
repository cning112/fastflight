import pyarrow as pa
from pyarrow import Table

from .base_data_service import BaseDataService
from ..models.data_source import DataSource
from ..models.tickets import NoSQLQueryTicket

T = NoSQLQueryTicket


@BaseDataService.register(DataSource.NoSQL)
class NoSQLDataService(BaseDataService[T]):
    """
    A data source class for NoSQL queries.
    """

    async def get_table(self, params: T) -> Table:
        """
        Fetch the entire dataset for NoSQL queries based on the given parameters.

        Args:
            params (NoSQLQueryTicket): The parameters for fetching data.

        Returns:
            Table: The fetched data in the form of a PyArrow Table.
        """
        # Implement fetching logic for NoSQL
        data = ...
        return pa.Table.from_pandas(data)
