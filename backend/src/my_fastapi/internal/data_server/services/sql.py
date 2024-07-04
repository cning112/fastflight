import pyarrow as pa
from pyarrow import Table

from .base_data_service import BaseDataService
from ..models.data_source import DataSource
from ..models.tickets import SQLQueryTicket

T = SQLQueryTicket


@BaseDataService.register(DataSource.SQL)
class SQLDataService(BaseDataService[T]):
    """
    A data source class for SQL queries.
    """

    async def get_table(self, params: T) -> Table:
        """
        Fetch the entire dataset for SQL queries based on the given parameters.

        Args:
            params (SQLQueryTicket): The parameters for fetching data.

        Returns:
            Table: The fetched data in the form of a PyArrow Table.
        """
        # Implement fetching logic for SQL
        data = ...
        return pa.Table.from_pandas(data)
