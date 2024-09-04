import logging
import sqlite3

import pandas as pd
import pyarrow as pa

from demo.flight_service.models.data_kinds import DataKind
from demo.flight_service.models.params import SqlParams
from fastflight.services.base_data_service import BaseDataService

logger = logging.getLogger(__name__)

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
        logger.debug("Received SqlParams %s", params.query)
        with sqlite3.connect(":memory:") as conn:
            # Implement fetching logic for SQL
            df = pd.read_sql_query(params.query, conn)
            return pa.Table.from_pandas(df)
