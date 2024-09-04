import logging
from abc import ABC, abstractmethod
from typing import ClassVar, Generic, TypeVar

import pyarrow as pa

from fastflight.services.base_params import BaseParams

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseParams)
DataServiceCls = type["BaseDataService"]


class BaseDataService(Generic[T], ABC):
    """
    A base class for data sources, specifying the ticket type it handles,
    providing methods to fetch data and batches of data, and managing the
    registry for different data source types.
    """

    registry: ClassVar[dict[str, DataServiceCls]] = {}

    @classmethod
    def register(cls, kind: str):
        """
        Register a data source type with its corresponding class.

        Args:
            kind (str): The type of the data source to register.

        Returns:
            type[BaseDataService]: The registered data source class.
        """

        def inner(subclass: DataServiceCls) -> DataServiceCls:
            if kind in cls.registry:
                raise ValueError(f"Data source type {kind} is already registered by {cls.registry[kind].__qualname__}.")
            cls.registry[kind] = subclass
            logger.info(f"Registered data source type {kind} for class {subclass.__qualname__}")
            return subclass

        return inner

    @classmethod
    def get_data_service_cls(cls, kind: str) -> DataServiceCls:
        """
        Get the data service class associated with the given data source type.

        Args:
            kind (str): The type of the data source to retrieve.

        Returns:
            type[BaseDataService]: The data source class associated with the data source type.

        Raises:
            ValueError: If the data source type is not registered.
        """
        data_service_cls = cls.registry.get(kind)
        if data_service_cls is None:
            logger.error(f"Data source type {kind} is not registered.")
            raise ValueError(f"Data source type {kind} is not registered.")
        return data_service_cls

    @abstractmethod
    async def aget_pa_table(self, params: T) -> pa.Table:
        """
        Fetch the entire dataset based on the given parameters.

        Args:
            params (T): The parameters for fetching data.

        Returns:
            Table: The fetched data in the form of a PyArrow Table.
        """
        raise NotImplementedError

    async def aget_reader(self, params: T, batch_size: int = 100) -> pa.RecordBatchReader:
        """
        Create a RecordBatchReader to read data in batches based on the given parameters.

        Args:
            params (T): The parameters for fetching data.
            batch_size (int): The maximum size of each batch. Defaults to 100.

        Returns:
            RecordBatchReader: A RecordBatchReader instance to read the data in batches.

        Raises:
            Exception: If there is an error in creating the batch reader.
        """
        try:
            table = await self.aget_pa_table(params)
            batches = table.to_batches(max_chunksize=batch_size)
            return pa.RecordBatchReader.from_batches(table.schema, batches)
        except Exception as e:
            logger.error(f"Error fetching batches: {e}")
            raise
