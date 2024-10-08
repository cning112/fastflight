import json
import logging
from abc import ABC, abstractmethod
from typing import AsyncIterable, ClassVar, Generic, TypeAlias, TypeVar

import pyarrow as pa
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="BaseParams")
ParamsCls: TypeAlias = type["BaseParams"]


def to_name(kind: any) -> str:
    return str(kind)


class BaseParams(BaseModel, ABC):
    """
    A base class for query params, implementing common serialization methods
    and managing the registry for different params types.
    """

    kind: ClassVar
    registry: ClassVar[dict[str, ParamsCls]] = {}

    @classmethod
    def register(cls, kind):
        """
        Register a data source type with the corresponding params class.

        Args:
            kind: The type of the params to register.
        """
        kind_str = to_name(kind)

        def inner(sub_params_cls: ParamsCls) -> ParamsCls:
            if kind_str in cls.registry:
                raise ValueError(
                    f"Params type {kind_str} is already registered by {cls.registry[kind_str].__qualname__}."
                )
            setattr(sub_params_cls, "kind", kind)
            cls.registry[kind_str] = sub_params_cls
            logger.info(f"Registered params type {kind_str} for class {sub_params_cls.__qualname__}")
            return sub_params_cls

        return inner

    @classmethod
    def lookup(cls, kind) -> ParamsCls:
        """
        Get the params class associated with the given params type.

        Args:
            kind: The type of the params to retrieve.

        Returns:
            type[BaseParams]: The params class associated with the params type.

        Raises:
            ValueError: If the params type is not registered.
        """
        kind_str = to_name(kind)
        params_cls = cls.registry.get(kind_str)
        if params_cls is None:
            logger.error(f"Params type {kind_str} is not registered.")
            raise ValueError(f"Params type {kind_str} is not registered.")
        return params_cls

    @classmethod
    def from_bytes(cls, data: bytes) -> T:
        """
        Deserialize a params from bytes.

        Args:
            data (bytes): The byte data to deserialize.

        Returns:
            BaseParams: The deserialized params object.
        """
        try:
            json_data = json.loads(data)
            kind_str = json_data.pop("kind")
            params_cls = cls.lookup(kind_str)
            return params_cls.model_validate(json_data)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error deserializing params: {e}")
            raise

    def to_bytes(self) -> bytes:
        """
        Serialize the params to bytes.

        Returns:
            bytes: The serialized byte data of the params.
        """
        try:
            json_data = self.model_dump()
            json_data["kind"] = to_name(self.kind)
            return json.dumps(json_data).encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing params: {e}")
            raise


DataServiceCls = type["BaseDataService"]


class BaseDataService(Generic[T], ABC):
    """
    A base class for data sources, specifying the ticket type it handles,
    providing methods to fetch data and batches of data, and managing the
    registry for different data source types.
    """

    registry: ClassVar[dict[str, DataServiceCls]] = {}

    @classmethod
    def register(cls, kind):
        """
        Register a data source type with its corresponding class.

        Args:
            kind: The type of the data source to register.

        Returns:
            type[BaseDataService]: The registered data source class.
        """
        kind_str = to_name(kind)

        def inner(subclass: DataServiceCls) -> DataServiceCls:
            if kind_str in cls.registry:
                raise ValueError(
                    f"Data source type {kind_str} is already registered by {cls.registry[kind_str].__qualname__}."
                )
            cls.registry[kind_str] = subclass
            logger.info(f"Registered data source type {kind_str} for class {subclass.__qualname__}")
            return subclass

        return inner

    @classmethod
    def lookup(cls, kind) -> DataServiceCls:
        """
        Get the data service class associated with the given data source type.

        Args:
            kind: The type of the data source to retrieve.

        Returns:
            type[BaseDataService]: The data source class associated with the data source type.

        Raises:
            ValueError: If the data source type is not registered.
        """
        kind_str = to_name(kind)
        data_service_cls = cls.registry.get(kind_str)
        if data_service_cls is None:
            logger.error(f"Data source type {kind_str} is not registered.")
            raise ValueError(f"Data source type {kind_str} is not registered.")
        return data_service_cls

    @abstractmethod
    async def aget_batches(self, params: T, batch_size: int | None = None) -> AsyncIterable[pa.RecordBatch]:
        """
        Fetch data in batches based on the given parameters.

        Args:
            params (T): The parameters for fetching data.
            batch_size: The maximum size of each batch. Defaults to None to be decided by the data service implementation.

        Yields:
            pa.RecordBatch: An async iterable of RecordBatches.

        """
        raise NotImplementedError
