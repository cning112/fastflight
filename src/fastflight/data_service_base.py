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

    kind: ClassVar[str]
    registry: ClassVar[dict[str, ParamsCls]] = {}

    @classmethod
    def register(cls):
        """
        Decorator for registering a DataParams subclass.

        This decorator registers a DataParams subclass in the global registry using its fully
        qualified name (i.e., "<module>.<qualname>") as the unique key. It also sets the 'kind'
        attribute of the subclass to this fully qualified name, ensuring a one-to-one binding
        between a DataParams subclass and its corresponding DataService.

        Returns:
            function: A decorator function that takes a DataParams subclass, registers it, and returns the subclass.

        Raises:
            ValueError: If a DataParams subclass with the same fully qualified name is already registered.
        """

        def inner(sub_params_cls: ParamsCls) -> ParamsCls:
            qual_name = sub_params_cls.qual_name()
            if qual_name in cls.registry:
                raise ValueError(f"Params type {qual_name=} is already registered by {cls.registry[qual_name]}.")
            setattr(sub_params_cls, "kind", qual_name)
            cls.registry[qual_name] = sub_params_cls
            logger.info(f"Registered params type {qual_name} for class {sub_params_cls}")
            return sub_params_cls

        return inner

    @classmethod
    def lookup(cls, qual_name: str) -> ParamsCls:
        """
        Get the params class associated with the given params type.

        Args:
            qual_name: The type of the params to retrieve.

        Returns:
            type[BaseParams]: The params class associated with the params type.

        Raises:
            ValueError: If the params type is not registered.
        """
        params_cls = cls.registry.get(qual_name)
        if params_cls is None:
            logger.error(f"Params type {qual_name} is not registered.")
            raise ValueError(f"Params type {qual_name} is not registered.")
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
            qual_name = json_data.pop("kind")
            params_cls = cls.lookup(qual_name)
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
            json_data["kind"] = self.qual_name()
            return json.dumps(json_data).encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing params: {e}")
            raise

    @classmethod
    def qual_name(cls):
        return f"{cls.__module__}.{cls.__qualname__}"


DataServiceCls = type["BaseDataService"]


class BaseDataService(Generic[T], ABC):
    """
    A base class for data sources, specifying the ticket type it handles,
    providing methods to fetch data and batches of data, and managing the
    registry for different data source types.
    """

    registry: ClassVar[dict[str, DataServiceCls]] = {}

    @classmethod
    def register(cls, params_cls: type[ParamsCls]):
        """
        Decorator for registering a DataService subclass for a given DataParams type.

        This method first registers the given DataParams type by calling BaseParams.register()(params_cls).
        It then acts as a decorator for a DataService subclass, registering the subclass in the global registry
        under two keys:
          - The fully qualified name of the DataParams type.
          - The fully qualified name of the DataService subclass.
        This dual-key registration enforces a one-to-one binding between a DataParams subclass and its
        corresponding DataService subclass.

        Args:
            params_cls (type[ParamsCls]): The DataParams subclass that the DataService handles.

        Returns:
            function: A decorator function that takes a DataService subclass, registers it, and returns the subclass.

        Raises:
            ValueError: If a DataService for the given DataParams type is already registered either under the
                        DataParams key or the DataService subclass's own fully qualified name.
        """
        BaseParams.register()(params_cls)

        def inner(subclass: DataServiceCls) -> DataServiceCls:
            cls_qual_name = f"{subclass.__module__}.{subclass.__qualname__}"
            params_qual_name = params_cls.qual_name()
            if params_qual_name in cls.registry:
                raise ValueError(
                    f"Data source type {params_qual_name} is already registered by {cls.registry[params_qual_name]}."
                )
            if cls_qual_name in cls.registry:
                raise ValueError(
                    f"Data source type {cls_qual_name} is already registered by {cls.registry[cls_qual_name]}."
                )
            cls.registry[params_qual_name] = cls.registry[cls_qual_name] = subclass
            logger.info(
                f"Registered data source type for class {subclass} with keys {params_qual_name} and {cls_qual_name}"
            )
            return subclass

        return inner

    @classmethod
    def lookup(cls, qual_name: str) -> DataServiceCls:
        """
        Get the data service class associated with the given data source type.

        Args:
            qual_name: The type of the data source to retrieve, it can be either the params type or the data source type.

        Returns:
            type[BaseDataService]: The data source class associated with the data source type.

        Raises:
            ValueError: If the data source type is not registered.
        """
        data_service_cls = cls.registry.get(qual_name)
        if data_service_cls is None:
            logger.error(f"Data source type {qual_name} is not registered.")
            raise ValueError(f"Data source type {qual_name} is not registered.")
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


def bind_service(params_cls: type[T], service_cls: type[BaseDataService[T]], *, alias: str | None = None) -> None:
    BaseParams.register()(params_cls)
