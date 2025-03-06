import json
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterable, ClassVar, Dict, Generic, Type, TypeAlias, TypeVar, cast

import pyarrow as pa
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="BaseParams")
ParamsCls: TypeAlias = type["BaseParams"]


class BaseParams(BaseModel, ABC):
    """
    A base class for query params, implementing common serialization methods
    and managing the registry for different params types.
    """

    kind: ClassVar[str]

    @classmethod
    def from_bytes(cls: Type[T], data: bytes) -> T:
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
            return cast(T, params_cls.model_validate(json_data))
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
            return json.dumps(self.to_json()).encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing params: {e}")
            raise

    def to_json(self) -> dict[str, Any]:
        try:
            json_data = self.model_dump()
            json_data["kind"] = self.qual_name()
            return json_data
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing params: {e}")
            raise

    @classmethod
    def qual_name(cls):
        return f"{cls.__module__}.{cls.__qualname__}"


DataServiceCls: TypeAlias = Type["BaseDataService[Any]"]


class BaseDataService(Generic[T], ABC):
    """
    A base class for data sources, specifying the ticket type it handles,
    providing methods to fetch data and batches of data, and managing the
    registry for different data source types.
    """

    registry: ClassVar[dict[str, DataServiceCls]] = {}
    params_registry: ClassVar[dict[str, ParamsCls]] = {}

    @classmethod
    def register(cls, params_cls: ParamsCls):
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

        # Register the DataParams type
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
            pa.RecordBatch: An async generator of pa.RecordBatches.

        """
        raise NotImplementedError


def bind_service(params_cls: type[T], service_cls: type[BaseDataService[T]], *, alias: str | None = None) -> None:
    BaseParams.register()(params_cls)


class RegistryManager:
    """管理 BaseParams 和 BaseDataService 的注册、别名映射，以及动态加载"""

    _params_registry: Dict[str, Type[BaseParams]] = {}
    _service_registry: Dict[str, Type[BaseDataService]] = {}
    _alias_registry: Dict[str, str] = {}  # 存储 alias -> qual_name 的映射

    @classmethod
    def register_params(cls, params_cls: Type[BaseParams], alias: str | None = None):
        """注册 BaseParams 子类，并可选绑定别名"""
        qual_name = f"{params_cls.__module__}.{params_cls.__qualname__}"
        cls._params_registry[qual_name] = params_cls
        if alias:
            cls._alias_registry[alias] = qual_name
        logger.info(f"Registered Params: {qual_name} (Alias: {alias})")

    @classmethod
    def register_service(
        cls, params_cls: Type[BaseParams], service_cls: Type[BaseDataService], alias: str | None = None
    ):
        """注册 BaseDataService，并可选绑定别名"""
        params_qual_name = f"{params_cls.__module__}.{params_cls.__qualname__}"
        service_qual_name = f"{service_cls.__module__}.{service_cls.__qualname__}"

        if params_qual_name in cls._service_registry:
            raise ValueError(f"Data service for {params_qual_name} is already registered.")

        cls._service_registry[params_qual_name] = service_cls
        if alias:
            cls._alias_registry[alias] = params_qual_name  # 绑定别名到参数类
        logger.info(f"Registered Data Service: {service_qual_name} for Params: {params_qual_name} (Alias: {alias})")

    @classmethod
    def lookup_params(cls, name: str) -> Type[BaseParams] | None:
        """查找 BaseParams 类，支持 FQN 和别名"""
        qual_name = cls._alias_registry.get(name, name)
        return cls._params_registry.get(qual_name)

    @classmethod
    def lookup_service(cls, name: str) -> Type[BaseDataService] | None:
        """查找 BaseDataService 类，支持 FQN 和别名"""
        qual_name = cls._alias_registry.get(name, name)
        return cls._service_registry.get(qual_name)

    @classmethod
    def register_alias(cls, alias: str, qual_name: str):
        """绑定新的别名"""
        cls._alias_registry[alias] = qual_name
        logger.info(f"Registered alias '{alias}' for '{qual_name}'")

    @classmethod
    def unregister_alias(cls, alias: str):
        """取消别名绑定"""
        if alias in cls._alias_registry:
            del cls._alias_registry[alias]
            logger.info(f"Unregistered alias '{alias}'")
        else:
            logger.warning(f"Tried to unregister alias '{alias}', but it was not registered.")

    @classmethod
    def unregister(cls, name: str):
        """同时取消注册 Params、Service 和别名"""
        # qual_name = cls._alias_registry.get(name, name)
        # cls.unregister_service(qual_name)
        # cls.unregister_params(qual_name)
        cls.unregister_alias(name)
