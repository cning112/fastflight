import json
import logging
from abc import ABC
from functools import partial
from typing import AsyncIterable, ClassVar, Generic, Iterable, Self, TypeVar, get_args, get_origin

import pyarrow as pa
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="BaseParams")
ParamsCls = type["BaseParams"]


class BaseParams(BaseModel, ABC):
    """
    A base class for query params, implementing common serialization methods
    and managing the registry for different params types.
    """

    registry: ClassVar[dict[str, ParamsCls]] = {}

    @classmethod
    def fqn(cls) -> str:
        return f"{cls.__module__}.{cls.__qualname__}"

    @classmethod
    def _register(cls, klass: ParamsCls) -> ParamsCls:
        """
        This registers a DataParams subclass using its fully qualified name ("<module>.<qualname>").
        It also sets the 'fqn' attribute of the subclass to the same fully qualified name.

        Args:
            klass (ParamsCls): The DataParams subclass to register.

        Returns:
            ParamsCls: The registered DataParams subclass.

        Raises:
            ValueError: If a DataParams subclass with the same fully qualified name is already registered.
        """
        if (fqn := klass.fqn()) in cls.registry:
            raise ValueError(f"Params type {fqn} is already registered by {cls.registry[fqn]}.")
        cls.registry[fqn] = klass
        logger.info(f"Registered params type {fqn} for class {klass}")
        return klass

    @classmethod
    def lookup(cls, fqn: str) -> ParamsCls:
        """
        Get the params class associated with the given params type.

        Args:
            fqn: The type of the params to retrieve.

        Returns:
            type[BaseParams]: The params class associated with the params type.

        Raises:
            ValueError: If the params type is not registered.
        """
        params_cls = cls.registry.get(fqn)
        if params_cls is None:
            logger.error(f"Params type {fqn} is not registered.")
            raise ValueError(f"Params type {fqn} is not registered.")
        return params_cls

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        """
        Deserialize a params from bytes which includes the fully qualified name.

        Args:
            data (bytes): The byte data to deserialize.

        Returns:
            BaseParams: The deserialized params object.
        """
        try:
            json_data = json.loads(data)
            fqn = json_data.pop("param_type")
            params_cls = cls.lookup(fqn)
            return params_cls.model_validate(json_data)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error deserializing params: {e}")
            raise

    def to_json(self) -> dict:
        """
        Serialize the params to json, including the fully qualified name.

        Returns:
            dict: The json representation of the params.
        """
        try:
            json_data = self.model_dump()
            json_data["param_type"] = self.__class__.fqn()
            return json_data
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing params: {e}")
            raise

    def to_bytes(self) -> bytes:
        return json.dumps(self.to_json()).encode()


DataServiceCls = type["BaseDataService"]


class BaseDataService(Generic[T], ABC):
    """
    A base class for data sources, specifying the ticket type it handles,
    providing methods to fetch data and batches of data, and managing the
    registry for different data source types.
    """

    _registry: ClassVar[dict[str, DataServiceCls]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Attempt to automatically register the parameter class and the service class
        # by inspecting the generic parameter to BaseDataService[ParamsCls]
        orig_bases = getattr(cls, "__orig_bases__", ())
        for base in orig_bases:
            origin = get_origin(base)
            if origin is BaseDataService:
                args = get_args(base)
                if args:
                    param_cls = args[0]
                    # Only auto-register if param_cls is a subclass of BaseParams
                    if isinstance(param_cls, type) and issubclass(param_cls, BaseParams):
                        try:
                            cls._register(param_cls, cls)
                        except ValueError as e:
                            logger.error(f"Automatic registration failed for {cls.fqn()}: {e}")
                break

    @classmethod
    def _register(cls, params_cls: ParamsCls, klass: DataServiceCls | None = None):
        """
        Register the given data params class and the data service class. Can be used as a decorator.

        This first registers the given DataParams type.
        It then a DataService subclass, registering the subclass in the global registry
        under two keys:
          - The fully qualified name of the DataParams type.
          - The fully qualified name of the DataService subclass.
        This dual-key registration enforces a one-to-one binding between a DataParams subclass and its
        corresponding DataService subclass.

        Args:
            params_cls (type[ParamsCls]): The DataParams subclass that the DataService handles.
            klass (type[DataService], optional): The DataService subclass to register.

        Raises:
            ValueError: If a DataService for the given DataParams type is already registered either under the
                        DataParams key or the DataService subclass's own fully qualified name.
        """
        if klass is None:
            return partial(cls._register, params_cls)

        # Register the DataParams class and add the `fqn` attribute to it
        BaseParams._register(params_cls)

        # Register the DataService subclass
        param_cls_fqn = params_cls.fqn()
        if ex := cls._registry.get(param_cls_fqn):
            raise ValueError(f"{param_cls_fqn} is already registered with {ex.fqn()}.")
        cls._registry[param_cls_fqn] = klass
        logger.info(f"Registered data service class {klass.fqn()} for params type {param_cls_fqn}")
        return klass

    @classmethod
    def lookup(cls, params_cls_fqn: str) -> DataServiceCls:
        """
        Get the data service class associated with the given data source type.

        Args:
            params_cls_fqn: The fqn of the data params class

        Returns:
            type[BaseDataService]: The data source class associated with the data source type.

        Raises:
            ValueError: If the data source type is not registered.
        """
        data_service_cls = cls._registry.get(params_cls_fqn)
        if data_service_cls is None:
            logger.error(f"Data source type {params_cls_fqn} is not registered.")
            raise ValueError(f"Data source type {params_cls_fqn} is not registered.")
        return data_service_cls

    @classmethod
    def fqn(cls):
        return f"{cls.__module__}.{cls.__qualname__}"

    async def aget_batches(self, params: T, batch_size: int | None = None) -> AsyncIterable[pa.RecordBatch]:
        """
        Fetch data in batches asynchronously based on the given parameters.

        This method wraps the blocking get_batches method in a thread executor to allow asynchronous usage.

        Args:
            params (T): The parameters for fetching data.
            batch_size: The maximum size of each batch. Defaults to None to be decided by the data service implementation.

        Yields:
            AsyncIterable[pa.RecordBatch]: An async iterable of RecordBatches.

        """
        raise NotImplementedError

    def get_batches(self, params: T, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        """Fetches data synchronously in batches.

        Args:
            params (T): The parameters for fetching data.
            batch_size: The maximum size of each batch. Defaults to None to be decided by the data service implementation.

        Yields:
            pa.RecordBatch: A generator of RecordBatch instances.
        """
        raise NotImplementedError
