import json
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterable, ClassVar, Dict, Generic, Literal, Type, TypeVar

import pyarrow as pa
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BaseParams(BaseModel, ABC):
    """A base class for query parameters used in data services.

    This class defines serialization and deserialization methods for handling
    parameters passed between clients and the FastFlight server.
    """

    data_type: ClassVar[str]  # The unique identifier for this parameter type

    @classmethod
    def from_bytes(cls, data: bytes) -> "BaseParams":
        """Deserializes a `BaseParams` instance from a bytes object.

        It looks up the class using `data_type`, which represents the primary identifier.

        Args:
            data (bytes): The byte representation of a `BaseParams` object.

        Returns:
            BaseParams: The deserialized instance.

        Raises:
            ValueError: If the parameter class is not found using either `data_type` or `alias`.
            JSONDecodeError, KeyError, TypeError: If deserialization fails.
        """
        try:
            json_data = json.loads(data)
            data_type = json_data.pop("data_type", None)
            params_cls = RegistryManager.lookup_params(data_type)
            if not params_cls:
                raise ValueError(f"Parameter class not found: data_type={data_type}")
            return params_cls.model_validate(json_data)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error deserializing parameters: {e}")
            raise

    def to_json(self) -> dict[str, Any]:
        """Serializes the `BaseParams` instance into a JSON-compatible dictionary.

        Returns:
            dict[str, Any]: The serialized dictionary representation.

        Raises:
            ValueError, TypeError: If serialization fails.
        """
        try:
            json_data = self.model_dump()
            json_data["data_type"] = self.data_type
            return json_data
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing parameters: {e}")
            raise

    def to_bytes(self) -> bytes:
        """Serializes the `BaseParams` instance into a bytes object.

        Returns:
            bytes: The serialized representation of the object.

        Raises:
            ValueError, TypeError: If serialization fails.
        """
        try:
            return json.dumps(self.to_json()).encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing parameters: {e}")
            raise

    @classmethod
    def qual_name(cls) -> str:
        """Returns the fully qualified name of the class."""
        return f"{cls.__module__}.{cls.__qualname__}"


T = TypeVar("T", bound="BaseParams")


class BaseDataService(Generic[T], ABC):
    """A base class for data services, managing registration and data retrieval."""

    @abstractmethod
    async def aget_batches(self, params: T, batch_size: int | None = None) -> AsyncIterable[pa.RecordBatch]:
        """Fetches data asynchronously in batches.

        Args:
            params (T): The parameters for fetching data.
            batch_size (int | None): The maximum size of each batch.

        Yields:
            pa.RecordBatch: A generator of RecordBatch instances.
        """
        raise NotImplementedError


LookupType = Literal["data_type", "qual_name", "auto"]


class RegistryManager:
    """Manages the registration, alias mapping, and dynamic loading of BaseParams and BaseDataService."""

    _params_registry: Dict[str, Type[BaseParams]] = {}  # Maps qual_name -> params_cls
    _service_registry: Dict[str, Type[BaseDataService]] = {}  # Maps qual_name -> service_cls
    _data_type_registry: Dict[str, str] = {}  # Maps data_type -> qual_name

    @classmethod
    def register_service(cls, params_cls: Type[BaseParams], service_cls: Type[BaseDataService]):
        """Registers a BaseDataService subclass and its corresponding BaseParams subclass.

        Args:
            params_cls (Type[BaseParams]): The parameter class that the service will handle.
            service_cls (Type[BaseDataService]): The data service class to register.

        Raises:
            ValueError: If the service is already registered for the given parameter type.
        """
        qual_name = params_cls.qual_name()

        # Ensure data_type is stored separately
        cls._data_type_registry[params_cls.data_type] = qual_name

        # Register the params class
        cls._params_registry[qual_name] = params_cls

        # Register the service class
        if existing_registered := cls._service_registry.get(qual_name):
            if existing_registered == service_cls:
                logger.info("Data service already registered for Params: %s", qual_name)
            else:
                raise ValueError(f"Data service already registered for {qual_name}")
        cls._service_registry[qual_name] = service_cls
        logger.info(
            f"Registered Data Service: {service_cls.__name__} for Params: {qual_name} (data_type: {params_cls.data_type})"
        )

    @classmethod
    def lookup_service(cls, query: str, lookup_type: LookupType = "auto") -> Type[BaseDataService] | None:
        """Finds a registered data service class by data_type, or qual_name.

        Args:
            query (str): The value to look up (data_type, or qual_name).
            lookup_type (str): The type of lookup ("data_type", "qual_name", or "auto").

        Returns:
            Type[BaseDataService] | None: The registered data service class, or None if not found.
        """
        if lookup_type == "data_type":
            if qual_name := cls._data_type_registry.get(query):
                return cls._service_registry.get(qual_name)
            else:
                return None
        elif lookup_type == "qual_name":
            return cls._service_registry.get(query)
        elif lookup_type == "auto":
            return cls.lookup_service(query, "data_type") or cls.lookup_service(query, "qual_name")
        else:
            raise ValueError(f"Invalid lookup_type: {lookup_type}")

    @classmethod
    def lookup_params(cls, query: str, lookup_type: LookupType = "auto") -> Type[BaseParams] | None:
        """Finds a registered BaseParams class by data_type, or qual_name.

        Args:
            query (str): The value to look up (data_type, or qual_name).
            lookup_type (str): The type of lookup ("data_type", "qual_name", or "auto").

        Returns:
            Type[BaseParams] | None: The registered BaseParams class, or None if not found.
        """
        if lookup_type == "data_type":
            if qual_name := cls._data_type_registry.get(query):
                return cls._params_registry.get(qual_name)
            else:
                return None
        elif lookup_type == "qual_name":
            return cls._params_registry.get(query)
        elif lookup_type == "auto":
            return cls.lookup_params(query, "data_type") or cls.lookup_params(query, "qual_name")
        else:
            raise ValueError(f"Invalid lookup_type: {lookup_type}")

    @classmethod
    def unregister_service(cls, name: str, lookup_type: LookupType = "auto"):
        """Unregisters a BaseDataService class and cleans up associated mappings."""
        params_cls = cls.lookup_params(name, lookup_type=lookup_type)
        if not params_cls:
            logger.warning(f"Attempted to unregister non-existent Data Service: {name}")
            return

        qual_name = params_cls.qual_name()

        # Remove any data_type mappings associated with this qual_name
        cls._data_type_registry = {k: v for k, v in cls._data_type_registry.items() if v != qual_name}

        if qual_name in cls._service_registry:
            del cls._service_registry[qual_name]
            logger.info(f"Unregistered Data Service: {qual_name}")

        if qual_name in cls._params_registry:
            cls._params_registry.pop(qual_name, None)
            logger.info(f"Unregistered Params: {qual_name}")

    @classmethod
    def unregister(cls, name: str, lookup_type: LookupType = "auto"):
        """Unregisters both BaseParams and BaseDataService."""
        cls.unregister_service(name, lookup_type)
