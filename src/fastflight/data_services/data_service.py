import json
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterable, Generic, Iterable, Type, TypeVar

import pyarrow as pa
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BaseParams(BaseModel, ABC):
    """A base class for query parameters used in data services.

    This class defines serialization and deserialization methods for handling
    parameters passed between clients and the FastFlight server.
    """

    @staticmethod
    @abstractmethod
    def default_service_class() -> Type["BaseDataService"]:
        """Return the default BaseDataService class associated with this parameter type."""
        pass

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

            # Extract the class name to determine the correct subclass
            params_class_name = json_data.pop("_params_class", None)
            if not params_class_name:
                raise ValueError("Missing `_params_class` in ticket, cannot determine correct parameter class.")

            # If called on BaseParams, dynamically resolve the subclass
            if cls is BaseParams:
                for subclass in cls.__subclasses__():
                    sub_fqn = f"{subclass.__module__}.{subclass.__name__}"
                    if sub_fqn == params_class_name:
                        return subclass.model_validate(json_data)
                raise ValueError(f"Unknown parameter class: {params_class_name}")
            else:
                return cls.model_validate(json_data)
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logger.error(f"Error deserializing parameters: {e}")
            raise

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


T = TypeVar("T", bound="BaseParams")


class BaseDataService(Generic[T], ABC):
    """A base class for data services, managing registration and data retrieval."""

    async def aget_batches(self, params: T, batch_size: int | None = None) -> AsyncIterable[pa.RecordBatch]:
        """Fetches data asynchronously in batches.

        Args:
            params (T): The parameters for fetching data.
            batch_size (int | None): The maximum size of each batch.

        Yields:
            pa.RecordBatch: A generator of RecordBatch instances.
        """
        raise NotImplementedError

    def get_batches(self, params: T, batch_size: int | None = None) -> Iterable[pa.RecordBatch]:
        """Fetches data synchronously in batches.

        Args:
            params (T): The parameters for fetching data.
            batch_size (int | None): The maximum size of each batch.

        Yields:
            pa.RecordBatch: A generator of RecordBatch instances.
        """
        raise NotImplementedError
