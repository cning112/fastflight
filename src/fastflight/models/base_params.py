import json
import logging
from abc import ABC
from typing import ClassVar, TypeAlias, TypeVar

from pydantic import BaseModel

from .data_source_kind import DataSourceKind

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="BaseParams")
ParamsCls: TypeAlias = type["BaseParams"]


class BaseParams(BaseModel, ABC):
    """
    A base class for query params, implementing common serialization methods
    and managing the registry for different params types.
    """

    kind: ClassVar[DataSourceKind]
    registry: ClassVar[dict[DataSourceKind, ParamsCls]] = {}

    @classmethod
    def register(cls, kind: DataSourceKind):
        """
        Register a data source type with the corresponding params class.

        Args:
            kind (DataSourceKind): The type of the params to register.
        """

        def inner(sub_params_cls: ParamsCls) -> ParamsCls:
            if kind in cls.registry:
                raise ValueError(f"Params type {kind} is already registered by {cls.registry[kind].__qualname__}.")
            setattr(sub_params_cls, "kind", kind)
            cls.registry[kind] = sub_params_cls
            logger.info(f"Registered params type {kind} for class {sub_params_cls.__qualname__}")
            return sub_params_cls

        return inner

    @classmethod
    def get_params_cls(cls, kind: DataSourceKind) -> ParamsCls:
        """
        Get the params class associated with the given params type.

        Args:
            kind (DataSourceKind): The type of the params to retrieve.

        Returns:
            type[BaseParams]: The params class associated with the params type.

        Raises:
            ValueError: If the params type is not registered.
        """
        params_cls = cls.registry.get(kind)
        if params_cls is None:
            logger.error(f"Params type {kind} is not registered.")
            raise ValueError(f"Params type {kind} is not registered.")
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
            params_type = DataSourceKind(json_data.pop("kind"))
            params_cls = cls.get_params_cls(params_type)
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
            json_data["kind"] = self.kind.value
            return json.dumps(json_data).encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing params: {e}")
            raise
