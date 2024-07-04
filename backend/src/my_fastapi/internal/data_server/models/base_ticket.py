import json
import logging
from abc import ABC
from typing import ClassVar, TypeVar

from pydantic import BaseModel

from .data_source import DataSource

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="BaseTicket")
TC = type["BaseTicket"]


class BaseTicket(BaseModel, ABC):
    """
    A base class for tickets, implementing common serialization methods
    and managing the registry for different ticket types.
    """

    kind: ClassVar[DataSource]
    registry: ClassVar[dict[DataSource, TC]] = {}

    @classmethod
    def register(cls, kind: DataSource):
        """
        Register a ticket type with its corresponding class.

        Args:
            kind (DataSource): The type of the ticket to register.
        """

        def inner(subclass: TC) -> TC:
            if kind in cls.registry:
                raise ValueError(f"Ticket type {kind} is already registered by {cls.registry[kind].__qualname__}.")
            setattr(subclass, "kind", kind)
            cls.registry[kind] = subclass
            logger.info(f"Registered ticket type {kind} for class {subclass.__qualname__}")
            return subclass

        return inner

    @classmethod
    def get_ticket_cls(cls, kind: DataSource) -> TC:
        """
        Get the ticket class associated with the given ticket type.

        Args:
            kind (DataSource): The type of the ticket to retrieve.

        Returns:
            type[BaseTicket]: The ticket class associated with the ticket type.

        Raises:
            ValueError: If the ticket type is not registered.
        """
        ticket_cls = cls.registry.get(kind)
        if ticket_cls is None:
            logger.error(f"Ticket type {kind} is not registered.")
            raise ValueError(f"Ticket type {kind} is not registered.")
        return ticket_cls

    @classmethod
    def from_bytes(cls, data: bytes) -> T:
        """
        Deserialize a ticket from bytes.

        Args:
            data (bytes): The byte data to deserialize.

        Returns:
            BaseTicket: The deserialized ticket object.
        """
        try:
            json_data = json.loads(data)
            ticket_type = DataSource(json_data.pop("kind"))
            ticket_cls = cls.get_ticket_cls(ticket_type)
            return ticket_cls.model_validate(json_data)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error deserializing ticket: {e}")
            raise

    def to_bytes(self) -> bytes:
        """
        Serialize the ticket to bytes.

        Returns:
            bytes: The serialized byte data of the ticket.
        """
        try:
            json_data = self.model_dump()
            json_data["kind"] = self.kind.value
            return json.dumps(json_data).encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.error(f"Error serializing ticket: {e}")
            raise
