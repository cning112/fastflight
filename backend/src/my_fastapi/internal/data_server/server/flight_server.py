import logging

from pyarrow import flight

from ..models.base_ticket import BaseTicket
from ..services.base_data_service import BaseDataService

logger = logging.getLogger(__name__)


class FlightServer(flight.FlightServerBase):
    def __init__(self, location: str):
        super().__init__(location)

    @staticmethod
    def get_ticket_and_data_service(ticket_bytes: bytes) -> tuple[BaseTicket, BaseDataService]:
        """
        Helper method to parse the ticket and get the corresponding data source instance.

        Args:
            ticket_bytes (bytes): The raw ticket bytes.

        Returns:
            tuple: A tuple containing the parsed ticket and data source instance.
        """
        actual_ticket = BaseTicket.from_bytes(ticket_bytes)
        ticket_type = actual_ticket.kind

        try:
            data_source_cls = BaseDataService.get_data_service_cls(ticket_type)
            data_source = data_source_cls()
            return actual_ticket, data_source
        except ValueError as e:
            logger.error(f"Data service unavailable for ticket type {ticket_type}: {e}")
            raise flight.FlightUnavailableError(f"Data service unavailable: {e}")
        except Exception as e:
            logger.error(f"Error getting data source for ticket type {ticket_type}: {e}")
            raise

    def do_get(self, context, ticket: flight.Ticket) -> flight.RecordBatchStream:
        try:
            actual_ticket, data_service = self.get_ticket_and_data_service(ticket.ticket)
            batch_reader = data_service.create_batch_reader(actual_ticket, batch_size=512)
            return flight.RecordBatchStream(batch_reader)
        except flight.FlightUnavailableError as e:
            raise e
        except Exception as e:
            logger.error(f"Internal server error: {e}")
            raise flight.FlightInternalError(f"Internal server error: {e}")
