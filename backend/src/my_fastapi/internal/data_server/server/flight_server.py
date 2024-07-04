import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from pyarrow import flight

from ..models.base_ticket import BaseTicket
from ..services.base_data_service import BaseDataService

logger = logging.getLogger(__name__)


class FlightServer(flight.FlightServerBase):
    """
    FlightServer is a subclass of flight.FlightServerBase designed to run in an asyncio environment.
    It provides an asynchronous interface to start and stop the server using a ThreadPoolExecutor.

    Attributes:
        location (str): The location where the FlightServer will be hosted.
        _executor (ThreadPoolExecutor): A thread pool executor to run the blocking serve method.
    """

    def __init__(self, location: str):
        """
        Initialize the FlightServer.

        Args:
            location (str): The location where the FlightServer will be hosted.
        """
        super().__init__(location)
        self.location = location
        self._executor = ThreadPoolExecutor(max_workers=1)

    def serve_blocking(self):
        """
        Start the FlightServer in blocking mode.

        This method will block the thread until the server is shut down.
        """
        logger.debug(f"FlightServer starting to serve at {self.location}")
        self.serve()
        logger.debug(f"FlightServer stopped serving at {self.location}")

    async def serve_async(self):
        """
        Start the FlightServer in an asynchronous mode.

        This method runs the blocking serve method in a thread pool executor to avoid blocking the asyncio event loop.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self.serve_blocking)

    def shutdown(self):
        """
        Shut down the FlightServer.

        This method stops the server and shuts down the thread pool executor.
        """
        logger.debug(f"FlightServer shutting down at {self.location}")
        super().shutdown()
        self._executor.shutdown(wait=True)

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
