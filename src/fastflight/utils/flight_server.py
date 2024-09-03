import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from pyarrow import flight

from fastflight.services.base_data_service import BaseDataService
from fastflight.services.base_params import BaseParams

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
    def load_params_and_data_service(flight_ticket_bytes: bytes) -> tuple[BaseParams, BaseDataService]:
        """
        Helper method to parse the params and get the corresponding data source instance.

        Args:
            flight_ticket_bytes (bytes): The raw params bytes.

        Returns:
            tuple: A tuple containing the parsed ticket and data source instance.
        """
        params = BaseParams.from_bytes(flight_ticket_bytes)

        try:
            data_service_cls = BaseDataService.get_data_service_cls(params.kind)
            data_service = data_service_cls()
            return params, data_service
        except ValueError as e:
            logger.error(f"Data service unavailable for ticket type {params.kind}: {e}")
            raise flight.FlightUnavailableError(f"Data service unavailable: {e}")
        except Exception as e:
            logger.error(f"Error getting data source for ticket type {params.kind}: {e}")
            raise

    def do_get(self, context, ticket: flight.Ticket) -> flight.RecordBatchStream:
        try:
            params, data_service = self.load_params_and_data_service(ticket.ticket)
            reader = asyncio.run(data_service.aget_reader(params, batch_size=512))
            return flight.RecordBatchStream(reader)
        except flight.FlightUnavailableError as e:
            raise e
        except Exception as e:
            logger.error(f"Internal server error: {e}")
            raise flight.FlightInternalError(f"Internal server error: {e}")


if __name__ == "__main__":
    loc = "grpc://0.0.0.0:8815"
    fl_server = FlightServer(loc)
    fl_server.serve_blocking()
