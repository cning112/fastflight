import functools
import logging
import multiprocessing

from pyarrow import flight

from fastflight.services.base import BaseDataService, BaseParams, create_kind_name
from fastflight.utils.custom_logging import setup_logging

logger = logging.getLogger(__name__)


def debuggable(func):
    """A decorator to enable GUI (i.e. PyCharm) debugging in the
    decorated Arrow Flight RPC Server function.

    See: https://github.com/apache/arrow/issues/36844
    for more details...
    """

    @functools.wraps(func)
    def wrapper_decorator(*args, **kwargs):
        try:
            import pydevd

            pydevd.connected = True
            pydevd.settrace(suspend=False)
        except ImportError:
            # Not running in debugger
            pass
        value = func(*args, **kwargs)
        return value

    return wrapper_decorator


class FlightServer(flight.FlightServerBase):
    """
    FlightServer is a subclass of flight.FlightServerBase designed to run in an asyncio environment.
    It provides an asynchronous interface to start and stop the server using a ThreadPoolExecutor.

    Attributes:
        location (str): The location where the FlightServer will be hosted.
    """

    def __init__(self, location: str):
        """
        Initialize the FlightServer.

        Args:
            location (str): The location where the FlightServer will be hosted.
        """
        super().__init__(location)
        self.location = location

    def shutdown(self):
        """
        Shut down the FlightServer.

        This method stops the server and shuts down the thread pool executor.
        """
        logger.debug(f"FlightServer shutting down at {self.location}")
        super().shutdown()

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
        kind_str = create_kind_name(params.kind)

        try:
            data_service_cls = BaseDataService.get_data_service_cls(kind_str)
            data_service = data_service_cls()
            return params, data_service
        except ValueError as e:
            logger.error(f"Data service unavailable for ticket type {kind_str}: {e}")
            raise flight.FlightUnavailableError(f"Data service unavailable: {e}")
        except Exception as e:
            logger.error(f"Error getting data source for ticket type {kind_str}: {e}")
            raise

    @debuggable
    def do_get(self, context, ticket: flight.Ticket) -> flight.RecordBatchStream:
        try:
            logger.debug("FlightServer received ticket: %s", ticket.ticket)
            params, data_service = self.load_params_and_data_service(ticket.ticket)
            reader = data_service.get_batch_reader(params)
            return flight.RecordBatchStream(reader)
        except flight.FlightUnavailableError:
            logger.error("Data service unavailable")
            raise
        except Exception as e:
            logger.error(f"Internal server error: {e}")
            raise flight.FlightInternalError(f"Internal server error: {e}")


def start_flight_server(location: str):
    server = FlightServer(location)
    logger.info("Serving FlightServer in process %s", multiprocessing.current_process().name)
    server.serve()


if __name__ == "__main__":
    setup_logging(log_file="flight_server.log")

    logger.info("Registered params types: %s", BaseParams.registry.keys())

    loc = "grpc://0.0.0.0:8815"
    start_flight_server(loc)
