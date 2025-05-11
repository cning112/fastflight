import itertools
import logging
import multiprocessing
import sys
from typing import cast

import pyarrow as pa
from pyarrow import RecordBatchReader, flight

from fastflight.core.base import BaseDataService, BaseParams
from fastflight.utils.debug import debuggable
from fastflight.utils.stream_utils import AsyncToSyncConverter

logger = logging.getLogger(__name__)


class FastFlightServer(flight.FlightServerBase):
    """
    FastFlightServer is an Apache Arrow Flight server that:
    - Handles pre-flight requests to dynamically register data services.
    - Manages the retrieval of tabular data via registered data services.
    - Ensures efficient conversion between asynchronous and synchronous data streams.

    Attributes:
        location (str): The URI where the server is hosted.
    """

    def __init__(self, location: str):
        super().__init__(location)
        self.location = location
        self._converter = AsyncToSyncConverter()

    def do_get(self, context, ticket: flight.Ticket) -> flight.RecordBatchStream:
        """
        Handles a data retrieval request from a client.

        This method:
        - Parses the `ticket` to extract the request parameters.
        - Loads the corresponding data service.
        - Retrieves tabular data in Apache Arrow format.

        Args:
            context: Flight request context.
            ticket (flight.Ticket): The request ticket containing serialized query parameters.

        Returns:
            flight.RecordBatchStream: A stream of record batches containing the requested data.

        Raises:
            flight.FlightUnavailableError: If the requested data service is not registered.
            flight.FlightInternalError: If an unexpected error occurs during retrieval.
        """
        try:
            logger.debug("Received ticket: %s", ticket.ticket)
            data_params, data_service = self._resolve_ticket(ticket)
            reader = self._get_batch_reader(data_service, data_params)
            return flight.RecordBatchStream(reader)
        except Exception as e:
            logger.error(f"Error processing request: {e}", exc_info=True)
            error_msg = f"Internal server error: {type(e).__name__}: {str(e)}"
            raise flight.FlightInternalError(error_msg)

    def _get_batch_reader(
        self, data_service: BaseDataService, params: BaseParams, batch_size: int | None = None
    ) -> pa.RecordBatchReader:
        """
        Args:
            data_service (BaseDataService): The data service instance.
            params (BaseParams): The parameters for fetching data.
            batch_size (int|None): The maximum size of each batch. Defaults to None to be decided by the data service

        Returns:
            RecordBatchReader: A RecordBatchReader instance to read the data in batches.
        """
        try:
            try:
                batch_iter = iter(data_service.get_batches(params, batch_size))
            except NotImplementedError:
                batch_iter = self._converter.syncify_async_iter(data_service.aget_batches(params, batch_size))

            first = next(batch_iter)
            return RecordBatchReader.from_batches(first.schema, itertools.chain((first,), batch_iter))
        except StopIteration:
            raise flight.FlightInternalError("Data service returned no batches.")
        except AttributeError as e:
            raise flight.FlightInternalError(f"Service method issue: {e}")
        except Exception as e:
            logger.error(f"Error retrieving data from {data_service.fqn()}: {e}", exc_info=True)
            raise flight.FlightInternalError(f"Error in data retrieval: {type(e).__name__}: {str(e)}")

    @staticmethod
    def _resolve_ticket(ticket: flight.Ticket) -> tuple[BaseParams, BaseDataService]:
        try:
            req_params = BaseParams.from_bytes(ticket.ticket)
            service_cls = BaseDataService.lookup(req_params.fqn())
            return req_params, cast(BaseDataService, service_cls())
        except KeyError as e:
            raise flight.FlightInternalError(f"Missing required field in ticket: {e}")
        except ValueError as e:
            raise flight.FlightInternalError(f"Invalid ticket format: {e}")
        except Exception as e:
            logger.error(f"Error processing ticket: {e}", exc_info=True)
            raise flight.FlightInternalError(f"Ticket processing error: {type(e).__name__}: {str(e)}")

    def shutdown(self):
        """
        Shut down the FastFlightServer.

        This method stops the server and shuts down the thread pool executor.
        """
        logger.debug(f"FastFlightServer shutting down at {self.location}")
        self._converter.close()
        super().shutdown()

    @classmethod
    def start_instance(cls, location: str, debug: bool = False):
        server = cls(location)
        logger.info("Serving FastFlightServer in process %s", multiprocessing.current_process().name)
        if debug or sys.gettrace() is not None:
            logger.info("Enabling debug mode")
            server.do_get = debuggable(server.do_get)  # type: ignore[method-assign]
        server.serve()


def main():
    from fastflight.utils.custom_logging import setup_logging

    setup_logging()
    FastFlightServer.start_instance("grpc://0.0.0.0:8815", True)


if __name__ == "__main__":
    main()
