import itertools
import logging
import multiprocessing
import sys
from typing import cast

import pyarrow as pa
from pyarrow import RecordBatchReader, flight

from fastflight.core.base import BaseDataService, BaseParams
from fastflight.core.distributed import DistributedTimeSeriesService
from fastflight.core.timeseries import TimeSeriesParams
from fastflight.utils.debug import debuggable
from fastflight.utils.stream_utils import AsyncToSyncConverter

logger = logging.getLogger(__name__)


class FastFlightServer(flight.FlightServerBase):
    """
    FastFlightServer with intelligent automatic distributed processing for time series.

    This Apache Arrow Flight server provides:
    - Dynamic registration and handling of data services
    - Automatic distributed processing for large time series queries
    - Efficient streaming of tabular data in Apache Arrow format
    - Seamless conversion between asynchronous and synchronous data streams
    - Zero-configuration performance optimization

    The server automatically detects time series queries that would benefit from
    distributed processing (>1000 estimated data points) and transparently applies
    parallel processing using Ray or AsyncIO backends.

    Attributes:
        location (str): The URI where the server is hosted.
        enable_auto_distribution (bool): Whether automatic distributed processing is enabled.
    """

    def __init__(self, location: str, enable_auto_distribution: bool = True):
        """
        Initialize the FastFlightServer.

        Args:
            location (str): The URI where the server should be hosted (e.g., "grpc://0.0.0.0:8815").
            enable_auto_distribution (bool): Whether to enable automatic distributed processing
                for time series queries. Defaults to True if distributed processing is available.
        """
        super().__init__(location)
        self.location = location
        self.enable_auto_distribution = enable_auto_distribution
        self._converter = AsyncToSyncConverter()

        if self.enable_auto_distribution:
            logger.info("Auto-distribution enabled for time series queries")

    def do_get(self, context, ticket: flight.Ticket) -> flight.RecordBatchStream:
        """
        Handles a data retrieval request from a client with automatic distributed processing.

        This method:
        - Parses the `ticket` to extract the request parameters.
        - Loads the corresponding data service.
        - Automatically applies distributed processing to large time series queries.
        - Retrieves tabular data in Apache Arrow format with proper streaming.

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
            data_params, data_service = self._resolve_ticket(ticket.ticket)

            # Apply distributed processing if beneficial
            if self._should_distribute(data_params):
                data_service = DistributedTimeSeriesService(data_service)
                logger.debug(f"Applied distributed processing to {data_service.base_service.__class__.__name__}")

            reader = self._get_batch_reader(data_service, data_params)
            return flight.RecordBatchStream(reader)
        except Exception as e:
            logger.error(f"Error processing request: {e}", exc_info=True)
            error_msg = f"Internal server error: {type(e).__name__}: {str(e)}"
            raise flight.FlightInternalError(error_msg)

    def _should_distribute(self, params: BaseParams) -> bool:
        """
        Determine if distributed processing should be applied to a query.

        Uses a simple but effective heuristic: distribute time series queries
        with more than 1000 estimated data points, as these typically benefit
        from parallel processing.

        Args:
            params (BaseParams): The query parameters to evaluate.

        Returns:
            bool: True if distributed processing should be applied, False otherwise.

        Note:
            This method only applies to TimeSeriesParams instances that have
            an estimate_data_points() method. Other parameter types are not
            distributed automatically.
        """
        return (
            self.enable_auto_distribution
            and isinstance(params, TimeSeriesParams)
            and hasattr(params, "estimate_data_points")
            and params.estimate_data_points() > 1000
        )

    def _get_batch_reader(
        self, data_service: BaseDataService, params: BaseParams, batch_size: int | None = None
    ) -> pa.RecordBatchReader:
        """
        Creates a RecordBatchReader for streaming data from a service.

        This method handles the conversion between asynchronous and synchronous data streams,
        ensuring efficient streaming of large datasets without memory accumulation.

        Args:
            data_service (BaseDataService): The data service instance (may be wrapped with distributed processing).
            params (BaseParams): The parameters for fetching data.
            batch_size (int|None): The maximum size of each batch. Defaults to None to be decided by the data service.

        Returns:
            RecordBatchReader: A RecordBatchReader instance to read the data in batches.

        Raises:
            flight.FlightInternalError: If the service returns no batches, has method issues, or encounters errors.
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
    def _resolve_ticket(ticket: bytes) -> tuple[BaseParams, BaseDataService]:
        try:
            req_params = BaseParams.from_bytes(ticket)
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
        Gracefully shut down the FastFlightServer.

        This method stops the server and properly cleans up resources including
        the async-to-sync converter and any associated thread pools.
        """
        logger.debug(f"FastFlightServer shutting down at {self.location}")
        self._converter.close()
        super().shutdown()

    @classmethod
    def start_instance(cls, location: str, debug: bool = False, enable_auto_distribution: bool = True):
        """
        Start a FastFlightServer instance.

        Args:
            location (str): The URI where the server should be hosted.
            debug (bool): Whether to enable debug mode with additional logging and debugging capabilities.
            enable_auto_distribution (bool): Whether to enable automatic distributed processing.
                Defaults to True.
        """
        server = cls(location, enable_auto_distribution)
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
