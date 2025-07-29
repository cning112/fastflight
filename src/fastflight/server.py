import itertools
import logging
import multiprocessing
import sys

import pyarrow as pa
from pyarrow import RecordBatchReader, flight

from fastflight.core.base import BaseDataService, BaseParams
from fastflight.utils.debug import debuggable
from fastflight.utils.stream_utils import get_thread_local_converter

logger = logging.getLogger(__name__)


class FastFlightServer(flight.FlightServerBase):
    """
    High-performance Apache Arrow Flight server with dynamic data service registration.

    FastFlightServer provides a flexible, extensible framework for serving tabular data
    over Arrow Flight protocol. It automatically handles both synchronous and asynchronous
    data services, making it easy to integrate with various data sources and processing pipelines.

    Key Features:
    - **Dynamic Service Registration**: Data services are resolved at runtime based on ticket content
    - **Hybrid Sync/Async Support**: Automatically detects and adapts to sync or async data services
    - **Efficient Streaming**: Uses Arrow's native streaming for optimal memory usage and performance
    - **Error Handling**: Comprehensive error handling with detailed logging and proper Flight exceptions
    - **Debug Support**: Built-in debugging capabilities for development and troubleshooting

    Architecture:
    - Each request contains a serialized ticket with service identifier and query parameters
    - Services are instantiated on-demand and must implement either get_batches() or aget_batches()
    - Data is streamed as Arrow RecordBatches for efficient network transfer and memory usage
    - Async services are converted to sync streams using thread-local converters

    Design Rationale - Sync/Async Hybrid:
    The server supports both sync and async data services due to PyArrow Flight's synchronous
    protocol constraint. The do_get() method must be synchronous, making this hybrid approach
    optimal:

    - **Sync Services**: Zero overhead - direct iterator access
    - **Async Services**: ~3-6% async-to-sync adapter overhead but essential for I/O-bound operations
    - **Fallback Strategy**: Always tries sync first, falls back to async only when needed

    This design avoids forcing all services to pay async conversion costs while still
    supporting modern async patterns where they provide real benefits.

    Performance Characteristics:
    - **Sync Services**: Direct iteration, minimal overhead, good for simple data sources
    - **Async Services**: Converted via AsyncToSyncConverter, ideal for I/O-bound operations
    - **Memory Efficiency**: Streaming architecture prevents loading entire datasets into memory
    - **Concurrency**: Multiple clients can be served simultaneously through Flight's threading model

    Example Usage:
        >>> server = FastFlightServer("grpc://0.0.0.0:8815")
        >>> server.serve()  # Blocks and serves requests

        Or for programmatic control:
        >>> with server:
        ...     # Server runs in background
        ...     pass

    Thread Safety:
        - Server instances should not be shared across threads
        - Each request gets its own service instance
        - Thread-local converters ensure safe async-to-sync conversion

    Attributes:
        location (str): The gRPC URI where the server is hosted (e.g., "grpc://0.0.0.0:8815")

    Note:
        Data services must be registered with BaseDataService.register() before server startup
        to be discoverable via ticket resolution.
    """

    def __init__(self, location: str):
        """
        Initialize FastFlightServer with the specified network location.

        Args:
            location (str): The gRPC server location (e.g., "grpc://0.0.0.0:8815", "grpc://localhost:8815").
                           Must include the protocol scheme and be a valid URI that Flight can bind to.

        Note:
            - Creates a thread-local async-to-sync converter for handling async data services
            - Does not start serving until serve() or start_instance() is called
            - The location becomes the server's identity for client connections
        """
        super().__init__(location)
        self.location = location
        self._converter = get_thread_local_converter()

    def do_get(self, _context, ticket: flight.Ticket) -> flight.RecordBatchStream:
        """
        Handle data retrieval requests from Flight clients.

        This is the core method that processes client requests for data. It:
        1. Parses the ticket to extract service parameters
        2. Resolves and instantiates the appropriate data service
        3. Retrieves data as streaming Arrow RecordBatches
        4. Returns a Flight-compatible stream

        Args:
            _context: Flight server context (authentication, metadata, etc.)
            ticket (flight.Ticket): Serialized request containing:
                - Service fully qualified name (fqn)
                - Query parameters and filters
                - Batch size preferences

        Returns:
            flight.RecordBatchStream: Streaming Arrow data compatible with Flight protocol.
                The stream contains:
                - Schema information from the first batch
                - Data batches in Arrow format
                - Proper Flight metadata

        Raises:
            flight.FlightInternalError: For various error conditions:
                - Service not found or invalid ticket format
                - Service implementation errors
                - Data retrieval failures
                - Empty result sets

        Performance Notes:
            - Sync services: Direct iteration with minimal overhead
            - Async services: Converted via thread-local AsyncToSyncConverter
            - Memory efficient: Data streamed rather than loaded entirely
            - First batch read immediately to establish schema
        """
        try:
            logger.debug("Received ticket: %s", ticket.ticket)
            data_params, data_service = self._resolve_ticket(ticket)
            reader = self._get_batch_reader(data_service, data_params)
            return flight.RecordBatchStream(reader)
        except Exception as e:
            logger.error(f"Error processing request: {e}", exc_info=True)
            error_msg = f"Internal server error: {type(e).__name__}: {e!s}"
            raise flight.FlightInternalError(error_msg) from e

    def _get_batch_reader(
        self, data_service: BaseDataService, params: BaseParams, batch_size: int | None = None
    ) -> pa.RecordBatchReader:
        """
        Create an Arrow RecordBatchReader from a data service.

        This method bridges different data service implementations (sync/async) into a unified
        streaming interface. It automatically detects the service type and adapts accordingly.

        Strategy:
        1. **Always try sync get_batches() first** (preferred path for performance)
        2. **Fallback to async aget_batches() only if sync raises NotImplementedError**
        3. Convert async iterator to sync using thread-local converter
        4. Consume first batch to establish schema and chain remaining batches

        Args:
            data_service (BaseDataService): Service instance to fetch data from
            params (BaseParams): Query parameters (filters, pagination, etc.)
            batch_size (int | None): Maximum records per batch. None = service default

        Returns:
            pa.RecordBatchReader: Arrow reader with established schema and data stream

        Raises:
            flight.FlightInternalError: If:
                - Service returns no data (empty iterator)
                - Service methods are missing or malformed
                - Data retrieval fails internally

        Performance Notes:
            - Sync services: Direct iterator, minimal overhead
            - Async services: AsyncToSyncConverter adds ~6% overhead
            - Memory efficient: Only first batch loaded for schema detection
        """
        try:
            try:
                batch_iter = iter(data_service.get_batches(params, batch_size))
            except NotImplementedError:
                batch_iter = self._converter.syncify_async_iter(data_service.aget_batches(params, batch_size))

            first = next(batch_iter)
            return RecordBatchReader.from_batches(first.schema, itertools.chain((first,), batch_iter))
        except StopIteration:
            raise flight.FlightInternalError("Data service returned no batches.") from None
        except AttributeError as e:
            raise flight.FlightInternalError(f"Service method issue: {e}") from e
        except Exception as e:
            logger.error(f"Error retrieving data from {data_service.fqn()}: {e}", exc_info=True)
            raise flight.FlightInternalError(f"Error in data retrieval: {type(e).__name__}: {e!s}") from e

    @staticmethod
    def _resolve_ticket(ticket: flight.Ticket) -> tuple[BaseParams, BaseDataService]:
        """
        Parse Flight ticket and instantiate the corresponding data service.

        Args:
            ticket (flight.Ticket): Serialized ticket containing service fqn and parameters

        Returns:
            tuple[BaseParams, BaseDataService]: Parsed parameters and service instance

        Raises:
            flight.FlightInternalError: For ticket parsing or service resolution failures
        """
        try:
            req_params = BaseParams.from_bytes(ticket.ticket)
            service_cls = BaseDataService.lookup(req_params.fqn())
            return req_params, service_cls()
        except KeyError as e:
            raise flight.FlightInternalError(f"Missing required field in ticket: {e}") from e
        except ValueError as e:
            raise flight.FlightInternalError(f"Invalid ticket format: {e}") from e
        except Exception as e:
            logger.error(f"Error processing ticket: {e}", exc_info=True)
            raise flight.FlightInternalError(f"Ticket processing error: {type(e).__name__}: {e!s}") from e

    def shutdown(self):
        """
        Gracefully shutdown the server and release resources.

        Closes the async-to-sync converter and calls parent cleanup.
        Should be called when server is no longer needed.
        """
        logger.debug(f"FastFlightServer shutting down at {self.location}")
        self._converter.close()
        super().shutdown()

    @classmethod
    def start_instance(cls, location: str, debug: bool = False):
        """
        Create and start a server instance (blocking).

        Args:
            location (str): Server gRPC location
            debug (bool): Enable debugging decorators for do_get method

        Note:
            This method blocks until server shutdown. Use threading for background operation.
        """
        server = cls(location)
        logger.info("Serving FastFlightServer in process %s", multiprocessing.current_process().name)
        if debug or sys.gettrace() is not None:
            logger.info("Enabling debug mode")
            server.do_get = debuggable(server.do_get)  # type: ignore[method-assign]
        server.serve()


def main():
    from fastflight.utils.custom_logging import setup_logging

    setup_logging()
    FastFlightServer.start_instance("grpc://0.0.0.0:8815", True)  # nosec B104


if __name__ == "__main__":
    main()
