import itertools
import logging
import multiprocessing
import sys
import time # For timing
from typing import Optional, cast

import pyarrow as pa
from pyarrow import RecordBatchReader, flight

from fastflight.config import flight_server_settings
from fastflight.core.base import BaseDataService, BaseParams
from fastflight.metrics import ( # Import metrics
    flight_server_active_connections,
    flight_server_bytes_transferred,
    flight_server_request_duration_seconds,
    flight_server_requests_total,
)
from fastflight.security import ServerAuthHandler
from fastflight.utils.custom_logging import setup_logging
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
        auth_handler (Optional[ServerAuthHandler]): The server authentication handler.
    """

    def __init__(self, location: str, auth_handler: Optional[ServerAuthHandler] = None):
        # The location string for super().__init__ might need to be adjusted if TLS is enabled,
        # e.g., from "grpc://host:port" to "grpc+tls://host:port"
        # This will be handled in start_instance or main.
        super().__init__(location, auth_handler=auth_handler)
        self.location = location # Store the original logical location
        self._auth_handler = auth_handler
        self._converter = AsyncToSyncConverter()
        # Initialize a counter for active do_get calls if not using more sophisticated connection tracking
        self._active_do_get_calls = 0 

    def do_get(self, context, ticket: flight.Ticket) -> flight.RecordBatchStream:
        """
        Handles a data retrieval request from a client, with Prometheus metrics.

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
        method_name = "do_get"
        start_time = time.monotonic()
        flight_server_active_connections.inc()
        self._active_do_get_calls += 1 # Manual tracking if gauge needs it per instance

        try:
            logger.debug("Received ticket (len): %s bytes", len(ticket.ticket) if ticket else 0)
            flight_server_bytes_transferred.labels(method=method_name, direction="received").inc(len(ticket.ticket or b""))

            data_params, data_service = self._resolve_ticket(ticket)
            # This is a RecordBatchReader; to count sent bytes, we'd need to iterate through it
            # and sum byte sizes of batches, or wrap it. This is complex here.
            # For now, we'll increment a placeholder for sent bytes or skip detailed byte counting for sent.
            # Let's assume _get_batch_reader returns a custom reader that can track bytes if we go deep.
            # As a simplification, we won't track sent bytes accurately here yet.
            
            reader = self._get_batch_reader(data_service, data_params)

            # To accurately track sent bytes, we would need to wrap the reader or the stream.
            # For example, by creating a generator that yields batches and counts their size.
            # This is an approximation for now, as actual sent bytes depend on client consumption.
            # flight_server_bytes_transferred.labels(method=method_name, direction="sent").inc(APPROX_SIZE_OR_IMPLEMENT_TRACKING_READER)
            
            flight_server_requests_total.labels(method=method_name, status="success").inc()
            return flight.RecordBatchStream(reader)
        except Exception as e:
            flight_server_requests_total.labels(method=method_name, status="error").inc()
            logger.error(f"Error processing request: {e}", exc_info=True)
            error_msg = f"Internal server error: {type(e).__name__}: {str(e)}"
            raise flight.FlightInternalError(error_msg)
        finally:
            duration = time.monotonic() - start_time
            flight_server_request_duration_seconds.labels(method=method_name).observe(duration)
            flight_server_active_connections.dec()
            self._active_do_get_calls -= 1

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
        # This is where actual data retrieval happens.
        # For accurate sent byte counting, this method or the data_service methods would need to be modified
        # to report the size of data produced.
        try:
            try:
                batch_iter = iter(data_service.get_batches(params, batch_size))
            except NotImplementedError:
                batch_iter = self._converter.syncify_async_iter(data_service.aget_batches(params, batch_size))

            first_batch = next(batch_iter)
            # Example: Approximate sent bytes based on first batch (very rough)
            # flight_server_bytes_transferred.labels(method="do_get", direction="sent").inc(first_batch.nbytes)
            
            # To count all bytes, we'd need a wrapper:
            # def byte_counting_iterator(original_iterator):
            #     total_bytes = 0
            #     for batch in original_iterator:
            #         total_bytes += batch.nbytes
            #         yield batch
            #     flight_server_bytes_transferred.labels(method="do_get", direction="sent").inc(total_bytes)
            # chained_iterator = byte_counting_iterator(itertools.chain((first_batch,), batch_iter))
            # return RecordBatchReader.from_batches(first_batch.schema, chained_iterator)

            return RecordBatchReader.from_batches(first_batch.schema, itertools.chain((first_batch,), batch_iter))
        except StopIteration:
            logger.warning("Data service returned no batches for params: %s", params.fqn())
            raise flight.FlightInternalError("Data service returned no batches.")
        except AttributeError as e: # E.g. if data_service doesn't have get_batches or aget_batches
            logger.error(f"Service method issue with {data_service.fqn()}: {e}", exc_info=True)
            raise flight.FlightInternalError(f"Service method issue: {e}")
        except Exception as e: # Other data retrieval errors
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
    def start_instance(
        cls,
        host: str,
        port: int,
        auth_handler: Optional[ServerAuthHandler] = None,
        tls_info: Optional[flight.ServerTLSInfo] = None,
        debug: bool = False,
    ):
        scheme = "grpc+tls" if tls_info else "grpc"
        location = f"{scheme}://{host}:{port}"
        
        server = cls(location, auth_handler=auth_handler)
        logger.info(
            "Serving FastFlightServer in process %s on %s (Auth: %s, TLS: %s)",
            multiprocessing.current_process().name,
            location,
            "Enabled" if auth_handler else "Disabled",
            "Enabled" if tls_info else "Disabled",
        )
        
        if debug or sys.gettrace() is not None or flight_server_settings.log_level.upper() == "DEBUG":
            logger.info("Enabling debug mode for FastFlightServer.do_get")
            server.do_get = debuggable(server.do_get)  # type: ignore[method-assign]
        
        server.serve(tls_info=tls_info, auth_handler=auth_handler) # Pass auth_handler to serve too


def main():
    setup_logging(service_name="FastFlightServer")

    auth_handler_instance: Optional[ServerAuthHandler] = None
    if flight_server_settings.auth_token:
        logger.info("Authentication enabled for Flight Server.")
        # For multiple tokens, ServerAuthHandler would need to be initialized with a list
        auth_handler_instance = ServerAuthHandler(valid_tokens=[flight_server_settings.auth_token])
    else:
        logger.info("Authentication disabled for Flight Server (no auth_token configured).")

    tls_info_instance: Optional[flight.ServerTLSInfo] = None
    if flight_server_settings.tls_server_cert_path and flight_server_settings.tls_server_key_path:
        logger.info("TLS enabled for Flight Server.")
        with open(flight_server_settings.tls_server_cert_path, 'rb') as cert_file, \
             open(flight_server_settings.tls_server_key_path, 'rb') as key_file:
            tls_info_instance = flight.ServerTLSInfo(
                cert_chain=cert_file.read(),
                private_key=key_file.read()
            )
    else:
        logger.info("TLS disabled for Flight Server (cert_path or key_path not configured).")
        if flight_server_settings.tls_server_cert_path or flight_server_settings.tls_server_key_path:
            logger.warning("TLS partially configured but not enabled: both cert and key paths are required.")


    logger.info(
        f"Starting FastFlight server with settings: host={flight_server_settings.host}, "
        f"port={flight_server_settings.port}, log_level={flight_server_settings.log_level}"
    )
    
    FastFlightServer.start_instance(
        host=flight_server_settings.host,
        port=flight_server_settings.port,
        auth_handler=auth_handler_instance,
        tls_info=tls_info_instance,
    )


if __name__ == "__main__":
    main()
