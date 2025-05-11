import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncContextManager, Callable

from fastapi import FastAPI

from fastflight.client import FastFlightClient

logger = logging.getLogger(__name__)


@asynccontextmanager
async def fast_flight_client_lifespan(
    app: FastAPI, registered_data_types: dict[str, str], flight_location: str = "grpc://0.0.0.0:8815"
):
    """
    An asynchronous context manager that handles the lifespan of a flight client.

    This function initializes a flight client helper at a specified location, sets it as the client helper for the given FastAPI application, and yields control back to the caller. When the context is exited, it stops the flight client helper and awaits its termination.

    Parameters:
        app (FastAPI): The FastAPI application instance.
        registered_data_types (dict[str, str]): A dictionary of registered parameter classes.
        flight_location (str, optional): The location of the flight client. Defaults to "grpc://0.0.0.0:8815".
    """
    logger.info("Starting flight_client_lifespan at %s", flight_location)
    client = FastFlightClient(flight_location, registered_data_types)
    set_flight_client(app, client)
    try:
        yield
    finally:
        logger.info("Stopping flight_client_lifespan")
        await client.close_async()
        logger.info("Ended flight_client_lifespan")


@asynccontextmanager
async def combine_lifespans(
    app: FastAPI,
    registered_data_types: dict[str, str],
    flight_location: str = "grpc://0.0.0.0:8815",
    *other: Callable[[FastAPI], AsyncContextManager],
):
    """
    An asynchronous context manager that handles the combined lifespan of a `FastFlightClient`
    and any other given context managers.

    Parameters:
        app (FastAPI): The FastAPI application instance.
        registered_data_types (dict[str, Type[BaseParams]]): A dictionary of registered parameter classes.
        flight_location (str, optional): The location of the flight client. Defaults to "grpc://0.0.0.0:8815".
    """
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(fast_flight_client_lifespan(app, registered_data_types, flight_location))
        for c in other:
            await stack.enter_async_context(c(app))
        logger.info("Entering combined lifespan")
        yield
        logger.info("Exiting combined lifespan")


def set_flight_client(app: FastAPI, client: FastFlightClient) -> None:
    """
    Sets the client helper for the given FastAPI application.

    Args:
        app (FastAPI): The FastAPI application instance.
        client (FastFlightClient): The client helper to be set.

    Returns:
        None
    """
    app.state._flight_client = client


def get_fast_flight_client(app: FastAPI) -> FastFlightClient:
    """
    Retrieves the client helper for the given FastAPI application.

    Args:
        app (FastAPI): The FastAPI application instance.

    Returns:
        FastFlightClient: The client helper associated with the given FastAPI application.
    """
    helper = getattr(app.state, "_flight_client", None)
    if helper is None:
        raise ValueError(
            "Flight client is not set in the FastAPI application. Use the :meth:`fastflight.debug.py.fastapi.lifespan.combined_lifespan` lifespan in your FastAPI application."
        )
    return helper
