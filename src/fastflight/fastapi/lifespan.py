import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncContextManager, Callable

from fastapi import FastAPI

from fastflight.flight_client import FlightClientManager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def flight_client_lifespan(app: FastAPI):
    """
    An asynchronous context manager that handles the lifespan of a flight client.

    This function initializes a flight client helper at a specified location, sets it as the client helper for the given FastAPI application, and yields control back to the caller. When the context is exited, it stops the flight client helper and awaits its termination.

    Parameters:
        app (FastAPI): The FastAPI application instance.
    """
    logger.info("Starting flight_client_lifespan")
    location = "grpc://localhost:8815"
    client = FlightClientManager(location)
    set_flight_client(app, client)
    try:
        yield
    finally:
        logger.info("Stopping flight_client_lifespan")
        await client.close_async()
        logger.info("Ended flight_client_lifespan")


@asynccontextmanager
async def combined_lifespan(app: FastAPI, *other: Callable[[FastAPI], AsyncContextManager]):
    """
    An asynchronous context manager that handles the combined lifespan of a flight client helper
    and any other given context managers.

    Parameters:
        app (FastAPI): The FastAPI application instance.
    """
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(flight_client_lifespan(app))
        for c in other:
            await stack.enter_async_context(c(app))
        logger.info("Entering combined lifespan")
        yield
        logger.info("Exiting combined lifespan")


def set_flight_client(app: FastAPI, client: FlightClientManager) -> None:
    """
    Sets the client helper for the given FastAPI application.

    Args:
        app (FastAPI): The FastAPI application instance.
        client (FlightClientManager): The client helper to be set.

    Returns:
        None
    """
    app.state._flight_client = client


def get_flight_client(app: FastAPI) -> FlightClientManager:
    """
    Retrieves the client helper for the given FastAPI application.

    Args:
        app (FastAPI): The FastAPI application instance.

    Returns:
        FlightClientManager: The client helper associated with the given FastAPI application.
    """
    helper = getattr(app.state, "_flight_client", None)
    if helper is None:
        raise ValueError(
            "Flight client is not set in the FastAPI application. Use the :meth:`fastflight.debug.py.fastapi.lifespan.combined_lifespan` lifespan in your FastAPI application."
        )
    return helper
