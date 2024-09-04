import asyncio
import logging
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI

from fastflight.utils.flight_client import PooledClient
from fastflight.utils.flight_server import FlightServer

logger = logging.getLogger(__name__)


@asynccontextmanager
async def flight_server_lifespan(app: FastAPI):
    """
    An asynchronous context manager that handles the lifespan of a flight server.

    This function initializes a flight server at a specified location, starts it asynchronously,
    and yields control back to the caller. When the context is exited, it stops the flight server
    and awaits its termination.

    Parameters:
        app (FastAPI): The FastAPI application instance.
    """
    logger.info("Starting flight_server_lifespan")
    location = "grpc://0.0.0.0:8815"
    fl_server = FlightServer(location)
    fl_server_task = asyncio.create_task(fl_server.serve_async())
    try:
        yield
    finally:
        logger.info("Stopping flight_server_lifespan")
        fl_server_task.cancel()
        await fl_server_task
        logger.info("Ended flight_server_lifespan")


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
    client = PooledClient(location)
    set_flight_client(app, client)
    try:
        yield
    finally:
        logger.info("Stopping flight_client_lifespan")
        await client.close_async()
        logger.info("Ended flight_client_lifespan")


@asynccontextmanager
async def combined_lifespan(app: FastAPI):
    """
    An asynchronous context manager that handles the combined lifespan of a flight server and a flight client helper.

    This function initializes both a flight server and a flight client helper, starts them asynchronously,
    and yields control back to the caller. When the context is exited, it stops both the flight server and the flight client helper
    and awaits their termination.

    Parameters:
        app (FastAPI): The FastAPI application instance.
    """
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(flight_server_lifespan(app))
        await stack.enter_async_context(flight_client_lifespan(app))
        logger.info("Entering combined lifespan")
        yield
        logger.info("Exiting combined lifespan")


def set_flight_client(app: FastAPI, client: PooledClient) -> None:
    """
    Sets the client helper for the given FastAPI application.

    Args:
        app (FastAPI): The FastAPI application instance.
        client (PooledClient): The client helper to be set.

    Returns:
        None
    """
    app.state._flight_client = client


def get_flight_client(app: FastAPI) -> PooledClient:
    """
    Retrieves the client helper for the given FastAPI application.

    Args:
        app (FastAPI): The FastAPI application instance.

    Returns:
        PooledClient: The client helper associated with the given FastAPI application.
    """
    helper = getattr(app.state, "_flight_client", None)
    if helper is None:
        raise ValueError(
            "Flight client is not set in the FastAPI application. Use the :meth:`fastflight.utils.fastapi_utils.lifespan.combined_lifespan` lifespan in your FastAPI application."
        )
    return helper
