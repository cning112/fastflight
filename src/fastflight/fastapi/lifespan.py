import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncContextManager, Callable

from fastapi import FastAPI

from fastflight.client import FastFlightBouncer

logger = logging.getLogger(__name__)


@asynccontextmanager
async def fast_flight_bouncer_lifespan(
    app: FastAPI, registered_data_types: dict[str, str], flight_location: str = "grpc://0.0.0.0:8815"
):
    """
    Manage FastFlightBouncer lifecycle for FastAPI application.

    Initializes the bouncer, registers it with the app, and handles cleanup on shutdown.

    Args:
        app: FastAPI application instance.
        registered_data_types: Registry of available data service types.
        flight_location: Flight server gRPC endpoint.
    """
    logger.info("Starting FastFlightBouncer at %s", flight_location)
    bouncer = FastFlightBouncer(flight_location, registered_data_types)
    set_flight_bouncer(app, bouncer)
    try:
        yield
    finally:
        logger.info("Shutting down FastFlightBouncer")
        await bouncer.close_async()
        logger.info("FastFlightBouncer shutdown complete")


@asynccontextmanager
async def combine_lifespans(
    app: FastAPI,
    registered_data_types: dict[str, str],
    flight_location: str = "grpc://0.0.0.0:8815",
    *other: Callable[[FastAPI], AsyncContextManager],
):
    """
    Combine FastFlightBouncer lifespan with other context managers.

    Args:
        app: FastAPI application instance.
        registered_data_types: Registry of data service types.
        flight_location: Flight server gRPC endpoint.
        *other: Additional context managers to combine.
    """
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(fast_flight_bouncer_lifespan(app, registered_data_types, flight_location))
        for c in other:
            await stack.enter_async_context(c(app))
        logger.info("Combined lifespan started")
        yield
        logger.info("Combined lifespan ended")


def set_flight_bouncer(app: FastAPI, bouncer: FastFlightBouncer) -> None:
    """Set FastFlightBouncer instance in FastAPI app state."""
    app.state._flight_client = bouncer


def get_fast_flight_bouncer(app: FastAPI) -> FastFlightBouncer:
    """
    Get FastFlightBouncer from FastAPI app state.

    Args:
        app: FastAPI application instance.

    Returns:
        FastFlightBouncer instance.

    Raises:
        ValueError: If bouncer not initialized in app lifespan.
    """
    helper = getattr(app.state, "_flight_client", None)
    if helper is None:
        raise ValueError("FastFlightBouncer not initialized. Use fast_flight_bouncer_lifespan in your FastAPI app.")
    return helper
