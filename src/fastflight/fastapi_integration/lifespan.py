import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager

from fastapi import FastAPI

from fastflight.client import FastFlightBouncer
from fastflight.resilience.config.resilience import ResilienceConfig

logger = logging.getLogger(__name__)


@asynccontextmanager
async def fast_flight_bouncer_lifespan(
    app: FastAPI,
    registered_data_types: dict[str, str],
    flight_location: str = "grpc://0.0.0.0:8815",  # nosec B104
    resilience_config: ResilienceConfig | None = None,
):
    """
    Manage FastFlightBouncer lifecycle for FastAPI application.

    Initializes the bouncer, registers it with the app, and handles cleanup on shutdown.

    Args:
        app: FastAPI application instance.
        registered_data_types: Registry of available data service types.
        flight_location: Flight server gRPC endpoint.
        resilience_config: Optional resilience configuration for the bouncer.
    """
    logger.info("Starting FastFlightBouncer at %s", flight_location)
    if resilience_config:
        logger.info("Using resilience configuration: %s", resilience_config)
    bouncer = FastFlightBouncer(flight_location, registered_data_types, resilience_config=resilience_config)
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
    flight_location: str = "grpc://0.0.0.0:8815",  # nosec B104
    resilience_config: ResilienceConfig | None = None,
    *other: Callable[[FastAPI], AbstractAsyncContextManager],
):
    """
    Combine FastFlightBouncer lifespan with other context managers.

    Args:
        app: FastAPI application instance.
        registered_data_types: Registry of data service types.
        flight_location: Flight server gRPC endpoint.
        resilience_config: Optional resilience configuration for the bouncer.
        *other: Additional context managers to combine.
    """
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(
            fast_flight_bouncer_lifespan(app, registered_data_types, flight_location, resilience_config)
        )
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
    return helper  # type: ignore[no-any-return]
