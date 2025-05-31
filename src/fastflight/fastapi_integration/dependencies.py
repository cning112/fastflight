from starlette.requests import Request

from fastflight.client import FastFlightBouncer
from fastflight.fastapi_integration.lifespan import get_fast_flight_bouncer


async def body_bytes(request: Request) -> bytes:
    """
    Retrieves the request body bytes from the provided Request object.

    Args:
        request (Request): The Request object containing the body bytes.

    Returns:
        bytes: The request body bytes.
    """
    return await request.body()


async def fast_flight_bouncer(request: Request) -> FastFlightBouncer:
    """
    Retrieve the FastFlightBouncer instance from the FastAPI application.

    Args:
        request: The incoming FastAPI request object.

    Returns:
        FastFlightBouncer: The bouncer instance for Flight server communication.
    """
    return get_fast_flight_bouncer(request.app)
