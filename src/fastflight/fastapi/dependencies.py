from starlette.requests import Request

from fastflight.client import FastFlightClient
from fastflight.fastapi.lifespan import get_fast_flight_client


async def body_bytes(request: Request) -> bytes:
    """
    Retrieves the request body bytes from the provided Request object.

    Args:
        request (Request): The Request object containing the body bytes.

    Returns:
        bytes: The request body bytes.
    """
    return await request.body()


async def fast_flight_client(request: Request) -> FastFlightClient:
    """
    Asynchronously retrieves the `FlightClientHelper` instance associated with the current FastAPI application.

    Args:
        request (Request): The incoming request object.

    Returns:
        FastFlightClient: The `FlightClientHelper` instance associated with the current FastAPI application.
    """
    return get_fast_flight_client(request.app)
