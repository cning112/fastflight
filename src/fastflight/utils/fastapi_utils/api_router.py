import logging

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from starlette.requests import Request

from fastflight.utils.fastapi_utils.lifespan import get_flight_client
from fastflight.utils.flight_client import PooledClient

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/fastflight")


async def _body_bytes(request: Request) -> bytes:
    """
    Retrieves the request body bytes from the provided Request object.

    Args:
        request (Request): The Request object containing the body bytes.

    Returns:
        bytes: The request body bytes.
    """
    return await request.body()


async def _client_helper(request: Request) -> PooledClient:
    """
    Asynchronously retrieves the `FlightClientHelper` instance associated with the current FastAPI application.

    Args:
        request (Request): The incoming request object.

    Returns:
        PooledClient: The `FlightClientHelper` instance associated with the current FastAPI application.
    """
    return get_flight_client(request.app)


@router.post("/")
async def read_data(body_bytes: bytes = Depends(_body_bytes), client_helper: PooledClient = Depends(_client_helper)):
    """
    Endpoint to read data from the Flight server and stream it back in Arrow format.

    Args:
        body_bytes (bytes): The raw request body bytes.
        client_helper(PooledClient): The FlightClientHelper instance for fetching data from the Flight server.

    Returns:
        StreamingResponse: The streamed response containing Arrow formatted data.
    """
    logger.debug("Received body bytes %s", body_bytes)
    return StreamingResponse(
        client_helper.aget_bytes_stream(body_bytes), media_type="application/vnd.apache.arrow.stream"
    )
