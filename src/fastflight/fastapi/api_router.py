import logging

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from starlette.requests import Request

from fastflight.fastapi.lifespan import get_flight_client
from fastflight.flight_client import FlightClientManager
from fastflight.utils.stream_utils import stream_arrow_data

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


async def _client_helper(request: Request) -> FlightClientManager:
    """
    Asynchronously retrieves the `FlightClientHelper` instance associated with the current FastAPI application.

    Args:
        request (Request): The incoming request object.

    Returns:
        FlightClientManager: The `FlightClientHelper` instance associated with the current FastAPI application.
    """
    return get_flight_client(request.app)


@router.post("/")
async def read_data(
    body_bytes: bytes = Depends(_body_bytes), client_helper: FlightClientManager = Depends(_client_helper)
):
    """
    Endpoint to read data from the Flight server and stream it back in Arrow format.

    Args:
        body_bytes (bytes): The raw request body bytes.
        client_helper(FlightClientManager): The FlightClientHelper instance for fetching data from the Flight server.

    Returns:
        StreamingResponse: The streamed response containing Arrow formatted data.
    """
    logger.debug("Received body bytes %s", body_bytes)
    stream_reader = await client_helper.aget_stream_reader(body_bytes)
    stream = await stream_arrow_data(stream_reader)
    return StreamingResponse(stream, media_type="application/vnd.apache.arrow.stream")
