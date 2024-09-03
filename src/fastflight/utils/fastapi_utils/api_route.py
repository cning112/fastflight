import logging

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from starlette.requests import Request

from fastflight.services.base_params import BaseParams
from fastflight.utils.client_helpers import FlightClientHelper

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/fastflight")


async def get_request_body_bytes(request: Request) -> bytes:
    return await request.body()


async def get_client_helper(request: Request) -> FlightClientHelper:
    return request.app.state.client_helper


def ticket_request_dependency() -> BaseParams:
    """
    This function doesn't actually parse the request body,
    but serves to indicate the expected request body type in OpenAPI documentation.
    """
    # Return a dummy instance just for OpenAPI documentation purposes
    return BaseParams.from_bytes(b'{"kind": "sql", "query": "1"}')


@router.post("/")
async def read_data(
    body_bytes: bytes = Depends(get_request_body_bytes),
    # TODO: this actually doesn't work. The swapper page doesn't show the expected data model
    ticket_request: BaseParams = Depends(ticket_request_dependency),
    client_helper: FlightClientHelper = Depends(get_client_helper),
):
    """
    Endpoint to read data from the Flight server and stream it back in Arrow format.

    Args:
        body_bytes (bytes): The raw request body bytes.
        ticket_request (BaseParams): Only used for OpenAPI documentation purposes. Won't parse body data.
        client_helper(FlightClientHelper): The FlightClientHelper instance for fetching data from the Flight server.

    Returns:
        StreamingResponse: The streamed response containing Arrow formatted data.
    """
    logger.debug("Received body bytes %s", body_bytes)
    return StreamingResponse(
        client_helper.aget_bytes_stream(body_bytes), media_type="application/vnd.apache.arrow.stream"
    )
