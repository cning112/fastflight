import asyncio
import logging
from contextlib import asynccontextmanager, AsyncExitStack
from typing import AsyncIterable

from fastapi import APIRouter, Depends, FastAPI
from fastapi.responses import StreamingResponse
from starlette.requests import Request

from ..internal.data_server.client_helpers import FlightClientHelper
from ..internal.data_server.models.base_ticket import BaseTicket
from ..internal.data_server.server.flight_server import FlightServer

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/data_server")


@asynccontextmanager
async def flight_server_lifespan(app: FastAPI):
    logger.info("Starting flight_server_lifespan")
    location = "grpc://0.0.0.0:8815"
    fl_server = FlightServer(location)
    flight_server_task = asyncio.create_task(fl_server.serve_async())
    try:
        yield
    finally:
        logger.info("Stopping flight_server_lifespan")
        flight_server_task.cancel()
        await flight_server_task
        logger.info("Ended flight_server_lifespan")


@asynccontextmanager
async def flight_client_helper_lifespan(app: FastAPI):
    logger.info("Starting flight_client_helper_lifespan")
    location = "grpc://localhost:8815"
    client_helper = FlightClientHelper(location)
    app.state.client_helper = client_helper
    try:
        yield
    finally:
        logger.info("Stopping flight_client_helper_lifespan")
        await client_helper.close()
        logger.info("Ended flight_client_helper_lifespan")


@asynccontextmanager
async def combined_lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(flight_server_lifespan(app))
        await stack.enter_async_context(flight_client_helper_lifespan(app))
        logger.info("Entering combined lifespan")
        yield
        logger.info("Exiting combined lifespan")


async def get_body_bytes(request: Request) -> bytes:
    return await request.body()


async def get_client_helper(request: Request) -> FlightClientHelper:
    return await request.app.state.client_helper


def ticket_request_dependency() -> BaseTicket:
    """
    This function doesn't actually parse the request body,
    but serves to indicate the expected request body type in OpenAPI documentation.
    """
    # Return a dummy instance just for OpenAPI documentation purposes
    return BaseTicket.from_bytes(b'{"kind": "sql", "query": "1"}')


@router.post("/")
async def read_data(
    body_bytes: bytes = Depends(get_body_bytes),
    ticket_request: BaseTicket = Depends(ticket_request_dependency),
    client_helper: FlightClientHelper = Depends(),
):
    """
    Endpoint to read data from the Flight server and stream it back in Arrow format.

    Args:
        body_bytes (bytes): The raw request body bytes.
        ticket_request (BaseTicket): Only used for OpenAPI documentation purposes. Won't parse body data.
        client_helper(FlightClientHelper): The FlightClientHelper instance for fetching data from the Flight server.

    Returns:
        StreamingResponse: The streamed response containing Arrow formatted data.
    """

    async def data_generator() -> AsyncIterable[bytes]:
        reader = await client_helper.fetch_data_async(body_bytes)
        for batch in reader:
            yield batch.data.to_bytes()

    return StreamingResponse(data_generator(), media_type="application/vnd.apache.arrow.stream")
