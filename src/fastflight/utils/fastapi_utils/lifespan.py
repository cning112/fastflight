import asyncio
import logging
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI

from fastflight.utils.client_helpers import FlightClientHelper
from fastflight.utils.flight_server import FlightServer

logger = logging.getLogger(__name__)


@asynccontextmanager
async def flight_server_lifespan(app: FastAPI):
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
async def flight_client_helper_lifespan(app: FastAPI):
    logger.info("Starting flight_client_helper_lifespan")
    location = "grpc://localhost:8815"
    client_helper = FlightClientHelper(location)
    app.state.client_helper = client_helper
    try:
        yield
    finally:
        logger.info("Stopping flight_client_helper_lifespan")
        await client_helper.close_async()
        logger.info("Ended flight_client_helper_lifespan")


@asynccontextmanager
async def combined_lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(flight_server_lifespan(app))
        await stack.enter_async_context(flight_client_helper_lifespan(app))
        logger.info("Entering combined lifespan")
        yield
        logger.info("Exiting combined lifespan")
