import json
import logging
from typing import Optional

import pyarrow as pa
import pyarrow.flight as fl
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..internal.flight_connection_pool import FlightConnectionPool

logger = logging.getLogger(__name__)

router = APIRouter()

# Configuration for the connection pool
HOST = "localhost"
PORT = 8815
POOL_SIZE = 10

# Initialize the connection pool with proper type hint
flight_pool: Optional[FlightConnectionPool] = None


@router.on_event("startup")
async def startup_event():
    global flight_pool
    if flight_pool is None:
        flight_pool = FlightConnectionPool(HOST, PORT, POOL_SIZE)
        logger.info("FlightConnectionPool initialized")


@router.on_event("shutdown")
async def shutdown_event():
    global flight_pool
    # Close all connections in the pool
    if flight_pool is not None:
        while not flight_pool.pool.empty():
            client = flight_pool.pool.get()
            client.close()
        flight_pool = None
        logger.info("FlightConnectionPool shut down")


class TimeSeriesRequest(BaseModel):
    symbol: str
    start_date: str
    end_date: str


async def stream_flight_data(readers):
    try:
        for reader in readers:
            while True:
                record_batch = reader.read_chunk()
                if not record_batch:
                    break
                table = pa.Table.from_batches([record_batch])
                df = table.to_pandas()
                for index, row in df.iterrows():
                    yield json.dumps(row.to_dict()) + "\\n"
    except Exception as e:
        logger.error(f"Error while streaming flight data: {str(e)}")
        yield f"Error: {str(e)}"


@router.post("/get-timeseries")
async def get_timeseries(request: TimeSeriesRequest):
    if flight_pool is None:
        logger.error("Connection pool not initialized")
        raise HTTPException(status_code=500, detail="Connection pool not initialized")

    logger.info(
        f"Received request for symbol: {request.symbol}, start_date: {request.start_date}, end_date: {request.end_date}"
    )

    with flight_pool.connection() as client:
        # Create Flight descriptor
        descriptor = fl.FlightDescriptor.for_command(
            f"timeseries/{request.symbol}?start_date={request.start_date}&end_date={request.end_date}"
        )
        logger.info("Flight descriptor created")

        # Retrieve Flight info
        flight_info = client.get_flight_info(descriptor)
        logger.info("Flight info retrieved")

        # Check if endpoints are available
        if not flight_info.endpoints:
            logger.warning("No data found")
            raise HTTPException(status_code=404, detail="No data found")

        # Create readers for all endpoints
        readers = [client.do_get(endpoint.ticket) for endpoint in flight_info.endpoints]
        logger.info(f"Created readers for {len(flight_info.endpoints)} endpoints")

        # Stream the data using StreamingResponse
        return StreamingResponse(stream_flight_data(readers), media_type="application/json")
