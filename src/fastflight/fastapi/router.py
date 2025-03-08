import logging

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from fastflight.client import FastFlightClient
from fastflight.fastapi.dependencies import body_bytes, fast_flight_client
from fastflight.utils.stream_utils import write_arrow_data_to_stream

logger = logging.getLogger(__name__)
fast_flight_router = APIRouter()


@fast_flight_router.get("/data_types")
def get_data_types(ff_client: FastFlightClient = Depends(fast_flight_client)):
    result = []
    for k, v in ff_client.get_data_types().items():
        service_cls = v.default_service_class()
        result.append(
            {
                "params_cls": f"{v.__module__}.{v.__name__}",
                "service_cls": f"{service_cls.__module__}.{service_cls.__name__}",
            }
        )
    return result


@fast_flight_router.post("/stream")
async def read_data(body: bytes = Depends(body_bytes), ff_client: FastFlightClient = Depends(fast_flight_client)):
    """
    Endpoint to read data from the Flight server and stream it back in Arrow format.

    Args:
        body (bytes): The raw request body bytes.
        ff_client(FastFlightClient): The FlightClientHelper instance for fetching data from the Flight server.

    Returns:
        StreamingResponse: The streamed response containing Arrow formatted data.
    """
    logger.debug("Received body %s", body)
    stream_reader = await ff_client.aget_stream_reader(body)
    stream = await write_arrow_data_to_stream(stream_reader)
    return StreamingResponse(stream, media_type="application/vnd.apache.arrow.stream")
