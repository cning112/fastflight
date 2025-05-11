import logging

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from fastflight.client import FastFlightClient
from fastflight.fastapi.dependencies import body_bytes, fast_flight_client
from fastflight.utils.stream_utils import write_arrow_data_to_stream

logger = logging.getLogger(__name__)
fast_flight_router = APIRouter()


@fast_flight_router.get("/registered_data_types")
def get_registered_data_types(ff_client: FastFlightClient = Depends(fast_flight_client)):
    """
    Retrieve all registered data types from the Flight client.

    Returns a list of dictionaries, each mapping a registered BaseParams class fully qualified name (FQN)
    to its corresponding BaseDataService class FQN. This endpoint is useful for debugging or introspection
    in client applications to understand the available data types and their associated services.

    The 'param_type' FQN is required in the request body when calling the `/stream` endpoint, so this endpoint
    helps users discover valid FQN values for streaming requests.
    """
    result = []
    for param_fqn, srv_fqn in ff_client.get_registered_data_types().items():
        result.append({"params_type": param_fqn, "service_type": srv_fqn})
    return result


@fast_flight_router.post("/stream")
async def read_data(body: bytes = Depends(body_bytes), ff_client: FastFlightClient = Depends(fast_flight_client)):
    """
    Endpoint to read data from the Flight server and stream it back in Arrow format.

    Args:
        body (bytes): The raw request body bytes. The body should be a JSON-serialized `BaseParams` instance.
            Crucially, it must include the `param_type` field specifying the fully qualified name (FQN) of the data params class.
        ff_client (FastFlightClient): The FlightClientHelper instance used to fetch data from the Flight server.

    Returns:
        StreamingResponse: A streamed response containing data in Apache Arrow format.
    """
    logger.debug("Received body %s", body)
    stream_reader = await ff_client.aget_stream_reader(body)
    stream = await write_arrow_data_to_stream(stream_reader)
    return StreamingResponse(stream, media_type="application/vnd.apache.arrow.stream")
