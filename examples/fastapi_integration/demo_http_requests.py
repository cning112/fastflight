import asyncio
import logging
from typing import Callable, Iterable, TypeVar

import httpx
from mock_data_service import MockDataParams

from fastflight.flight_client import FlightClientManager
from fastflight.utils.stream_utils import read_dataframe_from_arrow_stream

logger = logging.getLogger(__name__)

T = TypeVar("T")


def request_for_data(url: str, json_data: dict, handle_stream: Callable[[Iterable[bytes]], T]) -> T:
    with httpx.stream("POST", url, json=json_data, timeout=None) as response:
        response.raise_for_status()
        return handle_stream(response.iter_bytes())


if __name__ == "__main__":
    data_params = MockDataParams(rows_per_batch=5_000, delay_per_row=1e-6)

    # http request
    df = request_for_data("http://127.0.0.1:8000/fastflight/", data_params.to_json(), read_dataframe_from_arrow_stream)
    print("read df from http", df)

    # grpc request
    LOC = "grpc://localhost:8815"
    client = FlightClientManager(LOC)

    # get data in an async way
    async def main():
        reader = await client.aget_stream_reader(data_params)
        for batch in reader:
            print("read batch from grpc", batch.data)

    asyncio.run(main())

    # get data in a sync way
    df = client.read_pd_dataframe(data_params)
    print("read df from grpc", df)
