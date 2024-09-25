import asyncio
import logging
from typing import Callable, Iterable, TypeVar

import httpx

from fastflight.flight_client import FlightClientManager
from fastflight.utils.stream_utils import read_dataframe_from_arrow_stream

logger = logging.getLogger(__name__)

T = TypeVar("T")


def request_for_data(url: str, json_data: dict, read_fn: Callable[[Iterable[bytes]], T]) -> T:
    with httpx.stream("POST", url, json=json_data) as response:
        response.raise_for_status()
        return read_fn(response.iter_bytes())


if __name__ == "__main__":
    # http request
    df = request_for_data(
        "http://127.0.0.1:8000/fastflight/", {"kind": "Demo", "value": 5}, read_dataframe_from_arrow_stream
    )
    print("read df from http", df)

    # grpc request
    client = FlightClientManager("grpc://localhost:8815")

    async def main():
        # b = b'{"connection_string": "sqlite:///example.db", "query": "select 1 as a", "batch_size": 1000, "kind": "DataSource.PostgresSQL"}'
        b = b'{"value": 5, "kind": "Demo"}'
        reader = await client.aget_stream_reader(b)
        for batch in reader:
            print("read batch from grpc", batch.data)

        b = b'{"value": 5, "kind": "Demo"}'
        df = await client.aread_pd_df(b)
        print("read df from grpc", df)

    asyncio.run(main())

    b = b'{"value": 5, "kind": "Demo"}'
    df = client.read_pd_df(b)
    print("read df from grpc", df)
