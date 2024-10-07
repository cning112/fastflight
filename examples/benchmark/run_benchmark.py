import dataclasses
import os
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime

import pandas as pd
import pyarrow as pa
from mock_data_service import MockDataParams

from fastflight.flight_client import FlightClientManager


def calculate_throughput(start, end, data_size):
    """Calculate throughput in MB/s."""
    elapsed_time = end - start
    return (data_size / elapsed_time) / (1024 * 1024)


def get_data(client: FlightClientManager, params: MockDataParams) -> tuple[pa.Table, datetime, datetime]:
    start_time = datetime.now()
    table = client.read_pa_table(params)
    end_time = datetime.now()
    return table, start_time, end_time


@dataclasses.dataclass
class Result:
    records_per_batch: int
    concurrent_requests: int
    batch_generation_delay: float
    average_latency: float
    throughput_MBps: float


def run(client: FlightClientManager, records_per_batch: int, batch_generation_delay: float) -> Result:
    table, start_time, end_time = get_data(
        client, MockDataParams(records_per_batch=records_per_batch, batch_generation_delay=batch_generation_delay)
    )
    data_size = table.nbytes
    latency = (end_time - start_time).total_seconds()
    throughput = calculate_throughput(start_time.timestamp(), end_time.timestamp(), data_size)
    return Result(records_per_batch, 1, batch_generation_delay, latency, throughput)


def run_concurrently(
    client: FlightClientManager, concurrent_requests: int, records_per_batch: int, batch_generation_delay: float
) -> Result:
    futures = []
    with ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
        f = executor.submit(run, client, records_per_batch, batch_generation_delay)
        futures.append(f)

    wait(futures)

    return Result(
        records_per_batch=records_per_batch,
        concurrent_requests=concurrent_requests,
        batch_generation_delay=batch_generation_delay,
        average_latency=sum(f.result().average_latency for f in futures) / len(futures),
        throughput_MBps=sum(f.result().throughput_MBps for f in futures) / len(futures),
    )


if __name__ == "__main__":
    async_loc = "grpc://127.0.0.1:8815"
    sync_loc = "grpc://127.0.0.1:8816"

    async_results, sync_results = [], []

    # async i/o should be more performant with larger batch size
    for batch_generation_delay in [0.001, 0.01, 0.1]:
        for records_per_batch in [1000, 5000, 10_000]:
            for concurrent_requests in [10, 100, 500, 1000, 2000]:
                print(f"running {records_per_batch=}, {concurrent_requests=}, {batch_generation_delay=}")
                async_client = FlightClientManager(async_loc, concurrent_requests)
                sync_client = FlightClientManager(sync_loc, concurrent_requests)

                # pre-warm
                run_concurrently(async_client, concurrent_requests, records_per_batch, 0.0)
                print("async pre-warm done")
                async_results.append(
                    run_concurrently(async_client, concurrent_requests, records_per_batch, batch_generation_delay)
                )
                print("async done")

                # pre-warm
                run_concurrently(sync_client, concurrent_requests, records_per_batch, 0.0)
                print("sync pre-warm done")
                sync_results.append(
                    run_concurrently(sync_client, concurrent_requests, records_per_batch, batch_generation_delay)
                )
                print("sync done")

    results_df = pd.concat(
        [
            pd.DataFrame(data=[{**dataclasses.asdict(r), "type": "sync"} for r in sync_results]),
            pd.DataFrame(data=[{**dataclasses.asdict(r), "type": "async"} for r in async_results]),
        ]
    )
    if os.path.exists("results.csv"):
        prev_df = pd.read_csv("results.csv")
        results_df = pd.concat([prev_df, results_df]).drop_duplicates()
    results_df.to_csv("results.csv", index=False)
