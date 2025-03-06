import dataclasses
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime

import pandas as pd
import pyarrow as pa
from mock_data_service import MockDataParams

from fastflight.client import FastFlightClient


def calculate_throughput(start, end, data_size):
    """Calculate throughput in MB/s."""
    elapsed_time = end - start
    return (data_size / elapsed_time) / (1024 * 1024)


def get_data(client: FastFlightClient, params: MockDataParams) -> tuple[pa.Table, datetime, datetime]:
    start_time = datetime.now()
    table = client.get_pa_table(params)
    end_time = datetime.now()
    return table, start_time, end_time


@dataclasses.dataclass
class Result:
    rows_per_batch: int
    concurrent_requests: int
    delay_per_row: float
    average_latency: float
    throughput_MBps: float


def run(client: FastFlightClient, rows_per_batch: int, delay_per_row: float) -> Result:
    table, start_time, end_time = get_data(
        client, MockDataParams(rows_per_batch=rows_per_batch, delay_per_row=delay_per_row)
    )
    data_size = table.nbytes
    latency = (end_time - start_time).total_seconds()
    throughput = calculate_throughput(start_time.timestamp(), end_time.timestamp(), data_size)
    return Result(rows_per_batch, 1, delay_per_row, latency, throughput)


def run_concurrently(
    client: FastFlightClient, concurrent_requests: int, rows_per_batch: int, delay_per_row: float
) -> Result:
    global_start = datetime.now()
    with ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
        futures = [executor.submit(run, client, rows_per_batch, delay_per_row) for _ in range(concurrent_requests)]

    wait(futures)

    global_end = datetime.now()
    total_time = (global_end - global_start).total_seconds()
    total_data_size = sum(f.result().throughput_MBps * f.result().average_latency for f in futures)
    return Result(
        rows_per_batch=rows_per_batch,
        concurrent_requests=concurrent_requests,
        delay_per_row=delay_per_row,
        average_latency=total_time,
        throughput_MBps=total_data_size / total_time if total_time > 0 else 0,
    )


if __name__ == "__main__":
    # Make sure both servers are running before running the benchmark
    from start_flight_server_async import LOC as async_loc
    from start_flight_server_sync import LOC as sync_loc

    async_results, sync_results = [], []

    # async i/o should be more performant with larger batch size
    for delay_per_row in [1e-6, 1e-5]:
        for rows_per_batch in [1000, 5000, 10_000]:
            # for concurrent_requests in [10, 100, 500, 1000, 2000]:
            for concurrent_requests in [1, 3, 5, 10]:
                print(f"running {rows_per_batch=}, {concurrent_requests=}, {delay_per_row=}")
                async_client = FastFlightClient(async_loc, concurrent_requests)
                sync_client = FastFlightClient(sync_loc, concurrent_requests)

                # pre-warm
                run_concurrently(async_client, concurrent_requests, rows_per_batch, delay_per_row)
                print("async pre-warm done")
                async_results.append(run_concurrently(async_client, concurrent_requests, rows_per_batch, delay_per_row))
                print("async done")

                # pre-warm
                run_concurrently(sync_client, concurrent_requests, rows_per_batch, delay_per_row)
                print("sync pre-warm done")
                sync_results.append(run_concurrently(sync_client, concurrent_requests, rows_per_batch, delay_per_row))
                print("sync done")

    results_df = pd.concat(
        [
            pd.DataFrame(data=[{**dataclasses.asdict(r), "type": "sync"} for r in sync_results]),
            pd.DataFrame(data=[{**dataclasses.asdict(r), "type": "async"} for r in async_results]),
        ]
    )
    # if os.path.exists("results.csv"):
    #     prev_df = pd.read_csv("results.csv")
    #     results_df = pd.concat([prev_df, results_df]).drop_duplicates()
    results_df.to_csv("results.csv", index=False)
