import dataclasses
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime

import matplotlib.pyplot as plt
import pyarrow as pa
from mock_data_service import MockDataParams

from fastflight.flight_client import FlightClientManager


def calculate_throughput(start, end, data_size):
    """Calculate throughput in MB/s."""
    elapsed_time = end - start
    return (data_size / elapsed_time) / (1024 * 1024)


def get_data(client: FlightClientManager, params: MockDataParams) -> tuple[pa.Table, datetime, datetime]:
    """Send the generated data to the client and collect metrics."""
    start_time = datetime.now()
    table = client.read_pa_table(params)
    end_time = datetime.now()
    return table, start_time, end_time


@dataclasses.dataclass
class Result:
    latency_seconds: float
    throughputMB: float


@dataclasses.dataclass
class ResultList:
    results: list[Result]

    @property
    def latency_seconds(self) -> float:
        return sum(result.latency_seconds for result in self.results) / len(self.results)

    @property
    def throughputMB(self) -> float:
        return sum(result.throughputMB for result in self.results) / len(self.results)


def run(client: FlightClientManager, batch_size: int) -> Result:
    # Send data and collect time
    table, start_time, end_time = get_data(client, MockDataParams(batch_size=batch_size))

    # Data size in bytes
    data_size = table.nbytes

    # Calculate latency and throughput
    latency = (end_time - start_time).total_seconds()
    throughput = calculate_throughput(start_time.timestamp(), end_time.timestamp(), data_size)

    # Output results
    # print(f"Latency: {latency:.2f} seconds")
    # print(f"Throughput: {throughput:.2f} MB/s")
    return Result(latency, throughput)


def run_concurrently(client: FlightClientManager, concurrency_level: int, batch_size: int) -> ResultList:
    futures = []
    with ThreadPoolExecutor(max_workers=concurrency_level) as executor:
        f = executor.submit(run, client, batch_size)
        futures.append(f)

    wait(futures)

    return ResultList([f.result() for f in futures])


def plot_results(async_results: dict[int, dict[int, ResultList]], sync_results: dict[int, dict[int, ResultList]]):
    plt.figure(figsize=(10, 6))

    batch_sizes = sorted(async_results.keys())
    colors = ["r", "g", "b", "c", "m", "y", "k", "w"]

    for color, batch_size in zip(colors, batch_sizes):
        concurrency_levels = sorted(async_results[batch_size].keys())

        plt.plot(
            concurrency_levels,
            [sync_results[batch_size][c].throughputMB for c in concurrency_levels],
            label=f"Sync (batch_size={batch_size})",
            marker="o",
            linestyle="--",
            color=color,
        )
        plt.plot(
            concurrency_levels,
            [async_results[batch_size][c].throughputMB for c in concurrency_levels],
            label=f"Async (batch_size={batch_size})",
            marker="^",
            linestyle="-",
            color=color,
        )
        plt.xlabel("Concurrency Level")
        plt.ylabel("Throughput (MB/s)")
        plt.title("Throughput Comparison")
        plt.legend()
        plt.grid(True)
    plt.show()


if __name__ == "__main__":
    async_loc = "grpc://127.0.0.1:8815"
    sync_loc = "grpc://127.0.0.1:8816"

    async_results = defaultdict(dict)
    sync_results = defaultdict(dict)

    # async i/o should be more performant with larger batch size
    for batch_size in [100, 1000, 10_000, 20_000]:
        for concurrency_level in [1, 5, 10, 50, 100]:
            async_client = FlightClientManager(async_loc, concurrency_level)
            sync_client = FlightClientManager(sync_loc, concurrency_level)
            # pre-warm
            run_concurrently(async_client, concurrency_level, batch_size)
            async_results[batch_size][concurrency_level] = run_concurrently(async_client, concurrency_level, batch_size)
            # pre-warm
            run_concurrently(sync_client, concurrency_level, batch_size)
            sync_results[batch_size][concurrency_level] = run_concurrently(sync_client, concurrency_level, batch_size)

    plot_results(async_results, sync_results)
