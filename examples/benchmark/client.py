import threading
from datetime import datetime

import psutil
import pyarrow as pa

from fastflight.flight_client import FlightClientManager

from .mock_data_service import MockDataParams

# Server and client details
SERVER_LOCATION = "grpc://localhost:8815"

# Metrics collection
cpu_usage = []
ram_usage = []


def monitor_system_metrics():
    """Collect CPU and RAM usage metrics every second."""
    while True:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        cpu_usage.append(cpu)
        ram_usage.append(ram)


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


def run(nrows: int, ncols: int, batch_size: int):
    # Start monitoring system metrics in a separate thread
    monitor_thread = threading.Thread(target=monitor_system_metrics)
    monitor_thread.daemon = True
    monitor_thread.start()

    # Connect to Flight server
    client = FlightClientManager(SERVER_LOCATION)

    # Send data and collect time
    table, start_time, end_time = get_data(client, MockDataParams(nrows=nrows, ncols=ncols, batch_size=batch_size))

    # Data size in bytes
    data_size = table.nbytes

    # Calculate latency and throughput
    latency = (end_time - start_time).total_seconds()
    throughput = calculate_throughput(start_time.timestamp(), end_time.timestamp(), data_size)

    # Output results
    print(f"Latency: {latency:.2f} seconds")
    print(f"Throughput: {throughput:.2f} MB/s")
    print(f"Average CPU usage: {sum(cpu_usage) / len(cpu_usage):.2f}%")
    print(f"Average RAM usage: {sum(ram_usage) / len(ram_usage):.2f}%")


if __name__ == "__main__":
    run(10_000_000, 100, 100)
