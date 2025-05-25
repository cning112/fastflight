"""Example usage of TimeSeriesParams with distributed processing."""

import asyncio
from datetime import datetime, timedelta
from typing import Iterable

import pyarrow as pa

from fastflight.core.base import BaseDataService
from fastflight.core.optimization import OptimizationHint, optimize_time_series_query
from fastflight.core.timeseries import TimeSeriesParams

# Optional - only if Ray is available
try:
    from fastflight.core.distributed import DistributedTimeSeriesService

    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False


# 1. Define your time series params
class StockDataParams(TimeSeriesParams):
    symbol: str
    interval: str = "1min"  # 1min, 5min, 1hour, 1day

    def estimate_data_points(self) -> int:
        """Estimate based on interval and time range."""
        duration = self.time_range_duration()

        if self.interval == "1min":
            return int(duration.total_seconds() / 60)
        elif self.interval == "5min":
            return int(duration.total_seconds() / 300)
        elif self.interval == "1hour":
            return int(duration.total_seconds() / 3600)
        else:  # daily
            return duration.days


# 2. Implement your data service
class StockDataService(BaseDataService[StockDataParams]):
    def get_batches(self, params: StockDataParams, batch_size: int = None) -> Iterable[pa.RecordBatch]:
        # Mock implementation - replace with actual data fetching
        num_points = min(params.estimate_data_points(), 1000)  # Limit for demo

        data = {
            "timestamp": [params.start_time + timedelta(minutes=i) for i in range(num_points)],
            "symbol": [params.symbol] * num_points,
            "price": [100.0 + i * 0.1 for i in range(num_points)],
            "volume": [1000 + i * 10 for i in range(num_points)],
        }
        yield pa.record_batch(data)

    async def aget_batches(self, params: StockDataParams, batch_size: int = None):
        for batch in self.get_batches(params, batch_size):
            yield batch


# 3. Examples
async def demo_partitioning():
    """Demonstrate different partitioning strategies."""
    print("=== Time Series Partitioning Demo ===")

    params = StockDataParams(
        symbol="AAPL",
        interval="1min",
        start_time=datetime(2024, 1, 1, 9, 0),
        end_time=datetime(2024, 1, 1, 16, 0),  # 7 hours of trading
    )

    print(f"Original query: {params.start_time} to {params.end_time}")
    print(f"Duration: {params.time_range_duration()}")
    print(f"Estimated data points: {params.estimate_data_points()}")

    # Auto-partitioning
    partitions = params.get_optimal_partitions(max_workers=4)
    print(f"\nAuto-partitioning (4 workers): {len(partitions)} partitions")

    # Fixed window partitioning
    hourly_partitions = params.split_by_window_size(timedelta(hours=1))
    print(f"Hourly partitioning: {len(hourly_partitions)} partitions")

    # Optimization-based partitioning
    real_time_hint = OptimizationHint.for_real_time()
    rt_partitions = optimize_time_series_query(params, real_time_hint)
    print(f"Real-time optimized: {len(rt_partitions)} partitions")

    analytics_hint = OptimizationHint.for_analytics()
    analytics_partitions = optimize_time_series_query(params, analytics_hint)
    print(f"Analytics optimized: {len(analytics_partitions)} partitions")


async def demo_regular_processing():
    """Demonstrate regular (non-distributed) processing."""
    print("\n=== Regular Processing Demo ===")

    service = StockDataService()
    params = StockDataParams(
        symbol="TSLA",
        interval="5min",
        start_time=datetime(2024, 1, 1, 10, 0),
        end_time=datetime(2024, 1, 1, 11, 0),  # 1 hour
    )

    batch_count = 0
    total_rows = 0

    async for batch in service.aget_batches(params):
        batch_count += 1
        total_rows += batch.num_rows
        print(f"Batch {batch_count}: {batch.num_rows} rows")

    print(f"Total: {batch_count} batches, {total_rows} rows")


async def demo_distributed_processing():
    """Demonstrate distributed processing with Ray."""
    if not RAY_AVAILABLE:
        print("\n=== Distributed Processing Demo ===")
        print("Ray not available - skipping distributed demo")
        print("Install Ray with: pip install ray")
        return

    print("\n=== Distributed Processing Demo ===")

    base_service = StockDataService()
    distributed_service = DistributedTimeSeriesService(base_service)

    params = StockDataParams(
        symbol="NVDA",
        interval="1min",
        start_time=datetime(2024, 1, 1, 9, 0),
        end_time=datetime(2024, 1, 1, 17, 0),  # 8 hours
    )

    print(f"Processing {params.estimate_data_points()} estimated data points")

    # Test ordered processing (default)
    print("\n--- Ordered Processing (preserve_order=True) ---")
    batch_count = 0
    start_time = datetime.now()

    async for batch in distributed_service.aget_batches(params, preserve_order=True):
        batch_count += 1
        first_timestamp = batch.column("timestamp").to_pylist()[0]
        print(f"Ordered batch {batch_count}: {batch.num_rows} rows, starts at {first_timestamp.strftime('%H:%M')}")

    ordered_time = datetime.now() - start_time
    print(f"Ordered total: {batch_count} batches in {ordered_time.total_seconds():.1f}s")

    # Test unordered processing for comparison
    print("\n--- Unordered Processing (preserve_order=False) ---")
    batch_count = 0
    start_time = datetime.now()

    async for batch in distributed_service.aget_batches(params, preserve_order=False):
        batch_count += 1
        first_timestamp = batch.column("timestamp").to_pylist()[0]
        print(f"Unordered batch {batch_count}: {batch.num_rows} rows, starts at {first_timestamp.strftime('%H:%M')}")

    unordered_time = datetime.now() - start_time
    print(f"Unordered total: {batch_count} batches in {unordered_time.total_seconds():.1f}s")

    print(f"\nPerformance: Unordered was {((ordered_time - unordered_time) / ordered_time * 100):.1f}% faster")


def demo_optimization_patterns():
    """Demonstrate different optimization patterns."""
    print("\n=== Optimization Patterns Demo ===")

    # Large analytics query
    large_params = StockDataParams(
        symbol="SPY",
        interval="1min",
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 2, 1),  # 1 month
    )

    patterns = [
        ("Real-time", OptimizationHint.for_real_time()),
        ("Analytics", OptimizationHint.for_analytics()),
        ("Default", OptimizationHint()),
    ]

    for name, hint in patterns:
        partitions = optimize_time_series_query(large_params, hint)
        estimated_per_partition = large_params.estimate_data_points() // len(partitions)
        print(f"{name}: {len(partitions)} partitions, ~{estimated_per_partition:,} points each")


async def main():
    """Run all demos."""
    await demo_partitioning()
    await demo_regular_processing()
    await demo_distributed_processing()
    demo_optimization_patterns()


if __name__ == "__main__":
    asyncio.run(main())
