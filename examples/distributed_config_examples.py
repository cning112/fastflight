#!/usr/bin/env python3
"""
Examples showing different distributed processing configurations
"""

import asyncio
import sys
from datetime import datetime, timedelta
from typing import Iterable

import pyarrow as pa

# Add the source path
sys.path.insert(0, "../src")

from fastflight.core.base import BaseDataService
from fastflight.core.distributed import DistributedTimeSeriesService
from fastflight.core.timeseries import TimeSeriesParams


class StockDataParams(TimeSeriesParams):
    """Stock data parameters."""

    symbol: str
    interval: str = "1min"

    def estimate_data_points(self) -> int:
        return int(self.time_range_duration().total_seconds() / 60)


class StockDataService(BaseDataService[StockDataParams]):
    """Mock stock data service."""

    def get_batches(self, params: StockDataParams, batch_size: int = None) -> Iterable[pa.RecordBatch]:
        print(f"    📈 Fetching {params.symbol} data for {params.time_range_duration()}")

        # Mock data generation
        duration_minutes = int(params.time_range_duration().total_seconds() / 60)
        num_points = min(duration_minutes, 20)  # Limit for demo

        if num_points > 0:
            data = {
                "timestamp": [params.start_time + timedelta(minutes=i) for i in range(num_points)],
                "symbol": [params.symbol] * num_points,
                "price": [100.0 + i * 0.1 for i in range(num_points)],
                "volume": [1000 + i * 10 for i in range(num_points)],
            }
            yield pa.record_batch(data)


async def example_default_configuration():
    """Example 1: Default configuration - distributed enabled, auto backend selection"""
    print("\n🚀 Example 1: Default Configuration")
    print("-" * 40)

    base_service = StockDataService()
    service = DistributedTimeSeriesService(base_service)

    info = service.get_backend_info()
    print(f"Backend: {info['backend']}")
    print(f"Distributed: {info['distributed_enabled']}")
    print(f"Max workers: {info['max_workers']}")
    print(f"Ray available: {info['ray_available']}")

    params = StockDataParams(
        symbol="AAPL",
        start_time=datetime(2024, 1, 1, 9, 30),
        end_time=datetime(2024, 1, 1, 16, 0),  # Full trading day
        interval="1min",
    )

    print("\nProcessing with default configuration:")
    batch_count = 0
    async for batch in service.aget_batches(params):
        batch_count += 1
        print(f"  Batch {batch_count}: {batch.num_rows} rows")

    print(f"✅ Processed {batch_count} batches with distributed processing")


async def example_disabled_distribution():
    """Example 2: Disabled distribution - single-threaded processing"""
    print("\n🔧 Example 2: Disabled Distribution")
    print("-" * 40)

    base_service = StockDataService()
    service = DistributedTimeSeriesService(base_service, enable_distributed=False)

    info = service.get_backend_info()
    print(f"Backend: {info['backend']}")
    print(f"Distributed: {info['distributed_enabled']}")
    print(f"Max workers: {info['max_workers']}")

    params = StockDataParams(
        symbol="MSFT",
        start_time=datetime(2024, 1, 1, 10, 0),
        end_time=datetime(2024, 1, 1, 12, 0),  # 2 hours
        interval="1min",
    )

    print("\nProcessing with single-threaded mode:")
    batch_count = 0
    async for batch in service.aget_batches(params):
        batch_count += 1
        print(f"  Batch {batch_count}: {batch.num_rows} rows")

    print(f"✅ Processed {batch_count} batches in single-threaded mode")


async def example_custom_worker_count():
    """Example 3: Custom worker count"""
    print("\n⚙️ Example 3: Custom Worker Count")
    print("-" * 40)

    base_service = StockDataService()

    # Test different worker counts
    worker_configs = [1, 2, 4, 8]

    for workers in worker_configs:
        print(f"\n🔧 Testing with {workers} workers:")
        service = DistributedTimeSeriesService(base_service, max_workers=workers)

        info = service.get_backend_info()
        print(f"  Backend: {info['backend']}")
        print(f"  Configured workers: {info['max_workers']}")

        params = StockDataParams(
            symbol=f"TEST{workers}",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 11, 0),  # 1 hour
            interval="1min",
        )

        batch_count = 0
        async for batch in service.aget_batches(params):
            batch_count += 1

        print(f"  ✅ Processed {batch_count} batches with {workers} workers")


async def example_environment_specific_configs():
    """Example 4: Environment-specific configurations"""
    print("\n🌍 Example 4: Environment-Specific Configurations")
    print("-" * 50)

    base_service = StockDataService()

    # Development environment - single threaded for easy debugging
    print("\n🔧 Development Environment:")
    dev_service = DistributedTimeSeriesService(base_service, enable_distributed=False)
    print(f"  Backend: {dev_service.get_backend_info()['backend']}")
    print("  → Easy debugging, no parallelism complexity")

    # Testing environment - limited workers
    print("\n🧪 Testing Environment:")
    test_service = DistributedTimeSeriesService(base_service, max_workers=2)
    print(f"  Backend: {test_service.get_backend_info()['backend']}")
    print(f"  Workers: {test_service.get_backend_info()['max_workers']}")
    print("  → Controlled parallelism for consistent test results")

    # Production environment - full distributed processing
    print("\n🚀 Production Environment:")
    prod_service = DistributedTimeSeriesService(
        base_service
        # ray_address="ray://production-cluster:10001"  # If using Ray cluster
    )
    print(f"  Backend: {prod_service.get_backend_info()['backend']}")
    print(f"  Workers: {prod_service.get_backend_info()['max_workers']}")
    print("  → Maximum performance with auto-scaling")


def example_configuration_patterns():
    """Example 5: Common configuration patterns"""
    print("\n📋 Example 5: Common Configuration Patterns")
    print("-" * 50)

    base_service = StockDataService()

    print("\n🎯 Pattern 1: Performance-first (default)")
    perf_service = DistributedTimeSeriesService(base_service)
    print(f"  → Auto-selects best backend: {perf_service.get_backend_info()['backend']}")

    print("\n🐛 Pattern 2: Debug-friendly")
    debug_service = DistributedTimeSeriesService(base_service, enable_distributed=False)
    print(f"  → Single-threaded: {debug_service.get_backend_info()['backend']}")

    print("\n⚖️ Pattern 3: Resource-constrained")
    limited_service = DistributedTimeSeriesService(base_service, max_workers=2)
    print(f"  → Limited workers: {limited_service.get_backend_info()['max_workers']}")

    print("\n🎛️ Pattern 4: Explicit control")
    explicit_service = DistributedTimeSeriesService(
        base_service,
        enable_distributed=True,
        max_workers=4,
        ray_address=None,  # Local Ray only
    )
    print(f"  → Explicit config: {explicit_service.get_backend_info()['backend']}")


async def main():
    """Run all examples."""
    print("🔧 DistributedTimeSeriesService Configuration Examples")
    print("=" * 60)

    await example_default_configuration()
    await example_disabled_distribution()
    await example_custom_worker_count()
    await example_environment_specific_configs()
    example_configuration_patterns()

    print("\n" + "=" * 60)
    print("🎉 All configuration examples completed!")

    print("\n💡 Key Takeaways:")
    print("   • enable_distributed=True: Uses Ray or AsyncIO for parallel processing")
    print("   • enable_distributed=False: Forces single-threaded processing")
    print("   • max_workers: Controls the number of parallel workers")
    print("   • Same API works across all configurations")
    print("   • Backend selection is automatic and intelligent")

    print("\n🚀 Choose the right configuration for your use case:")
    print("   • Development: enable_distributed=False (easy debugging)")
    print("   • Testing: max_workers=2 (consistent results)")
    print("   • Production: default settings (maximum performance)")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Examples interrupted by user")
    except Exception as e:
        print(f"\n❌ Examples failed: {e}")
        import traceback

        traceback.print_exc()
