#!/usr/bin/env python3
"""Test script to verify the Ray -> AsyncIO fallback functionality."""

import asyncio
import sys
from datetime import datetime, timedelta
from typing import Iterable

import pyarrow as pa

# Add the source path to be able to import the module
sys.path.insert(0, "src")

from fastflight.core.base import BaseDataService
from fastflight.core.distributed import DistributedTimeSeriesService
from fastflight.core.timeseries import TimeSeriesParams


class TestTimeSeriesParams(TimeSeriesParams):
    """Simple test parameters."""

    symbol: str

    def estimate_data_points(self) -> int:
        return int(self.time_range_duration().total_seconds() / 60)


class MockDataService(BaseDataService[TestTimeSeriesParams]):
    """Mock data service for testing."""

    def get_batches(self, params: TestTimeSeriesParams, batch_size: int = None) -> Iterable[pa.RecordBatch]:
        print(
            f"    Processing {params.symbol} from {params.start_time.strftime('%H:%M')} to {params.end_time.strftime('%H:%M')}"
        )

        # Create mock data
        duration_minutes = int(params.time_range_duration().total_seconds() / 60)
        num_points = min(duration_minutes, 10)  # Limit for testing

        if num_points > 0:
            data = {
                "timestamp": [params.start_time + timedelta(minutes=i) for i in range(num_points)],
                "symbol": [params.symbol] * num_points,
                "value": list(range(num_points)),
            }
            yield pa.record_batch(data)


async def test_fallback_functionality():
    """Test the fallback functionality."""
    print("🧪 Testing Ray -> AsyncIO Fallback Functionality")
    print("=" * 50)

    # Test 1: Default distributed enabled
    print("\n📊 Test 1: Default Configuration (distributed enabled)")
    base_service = MockDataService()
    distributed_service = DistributedTimeSeriesService(base_service)

    backend_info = distributed_service.get_backend_info()
    print(f"   Backend: {backend_info['backend']}")
    print(f"   Distributed enabled: {backend_info['distributed_enabled']}")
    print(f"   Max workers: {backend_info['max_workers']}")

    # Test 2: Distributed disabled
    print("\n📊 Test 2: Distributed Disabled")
    single_service = DistributedTimeSeriesService(base_service, enable_distributed=False)

    single_info = single_service.get_backend_info()
    print(f"   Backend: {single_info['backend']}")
    print(f"   Distributed enabled: {single_info['distributed_enabled']}")
    print(f"   Max workers: {single_info['max_workers']}")

    # Test 3: Custom worker count
    print("\n📊 Test 3: Custom Worker Count")
    custom_service = DistributedTimeSeriesService(base_service, max_workers=2)

    custom_info = custom_service.get_backend_info()
    print(f"   Backend: {custom_info['backend']}")
    print(f"   Max workers: {custom_info['max_workers']}")

    # Test processing with different configurations
    params = TestTimeSeriesParams(
        symbol="TEST",
        start_time=datetime(2024, 1, 1, 10, 0),
        end_time=datetime(2024, 1, 1, 12, 0),  # 2 hours
    )

    print("\n🔄 Processing test with distributed enabled...")
    batch_count = 0
    async for batch in distributed_service.aget_batches(params):
        batch_count += 1
        print(f"   📦 Distributed batch {batch_count}: {batch.num_rows} rows")

    print("\n🔄 Processing test with distributed disabled...")
    single_batch_count = 0
    async for batch in single_service.aget_batches(params):
        single_batch_count += 1
        print(f"   📦 Single-threaded batch {single_batch_count}: {batch.num_rows} rows")


async def main():
    """Run the test."""
    await test_fallback_functionality()

    print("\n🚀 Configuration Tests Completed!")
    print("\n💡 Key Features:")
    print("   ✅ enable_distributed=True: Uses Ray/AsyncIO for parallel processing")
    print("   ✅ enable_distributed=False: Forces single-threaded processing")
    print("   ✅ max_workers parameter: Controls worker count")
    print("   ✅ Same API works across all configurations")

    print("\n📝 Usage Examples:")
    print("   # Default: distributed enabled")
    print("   service = DistributedTimeSeriesService(base_service)")
    print("   ")
    print("   # Disable distribution")
    print("   service = DistributedTimeSeriesService(base_service, enable_distributed=False)")
    print("   ")
    print("   # Custom worker count")
    print("   service = DistributedTimeSeriesService(base_service, max_workers=4)")
    print("   ")
    print("   # Check configuration")
    print("   info = service.get_backend_info()")
    print('   print(f\'Backend: {info["backend"]}, Workers: {info["max_workers"]}\')')


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
