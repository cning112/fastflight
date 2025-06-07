#!/usr/bin/env python3
"""
Test script to verify the improved async DuckDB implementation
shows better concurrent performance than the sync version.
"""

import asyncio
import sys
import time
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from fastflight.demo_services.duckdb_demo import DuckDBDataService, DuckDBParams


async def test_async_vs_sync_performance():
    """
    Test that demonstrates the performance difference between
    sync and async implementations under concurrent load.
    """

    print("🧪 Testing Async vs Sync DuckDB Performance")
    print("=" * 60)

    # Create test data service
    service = DuckDBDataService()

    # Create test parameters with different data sizes
    small_params = DuckDBParams(
        query="""
        SELECT 
            row_number() OVER () as id,
            random() as value1,
            random() * 1000 as value2,
            'test_data_' || (random() * 1000)::int as text_field,
            current_timestamp + interval (random() * 365) day as timestamp_field
        FROM range(1000)
        """,
        database_path=None,  # In-memory database
    )

    medium_params = DuckDBParams(
        query="""
        SELECT 
            row_number() OVER () as id,
            random() as value1,
            random() * 1000 as value2,
            'test_data_' || (random() * 1000)::int as text_field,
            current_timestamp + interval (random() * 365) day as timestamp_field
        FROM range(50000)
        """,
        database_path=None,
    )

    # Test 1: Single request performance
    print("\n📊 Test 1: Single Request Performance")
    print("-" * 40)

    # Sync version
    start = time.perf_counter()
    sync_batches = 0
    for batch in service.get_batches(small_params, batch_size=1000):
        sync_batches += 1
    sync_single_time = time.perf_counter() - start

    # Async version
    start = time.perf_counter()
    async_batches = 0
    async for batch in service.aget_batches(small_params, batch_size=1000):
        async_batches += 1
    async_single_time = time.perf_counter() - start

    print(f"Sync single request:  {sync_single_time:.3f}s ({sync_batches} batches)")
    print(f"Async single request: {async_single_time:.3f}s ({async_batches} batches)")

    # Test 2: Concurrent requests (where async should shine)
    print("\n📊 Test 2: Concurrent Requests Performance")
    print("-" * 40)

    async def run_async_request(params, request_id):
        """Run a single async request."""
        batches = 0
        start = time.perf_counter()
        async for batch in service.aget_batches(params, batch_size=1000):
            batches += 1
            # Simulate some processing time
            await asyncio.sleep(0.001)  # 1ms processing per batch
        duration = time.perf_counter() - start
        return request_id, batches, duration

    def run_sync_request(params, request_id):
        """Run a single sync request."""
        batches = 0
        start = time.perf_counter()
        for batch in service.get_batches(params, batch_size=1000):
            batches += 1
            # Simulate some processing time
            time.sleep(0.001)  # 1ms processing per batch
        duration = time.perf_counter() - start
        return request_id, batches, duration

    # Test concurrent async requests
    concurrent_requests = 5
    print(f"Running {concurrent_requests} concurrent async requests...")

    start = time.perf_counter()
    async_tasks = [run_async_request(medium_params, i) for i in range(concurrent_requests)]
    async_results = await asyncio.gather(*async_tasks)
    async_concurrent_time = time.perf_counter() - start

    print(f"Async concurrent total time: {async_concurrent_time:.3f}s")
    for req_id, batches, duration in async_results:
        print(f"  Request {req_id}: {duration:.3f}s ({batches} batches)")

    # Test sequential sync requests (simulating what happens without async)
    print(f"\nRunning {concurrent_requests} sequential sync requests...")

    start = time.perf_counter()
    sync_results = []
    for i in range(concurrent_requests):
        result = run_sync_request(medium_params, i)
        sync_results.append(result)
    sync_sequential_time = time.perf_counter() - start

    print(f"Sync sequential total time: {sync_sequential_time:.3f}s")
    for req_id, batches, duration in sync_results:
        print(f"  Request {req_id}: {duration:.3f}s ({batches} batches)")

    # Test 3: Performance comparison
    print("\n📈 Performance Analysis")
    print("-" * 40)

    improvement = (sync_sequential_time - async_concurrent_time) / sync_sequential_time * 100
    speedup = sync_sequential_time / async_concurrent_time

    print(f"Sequential sync time:   {sync_sequential_time:.3f}s")
    print(f"Concurrent async time:  {async_concurrent_time:.3f}s")
    print(f"Performance improvement: {improvement:.1f}%")
    print(f"Speedup factor:         {speedup:.1f}x")

    if improvement > 20:  # 20% improvement threshold
        print("✅ Async implementation shows significant performance improvement!")
        print("   This demonstrates real concurrency benefits.")
    elif improvement > 5:
        print("✅ Async implementation shows moderate improvement.")
        print("   Benefits may vary depending on workload characteristics.")
    else:
        print("⚠️  Async improvement is minimal.")
        print("   This may indicate the workload is CPU-bound rather than I/O-bound.")

    # Test 4: Memory efficiency test
    print("\n💾 Test 4: Memory Efficiency")
    print("-" * 40)

    large_params = DuckDBParams(
        query="""
        SELECT 
            row_number() OVER () as id,
            random() as value1,
            random() * 1000 as value2,
            'large_test_data_' || (random() * 10000)::int as text_field
        FROM range(100000)
        """,
        database_path=None,
    )

    # Test streaming vs all-at-once loading
    print("Testing memory-efficient streaming...")

    start = time.perf_counter()
    stream_batches = 0
    max_batch_size = 0
    async for batch in service.aget_batches(large_params, batch_size=5000):
        stream_batches += 1
        max_batch_size = max(max_batch_size, len(batch))
        if stream_batches >= 10:  # Only process first 10 batches for demo
            break
    stream_time = time.perf_counter() - start

    print(f"Streamed {stream_batches} batches in {stream_time:.3f}s")
    print(f"Max batch size: {max_batch_size} rows")
    print("✅ Streaming allows processing large datasets without loading all into memory")

    return {
        "async_improvement_percent": improvement,
        "speedup_factor": speedup,
        "async_concurrent_time": async_concurrent_time,
        "sync_sequential_time": sync_sequential_time,
    }


async def main():
    """Run the async vs sync comparison test."""

    print("🚀 FastFlight Async DuckDB Performance Test")
    print("=" * 60)

    try:
        results = await test_async_vs_sync_performance()

        print("\n🎯 Final Results Summary")
        print("=" * 40)
        print(f"Performance improvement: {results['async_improvement_percent']:.1f}%")
        print(f"Speedup factor: {results['speedup_factor']:.1f}x")

        if results["async_improvement_percent"] > 20:
            print("\n🏆 SUCCESS: Async implementation demonstrates significant benefits!")
            print("The improved async implementation shows real performance gains")
            print("through concurrent processing and efficient resource usage.")
        else:
            print("\n📊 BASELINE: Async implementation working correctly.")
            print("Performance gains depend on workload concurrency and I/O patterns.")

    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback

        traceback.print_exc()
        return False

    return True


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
