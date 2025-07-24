import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

from fastflight.utils.stream_utils import AsyncToSyncConverter, get_thread_local_converter


class TestAsyncToSyncPerformance:
    async def simple_async_func(self, x):
        """Simple async function for testing"""
        await asyncio.sleep(0.005)  # 5ms
        return x * 2

    def test_converter_overhead(self):
        """Test converter overhead"""
        converter = AsyncToSyncConverter()

        # Test direct async calls
        async def direct_async():
            start = time.perf_counter()
            results = []
            for i in range(100):
                result = await self.simple_async_func(i)
                results.append(result)
            end = time.perf_counter()
            return end - start, results

        direct_time, direct_results = asyncio.run(direct_async())

        # Test calls through converter
        start = time.perf_counter()
        converter_results = []
        for i in range(100):
            result = converter.run_coroutine(self.simple_async_func(i))
            converter_results.append(result)
        end = time.perf_counter()
        converter_time = end - start

        converter.close()

        # Calculate overhead
        overhead_ratio = converter_time / direct_time
        print(f"Direct async: {direct_time:.4f}s")
        print(f"Converter: {converter_time:.4f}s")
        print(f"Overhead ratio: {overhead_ratio:.2f}x")

        assert direct_results == converter_results
        assert overhead_ratio < 5  # Overhead should be less than 5x

    def test_concurrent_performance(self):
        """Test concurrent performance"""

        def sync_worker(converter, task_count):
            start = time.perf_counter()
            for i in range(task_count):
                converter.run_coroutine(self.simple_async_func(i))
            return time.perf_counter() - start

        async def async_worker(task_count):
            start = time.perf_counter()
            tasks = [self.simple_async_func(i) for i in range(task_count)]
            await asyncio.gather(*tasks)
            return time.perf_counter() - start

        task_count = 100
        thread_count = 4

        # Test concurrent sync calls
        converters = [AsyncToSyncConverter() for _ in range(thread_count)]
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            sync_start = time.perf_counter()
            futures = [executor.submit(sync_worker, converters[i], task_count) for i in range(thread_count)]
            for f in futures:
                f.result()
            sync_total = time.perf_counter() - sync_start

        for converter in converters:
            converter.close()

        # Test pure async
        async_total = asyncio.run(async_worker(task_count * thread_count))

        print(f"Concurrent sync (4 threads): {sync_total:.4f}s")
        print(f"Pure async: {async_total:.4f}s")
        print(f"Async advantage: {sync_total / async_total:.2f}x")

        # Async should be significantly faster
        assert async_total * 3 < sync_total


def benchmark_converter():
    """Quick benchmark test"""

    async def dummy_async():
        await asyncio.sleep(0.001)
        return "done"

    # Test 1000 calls
    converter = get_thread_local_converter()

    start = time.perf_counter()
    for _ in range(1000):
        converter.run_coroutine(dummy_async())
    duration = time.perf_counter() - start

    print(f"1000 calls took {duration:.3f}s")
    print(f"Average per call: {duration * 1000:.3f}ms")

    return duration


if __name__ == "__main__":
    benchmark_converter()
