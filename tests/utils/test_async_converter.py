"""
Advanced tests for AsyncToSyncConverter, focusing on edge cases and stress testing.
"""

import asyncio
import logging
import threading
import time
import unittest

from fastflight.utils.stream_utils import AsyncToSyncConverter


class TestAsyncToSyncConverterAdvanced(unittest.TestCase):
    """Advanced test cases for AsyncToSyncConverter focusing on edge cases and concurrency."""

    def setUp(self):
        """Set up test environment before each test."""
        # Set up logging
        logging.basicConfig(level=logging.DEBUG)
        # Create a converter for each test
        self.converter = AsyncToSyncConverter()

    def tearDown(self):
        """Clean up after each test."""
        self.converter.close()

    def test_nested_coroutines(self):
        """Test handling of nested coroutines."""

        async def inner_coro(value):
            await asyncio.sleep(0.01)  # Small delay
            return value * 2

        async def outer_coro(value):
            # Call inner coroutine
            result = await inner_coro(value)
            return result + 1

        result = self.converter.run_coroutine(outer_coro(5))
        self.assertEqual(result, 11)  # (5*2) + 1 = 11

    def test_parallel_runs(self):
        """Test running multiple coroutines in parallel through the converter."""

        async def delayed_return(value, delay):
            await asyncio.sleep(delay)
            return value

        # Create multiple coroutines with different delays
        values = []

        def run_task(val, delay):
            result = self.converter.run_coroutine(delayed_return(val, delay))
            values.append(result)

        # Run multiple calls in parallel from different threads
        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=run_task,
                args=(i, 0.05),  # Small but noticeable delay
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify all expected values were received
        self.assertEqual(sorted(values), [0, 1, 2, 3, 4])

    def test_long_running_coroutine(self):
        """Test behavior with a long-running coroutine."""

        async def long_runner():
            # Simulate a CPU-bound operation
            start = time.time()
            await asyncio.sleep(0.1)  # Use sleep to simulate work
            # Do some dummy calculations
            result = 0
            for i in range(1000000):
                result += i % 2
            end = time.time()
            return end - start, result

        # This should complete without blocking too long
        duration, result = self.converter.run_coroutine(long_runner())
        self.assertGreater(duration, 0.1)  # Should take at least the sleep time
        self.assertEqual(result, 500000)  # Half of numbers from 0-999999 are odd

    def test_many_small_coroutines(self):
        """Test handling of many small coroutines in quick succession."""

        async def small_coro(value):
            await asyncio.sleep(0.001)  # Very small delay
            return value

        # Run many coroutines in quick succession
        results = []
        for i in range(100):
            results.append(self.converter.run_coroutine(small_coro(i)))

        # Verify all expected results
        self.assertEqual(results, list(range(100)))

    def test_asyncgen_with_different_yield_rates(self):
        """Test async generator with different yield rates."""

        async def variable_rate_gen():
            # Start with fast yields
            for i in range(5):
                yield i

            # Then slower yields
            for i in range(5, 10):
                await asyncio.sleep(0.01)
                yield i

        results = list(self.converter.syncify_async_iter(variable_rate_gen()))
        self.assertEqual(results, list(range(10)))

    def test_exception_propagation_in_nested_coroutines(self):
        """Test that exceptions propagate correctly through nested coroutines."""

        class CustomError(Exception):
            pass

        async def inner_failing_coro():
            await asyncio.sleep(0.01)
            raise CustomError("Test error")

        async def outer_coro():
            try:
                await inner_failing_coro()
            except CustomError as e:
                return str(e)

        result = self.converter.run_coroutine(outer_coro())
        self.assertEqual(result, "Test error")

        # Test when exception is not caught in inner coroutine
        async def outer_without_try():
            await inner_failing_coro()
            return "This should not be reached"

        with self.assertRaises(CustomError):
            self.converter.run_coroutine(outer_without_try())

    def test_stress_with_many_threads(self):
        """Stress test with many threads using the converter concurrently."""

        async def simple_coro(value):
            # Small random delay to increase chance of concurrency issues
            await asyncio.sleep(0.001 * (value % 5))
            return value

        results = []
        lock = threading.Lock()

        def thread_work(start_idx, count):
            thread_results = []
            for i in range(start_idx, start_idx + count):
                thread_results.append(self.converter.run_coroutine(simple_coro(i)))

            with lock:
                results.extend(thread_results)

        # Create and start many threads
        threads = []
        thread_count = 10
        items_per_thread = 20

        for i in range(thread_count):
            thread = threading.Thread(target=thread_work, args=(i * items_per_thread, items_per_thread))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify we got all expected results (might be out of order)
        self.assertEqual(len(results), thread_count * items_per_thread)
        self.assertEqual(sorted(results), list(range(thread_count * items_per_thread)))

    def test_cancellation_during_iterator(self):
        """Test behavior when cancelling during iteration."""

        async def slow_gen():
            for i in range(10):
                await asyncio.sleep(0.1)
                yield i

        # Start iterating in another thread
        results = []
        stop_event = threading.Event()

        def iterate_and_collect():
            for value in self.converter.syncify_async_iter(slow_gen()):
                results.append(value)
                if stop_event.is_set():
                    break

        thread = threading.Thread(target=iterate_and_collect)
        thread.start()

        # Let it run for a short time then set stop event
        time.sleep(0.25)  # Should allow ~2-3 items to be yielded
        stop_event.set()

        # Wait for thread to complete
        thread.join(timeout=1.0)
        self.assertFalse(thread.is_alive())  # Thread should have completed

        # Check we got some but not all results
        self.assertGreater(len(results), 0)
        self.assertLess(len(results), 10)

    def test_asyncgen_that_never_yields(self):
        """Test handling of an async generator that never yields any values."""

        async def never_yield():
            if False:  # This condition is never met
                yield 1

        results = list(self.converter.syncify_async_iter(never_yield()))
        self.assertEqual(results, [])

    def test_multiple_converters(self):
        """Test using multiple converters simultaneously."""

        async def coro_with_id(converter_id, value):
            await asyncio.sleep(0.01)
            return f"Converter {converter_id}: {value}"

        # Create several converters
        converters = [AsyncToSyncConverter() for _ in range(3)]

        try:
            # Run coroutines in each converter
            results = []
            for i, converter in enumerate(converters):
                results.append(converter.run_coroutine(coro_with_id(i, 100)))

            # Verify results
            self.assertEqual(len(results), 3)
            for i, result in enumerate(results):
                self.assertEqual(result, f"Converter {i}: 100")
        finally:
            # Clean up all converters
            for converter in converters:
                converter.close()


if __name__ == "__main__":
    unittest.main()
