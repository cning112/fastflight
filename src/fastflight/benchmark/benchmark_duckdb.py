import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple

from fastflight.demo_services.duckdb_demo import DuckDBDataService, DuckDBParams


class DuckDBSyncClient:
    def __init__(self, params: DuckDBParams, batch_size: int = 10000):
        self.params = params
        self.batch_size = batch_size
        self.service = DuckDBDataService()

    def run(self) -> Tuple[float, int]:
        start = time.perf_counter()
        total_rows = 0
        for batch in self.service.get_batches(self.params, self.batch_size):
            total_rows += batch.num_rows
        duration = time.perf_counter() - start
        return duration, total_rows


class DuckDBAsyncClient:
    def __init__(self, params: DuckDBParams, batch_size: int = 10000):
        self.params = params
        self.batch_size = batch_size
        self.service = DuckDBDataService()

    async def run(self) -> Tuple[float, int]:
        start = time.perf_counter()
        total_rows = 0
        async for batch in self.service.aget_batches(self.params, self.batch_size):
            total_rows += batch.num_rows
        duration = time.perf_counter() - start
        return duration, total_rows


async def run_concurrent_async_benchmark(params: DuckDBParams, batch_size: int, concurrency: int) -> Tuple[float, int]:
    clients = [DuckDBAsyncClient(params, batch_size) for _ in range(concurrency)]

    async def run_one(client):
        return await client.run()

    start = time.perf_counter()
    results = await asyncio.gather(*[run_one(c) for c in clients])
    duration = time.perf_counter() - start
    total_rows = sum(r[1] for r in results)
    return duration, total_rows


def run_concurrent_sync_benchmark(params: DuckDBParams, batch_size: int, concurrency: int) -> Tuple[float, int]:
    clients = [DuckDBSyncClient(params, batch_size) for _ in range(concurrency)]

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(lambda c: c.run(), clients))
    duration = time.perf_counter() - start

    total_rows = sum(r[1] for r in results)
    return duration, total_rows


async def run_duckdb_benchmark():
    params = DuckDBParams(
        query="""
              SELECT
                  range AS id,
                  random() AS value1,
                  random() * 1000 AS value2,
                  'text_' || (random() * 1000)::int AS text_field,
                  current_timestamp + interval (random() * 365) day AS timestamp_field
              FROM range(1000000)
              """,
        database_path=":memory:",
    )

    sync_client = DuckDBSyncClient(params)
    async_client = DuckDBAsyncClient(params)
    concurrency = 5
    batch_size = 10000

    print("\n🧪 Running sync benchmark...")
    sync_duration, sync_rows = sync_client.run()
    print(f"🔁 Sync took {sync_duration:.2f}s for {sync_rows} rows")

    print("\n⚡ Running async benchmark...")
    async_duration, async_rows = await async_client.run()
    print(f"⚡ Async took {async_duration:.2f}s for {async_rows} rows")

    print(f"\n🧵 Running concurrent sync benchmark with {concurrency} threads...")
    concurrent_sync_duration, concurrent_sync_rows = run_concurrent_sync_benchmark(
        params, batch_size=batch_size, concurrency=concurrency
    )
    print(f"🧵 Concurrent Sync took {concurrent_sync_duration:.2f}s for {concurrent_sync_rows} rows")

    print(f"\n⚡ Running concurrent async benchmark with {concurrency} tasks...")
    concurrent_async_duration, concurrent_async_rows = await run_concurrent_async_benchmark(
        params, batch_size=batch_size, concurrency=concurrency
    )
    print(f"⚡ Concurrent Async took {concurrent_async_duration:.2f}s for {concurrent_async_rows} rows")

    print("\n📊 Summary:")
    print(
        f"  • Total rows: {sync_rows} (sync), {async_rows} (async), "
        f"{concurrent_sync_rows} (concurrent sync), {concurrent_async_rows} (concurrent async)"
    )
    print(f"  • Sync duration: {sync_duration:.2f}s")
    print(f"  • Async duration: {async_duration:.2f}s")
    print(f"  • Concurrent sync duration: {concurrent_sync_duration:.2f}s")
    print(f"  • Concurrent async duration: {concurrent_async_duration:.2f}s")
    print(f"  • Speedup (async over sync): {sync_duration / async_duration:.2f}x")
    print(
        f"  • Speedup (concurrent sync vs serial sync ×{concurrency}): "
        f"{(sync_duration * concurrency) / concurrent_sync_duration:.2f}x"
    )
    print(
        f"  • Speedup (concurrent async vs serial sync ×{concurrency}): "
        f"{(sync_duration * concurrency) / concurrent_async_duration:.2f}x"
    )


if __name__ == "__main__":
    asyncio.run(run_duckdb_benchmark())
