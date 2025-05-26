"""Distributed processing support with Ray -> AsyncIO fallback."""

import asyncio
import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import AsyncIterable, Callable, Generic, Iterable, Iterator, List, Literal, Optional, Tuple, TypeVar

try:
    import ray

    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False

import pyarrow as pa

from fastflight.core.base import BaseDataService
from fastflight.core.timeseries import TimeSeriesParams

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=TimeSeriesParams)
R = TypeVar("R", bound=Iterator)

# Define backend types as literals for type safety
BackendType = Literal["ray", "asyncio", "single_threaded"]

_dummy = object()


class DistributedTimeSeriesService(BaseDataService[T], Generic[T]):
    """
    Distributed data service with intelligent backend fallback and configurable distribution.

    Execution order: Ray -> AsyncIO -> Single-threaded

    - If Ray is available and distributed enabled: Uses Ray for true distributed processing
    - If Ray unavailable but distributed enabled: Falls back to AsyncIO with thread pool
    - If distributed disabled: Uses single-threaded sequential processing

    Args:
        base_service: The underlying data service to distribute
        ray_address: Ray cluster address (None for local Ray, ignored if Ray unavailable)
        max_workers: Maximum number of workers (auto-detected if None)
        enable_distributed: Whether to enable distributed processing (default: True)

    Examples:
        Basic usage (auto-selects best backend):

        >>> service = StockDataService()
        >>> distributed = DistributedTimeSeriesService(service)
        >>> # Uses Ray or AsyncIO automatically

        Disable distributed processing:

        >>> distributed = DistributedTimeSeriesService(service, enable_distributed=False)
        >>> # Forces single-threaded processing

        Control worker count:

        >>> distributed = DistributedTimeSeriesService(service, max_workers=4)
        >>> # Limits to 4 workers regardless of backend

        The service will automatically:
        1. Try Ray if available and distributed enabled
        2. Fall back to AsyncIO with threading if Ray unavailable
        3. Use single-threaded if distributed disabled
    """

    def __init__(
        self,
        base_service: BaseDataService[T],
        ray_address: Optional[str] = None,
        max_workers: Optional[int] = None,
        enable_distributed: bool = True,
    ):
        self.base_service = base_service
        self.base_service_class = base_service.__class__
        self.ray_address = ray_address
        self.max_workers = max_workers
        self.enable_distributed = enable_distributed

        # Determine which backend to use
        self.backend: BackendType = self._select_backend()
        logger.info(f"Selected backend: {self.backend} (distributed: {self.enable_distributed})")

    def _select_backend(self) -> BackendType:
        """Select the best available backend."""
        # If distributed processing is disabled, use single-threaded
        if not self.enable_distributed:
            return "single_threaded"

        if RAY_AVAILABLE:
            try:
                # Try to initialize Ray if not already done
                if not ray.is_initialized():
                    ray.init(address=self.ray_address)
                return "ray"
            except Exception as e:
                logger.warning(f"Ray initialization failed: {e}, falling back to asyncio")

        # AsyncIO is always available
        return "asyncio"

    async def aget_batches(
        self, params: T, batch_size: int | None = None, preserve_order: bool = True
    ) -> AsyncIterable[pa.RecordBatch]:
        """
        Get batches using the best available distributed processing backend.

        Args:
            params: Time series parameters with start_time, end_time, etc.
            batch_size: Maximum size per batch (passed to underlying service)
            preserve_order: If True, yield batches in time order; if False, yield as completed

        Yields:
            RecordBatch: Arrow record batches
        """
        # Determine optimal partitions
        max_workers = self._get_max_workers()
        partitions = params.get_optimal_partitions(max_workers)

        logger.info(f"Processing {len(partitions)} partitions with {self.backend} backend")

        if self.backend == "ray":
            async for batch in self._process_with_ray(partitions, batch_size, preserve_order):
                yield batch
        elif self.backend == "asyncio":
            async for batch in self._process_with_asyncio(partitions, batch_size, preserve_order):
                yield batch
        else:
            # Single-threaded processing
            async for batch in self._process_single_threaded(partitions, batch_size, preserve_order):
                yield batch

    def get_batches(self, params: T, batch_size: int | None = None, preserve_order: bool = True):
        """
        Synchronous version that avoids AsyncToSyncConverter for better performance.

        This method implements the distributed processing logic directly in synchronous code
        to avoid the single-threaded bottleneck of AsyncToSyncConverter.
        """
        # Determine optimal partitions (CPU work done synchronously)
        max_workers = self._get_max_workers()
        partitions = params.get_optimal_partitions(max_workers)

        logger.info(f"Processing {len(partitions)} partitions with {self.backend} backend")

        if self.backend == "ray":
            yield from self._process_with_ray_sync(partitions, batch_size, preserve_order)
        elif self.backend == "asyncio":
            yield from self._process_with_asyncio_sync(partitions, batch_size, preserve_order)
        else:
            # Single-threaded processing
            yield from self._process_single_threaded_sync(partitions, batch_size, preserve_order)

    def _process_with_ray_sync(self, partitions: List[T], batch_size: Optional[int], preserve_order: bool):
        """Process partitions using Ray - synchronous version."""
        # Submit remote tasks
        futures = [
            process_partition_remote.remote(self.base_service_class, partition, batch_size) for partition in partitions
        ]

        yield from self._stream_results(futures, self._get_ray_result, preserve_order)

    def _process_with_asyncio_sync(self, partitions: List[T], batch_size: Optional[int], preserve_order: bool):
        """Process partitions using AsyncIO - synchronous version."""
        max_workers = min(self._get_max_workers(), len(partitions))

        # For AsyncIO, we need to handle streaming differently since we can't easily
        # check if a generator is "ready" without consuming it
        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            def sync_process_partition(partition: T) -> Iterable[pa.RecordBatch]:
                """Process a single partition and return streaming generator."""
                service = self.base_service_class()
                yield from service.get_batches(partition, batch_size)

            # Submit all tasks
            futures = [executor.submit(sync_process_partition, partition) for partition in partitions]

            yield from self._stream_results(futures, lambda f: f.result(), preserve_order)

    def _process_single_threaded_sync(self, partitions: List[T], batch_size: Optional[int], preserve_order: bool):
        """Process partitions sequentially in single thread."""
        logger.info("Using single-threaded processing (distributed disabled)")

        for partition in partitions:
            try:
                service = self.base_service_class()
                for batch in service.get_batches(partition, batch_size):
                    yield batch
            except Exception as e:
                logger.error(f"Single-threaded partition failed: {e}")
                continue

    @staticmethod
    def _stream_ready_futures(
        futures: List[Future],
        result_getter: Callable[[Future], Iterable[pa.RecordBatch] | None],
        handle_ready: Callable[[List[Tuple[Future, Iterable[pa.RecordBatch]]]], Iterable[pa.RecordBatch]],
    ) -> Iterable[pa.RecordBatch]:
        pending = set(futures)

        while pending:
            # Find ready futures using the result getter
            ready_futures = []
            for future in list(pending):
                try:
                    result = result_getter(future)
                    if result is not None:  # Future is ready
                        ready_futures.append((future, result))
                        pending.remove(future)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
                    pending.remove(future)
                    continue

            # If no futures are ready, wait a bit and try again
            if not ready_futures:
                import time

                time.sleep(0.01)  # 10ms sleep
                continue

            yield from handle_ready(ready_futures)

    @staticmethod
    def _stream_results(
        futures: List[Future], result_getter: Callable[[Future], Iterable[pa.RecordBatch] | None], ordered: bool
    ) -> Iterable[pa.RecordBatch]:
        """
        Stream results in submission order using a generic result getter.

        Args:
            futures: List of futures (Ray ObjectRefs or concurrent.futures.Future)
            result_getter: Function to get result from a future and check if ready
        """
        if ordered:
            results = {}  # future_index -> List[RecordBatch]
            future_to_idx = {future: i for i, future in enumerate(futures)}
            next_expected_idx = 0

            def handle_ready(ready_futures):
                nonlocal next_expected_idx

                # Process ready results
                for future, batches in ready_futures:
                    idx = future_to_idx[future]
                    results[idx] = batches

                    # Yield consecutive completed partitions in order
                    while next_expected_idx in results:
                        for batch in results.pop(next_expected_idx):
                            yield batch
                        next_expected_idx += 1
        else:

            def handle_ready(ready_futures):
                for _, batches in ready_futures:
                    for batch in batches:
                        yield batch

        yield from DistributedTimeSeriesService._stream_ready_futures(futures, result_getter, handle_ready)

    @staticmethod
    def _get_ray_result(future) -> Iterable[pa.RecordBatch] | None:
        """
        Get result from Ray ObjectRef.

        Args:
            future: Ray ObjectRef

        Returns:
            List of batches or None if not ready
        """
        # Check if ready without blocking
        ready, _ = ray.wait([future], num_returns=1, timeout=0)
        if ready:
            return ray.get(future)
        return None

    async def _process_with_ray(
        self, partitions: List[T], batch_size: Optional[int], preserve_order: bool
    ) -> AsyncIterable[pa.RecordBatch]:
        """Process partitions using Ray."""
        # Submit remote tasks
        futures = [
            process_partition_remote.remote(self.base_service_class, partition, batch_size) for partition in partitions
        ]

        if preserve_order:
            async for batch in self._stream_ray_results_ordered(futures):
                yield batch
        else:
            async for batch in self._stream_ray_results_unordered(futures):
                yield batch

    async def _process_with_asyncio(
        self, partitions: List[T], batch_size: Optional[int], preserve_order: bool
    ) -> AsyncIterable[pa.RecordBatch]:
        """
        Process partitions using AsyncIO with thread pool while maintaining streaming.

        This method uses ThreadPoolExecutor to process partitions in parallel threads,
        but maintains streaming behavior by yielding batches as they are produced
        rather than collecting them in memory.

        Args:
            partitions: List of time series partitions to process.
            batch_size: Maximum size per batch passed to underlying services.
            preserve_order: If True, yield batches in partition order; if False, yield as completed.

        Yields:
            pa.RecordBatch: Arrow record batches streamed from partitions.
        """
        """Process partitions using AsyncIO with thread pool - maintains streaming."""
        max_workers = min(self._get_max_workers(), len(partitions))

        # Use a single ThreadPoolExecutor for all partitions
        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            def sync_process_single_batch(partition: T):
                """
                Process a single partition and return streaming generator.

                This function creates a new service instance for the partition
                and returns the batch generator directly to maintain streaming behavior.

                Args:
                    partition: The time series partition to process.

                Returns:
                    Generator yielding pa.RecordBatch instances.
                """
                service = self.base_service_class()
                # Return the generator directly - don't convert to list to maintain streaming
                return service.get_batches(partition, batch_size)

            # Submit all partitions to thread pool
            loop = asyncio.get_running_loop()

            if preserve_order:
                # Process in order - submit all tasks first
                futures = [
                    loop.run_in_executor(executor, sync_process_single_batch, partition) for partition in partitions
                ]

                # Wait for each partition in order and stream its batches
                for future in futures:
                    try:
                        batch_generator = await future
                        for batch in batch_generator:
                            yield batch
                    except Exception as e:
                        logger.error(f"AsyncIO partition failed: {e}")
                        continue
            else:
                # Process as completed - submit and process immediately
                futures = [
                    loop.run_in_executor(executor, sync_process_single_batch, partition) for partition in partitions
                ]

                # Stream batches as partitions complete
                for future in asyncio.as_completed(futures):
                    try:
                        batch_generator = await future
                        for batch in batch_generator:
                            yield batch
                    except Exception as e:
                        logger.error(f"AsyncIO partition failed: {e}")
                        continue

    async def _process_single_threaded(
        self, partitions: List[T], batch_size: Optional[int], preserve_order: bool
    ) -> AsyncIterable[pa.RecordBatch]:
        """Process partitions sequentially in single thread."""
        logger.info("Using single-threaded processing (distributed disabled)")

        for partition in partitions:
            try:
                service = self.base_service_class()
                for batch in service.get_batches(partition, batch_size):
                    yield batch
            except Exception as e:
                logger.error(f"Single-threaded partition failed: {e}")
                continue

    async def _stream_ray_results_ordered(self, futures: List) -> AsyncIterable[pa.RecordBatch]:
        """Stream Ray results in partition order."""
        results = {}  # partition_index -> List[RecordBatch]
        future_to_idx = {future: i for i, future in enumerate(futures)}
        pending = set(futures)
        next_expected_idx = 0

        while pending:
            # Wait for at least one task to complete
            ready, pending = ray.wait(list(pending), num_returns=1, timeout=1.0)

            for future in ready:
                try:
                    idx = future_to_idx[future]
                    results[idx] = ray.get(future)

                    # Yield consecutive completed partitions in order
                    while next_expected_idx in results:
                        for batch in results.pop(next_expected_idx):
                            yield batch
                        next_expected_idx += 1

                except Exception as e:
                    logger.error(f"Ray task failed: {e}")
                    continue

    async def _stream_ray_results_unordered(self, futures: List) -> AsyncIterable[pa.RecordBatch]:
        """Stream Ray results as they complete."""
        pending = set(futures)

        while pending:
            # Wait for at least one task to complete
            ready, pending = ray.wait(list(pending), num_returns=1, timeout=1.0)

            for future in ready:
                try:
                    batches = ray.get(future)
                    for batch in batches:
                        yield batch
                except Exception as e:
                    logger.error(f"Ray task failed: {e}")
                    continue

    def _get_max_workers(self) -> int:
        """Get maximum number of workers based on backend."""
        if not self.enable_distributed:
            return 1

        if self.max_workers:
            return self.max_workers

        if self.backend == "ray":
            try:
                return len(ray.nodes()) * 2 if ray.is_initialized() else 4
            except:
                return 4
        else:
            # For AsyncIO, use a reasonable number of threads
            return min(16, (os.cpu_count() or 1) * 2)

    def get_backend_info(self) -> dict:
        """Get information about the current backend."""
        return {
            "backend": self.backend,
            "distributed_enabled": self.enable_distributed,
            "ray_available": RAY_AVAILABLE,
            "max_workers": self._get_max_workers(),
            "ray_initialized": ray.is_initialized() if RAY_AVAILABLE else False,
        }


# Ray remote function (only defined if Ray is available)
if RAY_AVAILABLE:

    @ray.remote
    def process_partition_remote(
        service_class, params: TimeSeriesParams, batch_size: Optional[int] = None
    ) -> List[pa.RecordBatch]:
        """
        Remote function to process a single time series partition on a Ray worker.

        Args:
            service_class: Class of the data service (not instance, to avoid serialization)
            params: Time series parameters for this specific partition
            batch_size: Maximum batch size for processing

        Returns:
            List of RecordBatch objects from this partition
        """
        service = service_class()
        batches = []

        try:
            for batch in service.get_batches(params, batch_size):
                batches.append(batch)
        except Exception as e:
            logger.error(f"Ray worker error processing partition: {e}")
            # Return empty list instead of failing the entire job

        return batches
else:
    # Dummy function when Ray is not available
    def process_partition_remote(*args, **kwargs):
        """Dummy function when Ray is not available."""
        raise ImportError("Ray is not available")
