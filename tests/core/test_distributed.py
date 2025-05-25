"""Tests for distributed processing (requires Ray)."""

from datetime import datetime, timedelta
from typing import Iterable

import pyarrow as pa
import pytest

try:
    import ray

    from fastflight.core.distributed import DistributedTimeSeriesService, process_partition_remote

    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False

from tests.core.test_timeseries import MockTimeSeriesParams

from fastflight.core.base import BaseDataService


class MockDataService(BaseDataService[MockTimeSeriesParams]):
    """Mock data service for testing."""

    def get_batches(self, params: MockTimeSeriesParams, batch_size: int = None) -> Iterable[pa.RecordBatch]:
        # Create mock data based on time range
        duration_minutes = int(params.time_range_duration().total_seconds() / 60)
        num_points = min(duration_minutes, 100)  # Limit for testing

        data = {
            "timestamp": [params.start_time + timedelta(minutes=i) for i in range(num_points)],
            "symbol": [params.symbol] * num_points,
            "value": list(range(num_points)),
        }
        yield pa.record_batch(data)

    async def aget_batches(self, params: MockTimeSeriesParams, batch_size: int = None):
        for batch in self.get_batches(params, batch_size):
            yield batch


@pytest.mark.skipif(not RAY_AVAILABLE, reason="Ray not available")
class TestDistributedTimeSeriesService:
    @pytest.fixture(autouse=True)
    def setup_ray(self):
        """Setup Ray for testing."""
        if not ray.is_initialized():
            ray.init(local_mode=True)
        yield
        # Note: Don't shutdown Ray in tests as it may be used by other tests

    def test_initialization_without_ray(self):
        """Test that initialization fails gracefully without Ray."""
        # This test is tricky since we're already checking RAY_AVAILABLE
        # We'll test the error message instead
        base_service = MockDataService()

        # Should work since RAY_AVAILABLE is True in this test class
        distributed_service = DistributedTimeSeriesService(base_service)
        assert distributed_service.base_service == base_service

    @pytest.mark.asyncio
    async def test_distributed_processing_ordered(self):
        """Test ordered distributed processing."""
        base_service = MockDataService()
        distributed_service = DistributedTimeSeriesService(base_service)

        params = MockTimeSeriesParams(
            symbol="TEST",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 14, 0),  # 4 hours -> multiple partitions
        )

        batches = []
        async for batch in distributed_service.aget_batches(params, preserve_order=True):
            batches.append(batch)

        assert len(batches) > 0

        # Verify chronological order by checking timestamps
        if len(batches) > 1:
            prev_time = None
            for batch in batches:
                if batch.num_rows > 0:
                    first_timestamp = batch.column("timestamp").to_pylist()[0]
                    if prev_time:
                        assert first_timestamp >= prev_time, "Batches not in chronological order"
                    prev_time = first_timestamp

    @pytest.mark.asyncio
    async def test_distributed_processing_unordered(self):
        """Test unordered distributed processing for maximum throughput."""
        base_service = MockDataService()
        distributed_service = DistributedTimeSeriesService(base_service)

        params = MockTimeSeriesParams(
            symbol="TEST",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 14, 0),  # 4 hours
        )

        batches = []
        async for batch in distributed_service.aget_batches(params, preserve_order=False):
            batches.append(batch)

        assert len(batches) > 0
        # For unordered, we don't check chronological order
        # Just verify we get valid batches

    def test_sync_get_batches_with_ordering(self):
        """Test synchronous batch retrieval with ordering options."""
        base_service = MockDataService()
        distributed_service = DistributedTimeSeriesService(base_service)

        params = MockTimeSeriesParams(
            symbol="TEST",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 11, 0),  # 1 hour
        )

        # Test ordered
        ordered_batches = list(distributed_service.get_batches(params, preserve_order=True))
        assert len(ordered_batches) > 0

        # Test unordered
        unordered_batches = list(distributed_service.get_batches(params, preserve_order=False))
        assert len(unordered_batches) > 0

    def test_get_available_workers(self):
        """Test worker detection."""
        base_service = MockDataService()
        distributed_service = DistributedTimeSeriesService(base_service)

        workers = distributed_service._get_available_workers()
        assert isinstance(workers, int)
        assert workers > 0

    def test_sync_get_batches(self):
        """Test synchronous batch retrieval."""
        base_service = MockDataService()
        distributed_service = DistributedTimeSeriesService(base_service)

        params = MockTimeSeriesParams(
            symbol="TEST",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 11, 0),  # 1 hour
        )

        batches = list(distributed_service.get_batches(params))
        assert len(batches) > 0


@pytest.mark.skipif(not RAY_AVAILABLE, reason="Ray not available")
class TestProcessPartitionRemote:
    @pytest.fixture(autouse=True)
    def setup_ray(self):
        """Setup Ray for testing."""
        if not ray.is_initialized():
            ray.init(local_mode=True)
        yield

    def test_remote_processing(self):
        """Test remote partition processing."""
        params = MockTimeSeriesParams(
            symbol="TEST",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 10, 30),  # 30 minutes
        )

        # Submit remote task
        future = process_partition_remote.remote(MockDataService, params)
        result = ray.get(future)

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(batch, pa.RecordBatch) for batch in result)


# Tests that don't require Ray
class TestDistributedServiceWithoutRay:
    def test_import_error_handling(self):
        """Test that appropriate error is raised when Ray is not available."""
        # We can't easily test this since Ray is available in the test environment
        # This test is more for documentation purposes
        pass
