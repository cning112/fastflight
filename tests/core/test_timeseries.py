"""Tests for time series parameters and utilities."""

from datetime import datetime, timedelta

import pytest

from fastflight.core.timeseries import TimeSeriesParams


class MockTimeSeriesParams(TimeSeriesParams):
    """Mock implementation for testing."""

    symbol: str
    interval: str = "1min"

    def estimate_data_points(self) -> int:
        duration = self.time_range_duration()
        if self.interval == "1min":
            return int(duration.total_seconds() / 60)
        return int(duration.total_seconds() / 3600)


class TestTimeSeriesParams:
    def test_time_range_duration(self):
        params = MockTimeSeriesParams(
            symbol="TEST", start_time=datetime(2024, 1, 1, 10, 0), end_time=datetime(2024, 1, 1, 12, 0)
        )
        assert params.time_range_duration() == timedelta(hours=2)

    def test_split_by_time_windows(self):
        params = MockTimeSeriesParams(
            symbol="TEST",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 14, 0),  # 4 hours
        )

        partitions = params.split_by_time_windows(4)
        assert len(partitions) == 4

        # Each partition should be 1 hour
        for i, partition in enumerate(partitions):
            expected_start = datetime(2024, 1, 1, 10 + i, 0)
            expected_end = datetime(2024, 1, 1, 11 + i, 0)
            assert partition.start_time == expected_start
            assert partition.end_time == expected_end

    def test_split_by_window_size(self):
        params = MockTimeSeriesParams(
            symbol="TEST",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 13, 0),  # 3 hours
        )

        partitions = params.split_by_window_size(timedelta(hours=1))
        assert len(partitions) == 3

        for i, partition in enumerate(partitions):
            assert partition.time_range_duration() == timedelta(hours=1)

    def test_get_optimal_partitions(self):
        params = MockTimeSeriesParams(
            symbol="TEST",
            interval="1min",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 12, 0),  # 2 hours = 120 data points
        )

        # With target of 60 points per partition, should get 2 partitions
        partitions = params.get_optimal_partitions(max_workers=8, target_points_per_partition=60)
        assert len(partitions) == 2

    def test_estimate_data_points(self):
        params = MockTimeSeriesParams(
            symbol="TEST",
            interval="1min",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 11, 0),  # 1 hour
        )

        assert params.estimate_data_points() == 60  # 60 minutes

    def test_split_single_partition(self):
        params = MockTimeSeriesParams(
            symbol="TEST", start_time=datetime(2024, 1, 1, 10, 0), end_time=datetime(2024, 1, 1, 11, 0)
        )

        partitions = params.split_by_time_windows(1)
        assert len(partitions) == 1
        assert partitions[0].start_time == params.start_time
        assert partitions[0].end_time == params.end_time

    def test_invalid_partitions(self):
        params = MockTimeSeriesParams(
            symbol="TEST", start_time=datetime(2024, 1, 1, 10, 0), end_time=datetime(2024, 1, 1, 11, 0)
        )

        with pytest.raises(ValueError):
            params.split_by_time_windows(0)

        with pytest.raises(ValueError):
            params.split_by_time_windows(-1)
