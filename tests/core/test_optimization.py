"""Tests for query optimization utilities."""

from datetime import datetime

from tests.core.test_timeseries import MockTimeSeriesParams

from fastflight.core.optimization import OptimizationHint, QueryPattern, optimize_time_series_query


class TestOptimizationHint:
    def test_default_hint(self):
        hint = OptimizationHint()
        assert hint.pattern == QueryPattern.HISTORICAL
        assert hint.max_workers == 8
        assert hint.target_batch_size == 10000

    def test_real_time_hint(self):
        hint = OptimizationHint.for_real_time()
        assert hint.pattern == QueryPattern.REAL_TIME
        assert hint.max_workers == 2
        assert hint.target_batch_size == 1000
        assert hint.prefer_recent_data is True

    def test_analytics_hint(self):
        hint = OptimizationHint.for_analytics()
        assert hint.pattern == QueryPattern.ANALYTICS
        assert hint.max_workers == 16
        assert hint.target_batch_size == 50000


class TestOptimizeTimeSeriesQuery:
    def test_real_time_short_query(self):
        """Real-time queries <= 1 hour should not be partitioned."""
        params = MockTimeSeriesParams(
            symbol="TEST",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 10, 30),  # 30 minutes
        )

        hint = OptimizationHint.for_real_time()
        partitions = optimize_time_series_query(params, hint)

        assert len(partitions) == 1
        assert partitions[0].start_time == params.start_time
        assert partitions[0].end_time == params.end_time

    def test_real_time_long_query(self):
        """Real-time queries > 1 hour should be split by 15-minute windows."""
        params = MockTimeSeriesParams(
            symbol="TEST",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 12, 0),  # 2 hours
        )

        hint = OptimizationHint.for_real_time()
        partitions = optimize_time_series_query(params, hint)

        # 2 hours / 15 minutes = 8 partitions
        assert len(partitions) == 8

        # Check first partition
        assert partitions[0].start_time == datetime(2024, 1, 1, 10, 0)
        assert partitions[0].end_time == datetime(2024, 1, 1, 10, 15)

    def test_analytics_query(self):
        """Analytics queries should use optimal partitioning."""
        params = MockTimeSeriesParams(
            symbol="TEST",
            interval="1min",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 1, 18, 0),  # 8 hours = 480 data points
        )

        hint = OptimizationHint.for_analytics()  # target_batch_size=50000
        partitions = optimize_time_series_query(params, hint)

        # 480 points < 50000, so should get 1 partition
        assert len(partitions) == 1

    def test_default_optimization(self):
        """Default optimization should use balanced approach."""
        params = MockTimeSeriesParams(
            symbol="TEST",
            interval="1min",
            start_time=datetime(2024, 1, 1, 10, 0),
            end_time=datetime(2024, 1, 2, 10, 0),  # 24 hours = 1440 data points
        )

        hint = OptimizationHint()  # default: max_workers=8, target_batch_size=10000
        partitions = optimize_time_series_query(params, hint)

        # 1440 points < 10000, so should get 1 partition
        assert len(partitions) == 1

    def test_large_analytics_query(self):
        """Large analytics queries should be heavily partitioned."""
        params = MockTimeSeriesParams(
            symbol="TEST",
            interval="1min",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 2, 1),  # 1 month ≈ 44640 data points
        )

        hint = OptimizationHint.for_analytics()  # max_workers=16, target_batch_size=50000
        partitions = optimize_time_series_query(params, hint)

        # Should get 1 partition since 44640 < 50000
        assert len(partitions) == 1

    def test_very_large_query_partitioning(self):
        """Very large queries should be split into multiple partitions."""

        # Create a mock class that reports large data points
        class LargeDataParams(MockTimeSeriesParams):
            def estimate_data_points(self) -> int:
                return 100000  # Force large dataset

        params = LargeDataParams(symbol="TEST", start_time=datetime(2024, 1, 1), end_time=datetime(2024, 2, 1))

        hint = OptimizationHint.for_analytics()  # target_batch_size=50000
        partitions = optimize_time_series_query(params, hint)

        # 100000 / 50000 = 2 partitions
        assert len(partitions) == 2
