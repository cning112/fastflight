"""Query optimization utilities for time series data."""

from datetime import timedelta
from enum import Enum
from typing import List

from fastflight.core.timeseries import TimeSeriesParams


class QueryPattern(Enum):
    """Common time series query patterns."""

    REAL_TIME = "real_time"  # Latest data, small windows
    HISTORICAL = "historical"  # Large time ranges, aggregations
    BACKFILL = "backfill"  # Missing data recovery
    ANALYTICS = "analytics"  # Complex analysis queries


class OptimizationHint:
    """Hints for optimizing time series queries."""

    def __init__(
        self,
        pattern: QueryPattern = QueryPattern.HISTORICAL,
        max_workers: int = 8,
        target_batch_size: int = 10000,
        prefer_recent_data: bool = False,
        enable_caching: bool = True,
    ):
        self.pattern = pattern
        self.max_workers = max_workers
        self.target_batch_size = target_batch_size
        self.prefer_recent_data = prefer_recent_data
        self.enable_caching = enable_caching

    @classmethod
    def for_real_time(cls) -> "OptimizationHint":
        return cls(pattern=QueryPattern.REAL_TIME, max_workers=2, target_batch_size=1000, prefer_recent_data=True)

    @classmethod
    def for_analytics(cls) -> "OptimizationHint":
        return cls(pattern=QueryPattern.ANALYTICS, max_workers=16, target_batch_size=50000, prefer_recent_data=False)


def optimize_time_series_query(params: TimeSeriesParams, hint: OptimizationHint) -> List[TimeSeriesParams]:
    """Optimize time series query based on pattern and hints."""
    duration = params.time_range_duration()

    # Pattern-based optimization
    if hint.pattern == QueryPattern.REAL_TIME:
        # Small, frequent queries - minimal partitioning
        if duration <= timedelta(hours=1):
            return [params]
        return params.split_by_window_size(timedelta(minutes=15))

    elif hint.pattern == QueryPattern.ANALYTICS:
        # Large analysis queries - aggressive partitioning
        return params.get_optimal_partitions(hint.max_workers, hint.target_batch_size)

    else:
        # Default balanced approach
        return params.get_optimal_partitions(hint.max_workers)
