"""Time series data parameters and utilities."""

import math
from abc import ABC
from datetime import datetime, timedelta
from typing import List, Optional

from fastflight.core.base import BaseParams


class TimeSeriesParams(BaseParams, ABC):
    """Base class for time series data request parameters."""

    start_time: datetime
    end_time: datetime

    def time_range_duration(self) -> timedelta:
        """Calculate the total duration of the time range."""
        return self.end_time - self.start_time

    def split_by_time_windows(self, num_partitions: int) -> List["TimeSeriesParams"]:
        """Split time series params into equal time windows."""
        if num_partitions <= 0:
            raise ValueError("num_partitions must be positive")

        if num_partitions == 1:
            return [self]

        duration = self.time_range_duration()
        window_size = duration / num_partitions

        partitions = []
        current_start = self.start_time

        for i in range(num_partitions):
            current_end = current_start + window_size
            # Ensure last partition includes the exact end time
            if i == num_partitions - 1:
                current_end = self.end_time

            partition = self.model_copy(update={"start_time": current_start, "end_time": current_end})
            partitions.append(partition)
            current_start = current_end

        return partitions

    def split_by_window_size(self, window_size: timedelta) -> List["TimeSeriesParams"]:
        """Split time series params by fixed window size."""
        partitions = []
        current_start = self.start_time

        while current_start < self.end_time:
            current_end = min(current_start + window_size, self.end_time)

            partition = self.model_copy(update={"start_time": current_start, "end_time": current_end})
            partitions.append(partition)
            current_start = current_end

        return partitions

    def estimate_data_points(self) -> Optional[int]:
        """
        Estimate number of data points in this time range.
        Override in subclasses for better estimates.
        """
        return None

    def get_optimal_partitions(
        self, max_workers: int, target_points_per_partition: int = 10000
    ) -> List["TimeSeriesParams"]:
        """Get optimal number of partitions based on data estimate."""
        estimated_points = self.estimate_data_points()

        if estimated_points is None:
            # Fallback to time-based partitioning
            return self.split_by_time_windows(min(max_workers, 8))

        optimal_partitions = max(1, min(max_workers, math.ceil(estimated_points / target_points_per_partition)))

        return self.split_by_time_windows(optimal_partitions)
