"""
Time-series base parameters for FastFlight.
"""

from datetime import datetime

from fastflight.core.base import BaseParams


class TimeSeriesParams(BaseParams):
    """
    Base parameters for time-series data requests.

    Contains only the essential information that all time-series data sources need.
    Specific implementations should subclass this to add their own fields.
    """

    start_time: datetime
    end_time: datetime
