"""
FastFlight - High-performance data transfer framework built on Apache Arrow Flight.

This package provides easy-to-use, easy-to-integrate, and modular data transfer capabilities
with comprehensive error handling, retry mechanisms, and circuit breaker patterns for
production-ready resilience.
"""

from fastflight.client import FastFlightBouncer
from fastflight.core.base import BaseDataService, BaseParams
from fastflight.core.optimization import OptimizationHint, QueryPattern, optimize_time_series_query
from fastflight.core.timeseries import TimeSeriesParams
from fastflight.exceptions import (
    FastFlightAuthenticationError,
    FastFlightCircuitOpenError,
    FastFlightConnectionError,
    FastFlightDataServiceError,
    FastFlightDataValidationError,
    FastFlightError,
    FastFlightResourceExhaustionError,
    FastFlightRetryExhaustedError,
    FastFlightSerializationError,
    FastFlightServerError,
    FastFlightTimeoutError,
)
from fastflight.resilience import CircuitBreakerConfig, ResilienceConfig, ResilienceManager, RetryConfig, RetryStrategy
from fastflight.server import FastFlightServer

# Optional distributed processing - only import if Ray is available
try:
    from fastflight.core.distributed import DistributedTimeSeriesService

    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False

__all__ = [
    # Core classes
    "FastFlightBouncer",
    "FastFlightServer",
    "BaseDataService",
    "BaseParams",
    "TimeSeriesParams",
    # Optimization
    "OptimizationHint",
    "QueryPattern",
    "optimize_time_series_query",
    # Exception hierarchy
    "FastFlightError",
    "FastFlightConnectionError",
    "FastFlightTimeoutError",
    "FastFlightAuthenticationError",
    "FastFlightServerError",
    "FastFlightDataServiceError",
    "FastFlightDataValidationError",
    "FastFlightSerializationError",
    "FastFlightResourceExhaustionError",
    "FastFlightCircuitOpenError",
    "FastFlightRetryExhaustedError",
    # Resilience components
    "ResilienceConfig",
    "RetryConfig",
    "RetryStrategy",
    "CircuitBreakerConfig",
    "ResilienceManager",
]

# Add distributed service to __all__ if available
if RAY_AVAILABLE:
    __all__.append("DistributedTimeSeriesService")
