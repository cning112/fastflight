"""
FastFlight - High-performance data transfer framework built on Apache Arrow Flight.

This package provides easy-to-use, easy-to-integrate, and modular data transfer capabilities
with comprehensive error handling, retry mechanisms, and circuit breaker patterns for
production-ready resilience.
"""

import importlib.metadata

from fastflight.client import FastFlightBouncer
from fastflight.core.base import BaseDataService, BaseParams
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

# Get version from package metadata
try:
    __version__ = importlib.metadata.version("fastflight")
except importlib.metadata.PackageNotFoundError:
    __version__ = "dev"

__all__ = [
    # Core classes
    "BaseDataService",
    "BaseParams",
    # Resilience components
    "CircuitBreakerConfig",
    # Exception hierarchy
    "FastFlightAuthenticationError",
    "FastFlightBouncer",
    "FastFlightCircuitOpenError",
    "FastFlightConnectionError",
    "FastFlightDataServiceError",
    "FastFlightDataValidationError",
    "FastFlightError",
    "FastFlightResourceExhaustionError",
    "FastFlightRetryExhaustedError",
    "FastFlightSerializationError",
    "FastFlightServer",
    "FastFlightServerError",
    "FastFlightTimeoutError",
    "ResilienceConfig",
    "ResilienceManager",
    "RetryConfig",
    "RetryStrategy",
    # Version
    "__version__",
]
