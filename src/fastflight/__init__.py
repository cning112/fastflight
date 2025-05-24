"""
FastFlight - High-performance data transfer framework built on Apache Arrow Flight.

This package provides easy-to-use, easy-to-integrate, and modular data transfer capabilities
with comprehensive error handling, retry mechanisms, and circuit breaker patterns for
production-ready resilience.
"""

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

__all__ = [
    # Core classes
    "FastFlightBouncer",
    "FastFlightServer",
    "BaseDataService",
    "BaseParams",
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
