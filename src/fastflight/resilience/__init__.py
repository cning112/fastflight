"""
Resilience patterns for FastFlight client operations.

This package implements retry mechanisms and circuit breaker patterns to enhance
system resilience and fault tolerance in distributed data processing environments.
"""

# Import types
# Import configuration models
from .config import CircuitBreakerConfig, ResilienceConfig, RetryConfig

# Import core implementation classes
from .core import CircuitBreaker, ResilienceManager
from .types import CircuitState, RetryStrategy

__all__ = [
    # Types
    "RetryStrategy",
    "CircuitState",
    # Configuration
    "RetryConfig",
    "CircuitBreakerConfig",
    "ResilienceConfig",
    # Core classes
    "CircuitBreaker",
    "ResilienceManager",
]
