"""
Dead-simple resilience patterns for FastFlight operations.

Make any function resilient with automatic retries and circuit breakers.

Quick Start:
    >>> from fastflight.resilience import ResilienceManager
    >>>
    >>> manager = ResilienceManager()  # Sensible defaults
    >>> result = await manager.execute_with_resilience(your_function)

Examples:
    # Retry-only configuration
    >>> config = ResilienceConfig(
    ...     retry_config=RetryConfig(max_attempts=5),
    ...     enable_circuit_breaker=False
    ... )

    # Circuit breaker-only
    >>> config = ResilienceConfig(
    ...     circuit_breaker_name="payments_api",
    ...     retry_config=None
    ... )

    # Production presets
    >>> config = ResilienceConfig.create_for_high_availability()
    >>> config = ResilienceConfig.create_for_batch_processing()

Key Classes:
    - ResilienceManager: Main API - wrap any function with resilience
    - ResilienceConfig: Combined retry + circuit breaker configuration
    - RetryConfig: Retry-specific settings with validation
    - CircuitBreakerConfig: Circuit breaker settings with validation
"""

# Import types
# Import configuration models
from .config import CircuitBreakerConfig, ResilienceConfig, RetryConfig

# Import core implementation classes
from .core import CircuitBreaker, ResilienceManager
from .types import CircuitState, RetryStrategy

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "ResilienceConfig",
    "ResilienceManager",
    "RetryConfig",
    "RetryStrategy",
]
