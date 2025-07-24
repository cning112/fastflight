"""
ResilienceManager: Unified retry and circuit breaker executor.

This class allows you to wrap any callable (sync or async) with built-in fault-tolerance strategies:
- Retry with configurable backoff strategies
- Circuit breaker protection to prevent cascading failures

Usage:
    manager = ResilienceManager()
    result = await manager.execute_with_resilience(your_async_func, *args, retry_config=..., circuit_breaker_name="api")
"""

import asyncio
import logging
from collections.abc import Callable
from typing import cast

from fastflight.exceptions import FastFlightRetryExhaustedError

from ..config import CircuitBreakerConfig, ResilienceConfig, RetryConfig
from ..types import T
from .circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)


class ResilienceManager:
    """
    Dead-simple resilience for any function - just wrap and go.

    Automatically retries failed operations and protects against cascading failures
    using circuit breaker patterns. Works with any sync or async function.

    Examples:
        Basic usage (3 retries + circuit breaker):
        >>> manager = ResilienceManager()
        >>> result = await manager.execute_with_resilience(risky_function)

        Custom configuration:
        >>> config = ResilienceConfig.create_for_high_availability()
        >>> result = await manager.execute_with_resilience(api_call, config=config)

        Retry only (no circuit breaker):
        >>> config = ResilienceConfig(
        ...     retry_config=RetryConfig(max_attempts=5),
        ...     enable_circuit_breaker=False
        ... )
        >>> result = await manager.execute_with_resilience(flaky_operation, config=config)
    """

    def __init__(self, default_config: ResilienceConfig | None = None):
        self.circuit_breakers: dict[str, CircuitBreaker] = {}
        self.default_config = default_config or ResilienceConfig.create_default()

    def get_circuit_breaker(self, name: str, config: CircuitBreakerConfig | None = None) -> CircuitBreaker:
        """
        Get or create a circuit breaker with the specified name and configuration.

        Args:
            name: The unique name of the circuit breaker.
            config: Optional configuration for the circuit breaker.

        Returns:
            The circuit breaker instance.
        """
        if name not in self.circuit_breakers:
            if config is None:
                raise ValueError(f"Circuit breaker '{name}' not found and no configuration provided")
            self.circuit_breakers[name] = CircuitBreaker(name, config)
        return self.circuit_breakers[name]

    async def execute_with_resilience(
        self, func: Callable[..., T], *args, config: ResilienceConfig | None = None, **kwargs
    ) -> T:
        """
        Execute a function (sync or async) with automatic retry and circuit breaker support.

        This is the main entry point for fault-tolerant execution. You can pass in any function,
        and it will be executed with the resilience strategy configured via `ResilienceConfig`.

        This method itself is async, so it must be awaited, even if the function you provide is synchronous.
        Synchronous functions are automatically wrapped to be compatible with async execution.

        Args:
            func: A sync or async callable to protect
            *args: Positional arguments for the callable
            config: Optional ResilienceConfig to override the manager's default config
            **kwargs: Keyword arguments for the callable

        Returns:
            The result of the function execution (same return type as `func`)

        Raises:
            FastFlightRetryExhaustedError: If all retry attempts fail
            FastFlightCircuitOpenError: If the circuit breaker is open
            Exception: Any other uncaught exception from the function
        """
        effective_config = config or self.default_config

        # Apply circuit breaker if enabled and named
        wrapped_func = func
        if effective_config.enable_circuit_breaker and effective_config.circuit_breaker_name:
            circuit_breaker = self.get_circuit_breaker(
                effective_config.circuit_breaker_name, effective_config.circuit_breaker_config
            )
            # Create a wrapped function that applies circuit breaker
            if asyncio.iscoroutinefunction(func):

                async def circuit_wrapped_func(*a, **kw):
                    return await circuit_breaker.call(func, *a, **kw)
            else:

                async def circuit_wrapped_func(*a, **kw):
                    return await circuit_breaker.call(func, *a, **kw)

            wrapped_func = circuit_wrapped_func

        # Apply retry logic if configured
        if effective_config.retry_config:
            return await self._execute_with_retry(wrapped_func, effective_config.retry_config, *args, **kwargs)
        else:
            # Execute without retry if no retry config provided
            if asyncio.iscoroutinefunction(wrapped_func):
                return cast(T, await wrapped_func(*args, **kwargs))
            else:
                return wrapped_func(*args, **kwargs)

    async def _execute_with_retry(self, func: Callable[..., T], retry_config: RetryConfig, *args, **kwargs) -> T:
        """
        Execute a function with retry logic.

        Args:
            func: The function to execute.
            retry_config: The retry configuration to use.

        Returns:
            The result of the function execution.
        """
        last_exception: Exception | None = None

        for attempt in range(1, retry_config.max_attempts + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    return cast(T, await func(*args, **kwargs))
                else:
                    return func(*args, **kwargs)

            except Exception as e:
                last_exception = e

                if not retry_config.is_retryable_exception(e):
                    raise

                if attempt < retry_config.max_attempts:
                    delay = retry_config.calculate_delay(attempt)
                    logger.warning(
                        f"Operation failed (attempt {attempt}/{retry_config.max_attempts}), retrying in {delay:.2f}s: "
                        f"{e}"
                    )
                    await asyncio.sleep(delay)

        # If we reach here, all retries have been exhausted
        raise FastFlightRetryExhaustedError(
            f"Operation failed after {retry_config.max_attempts} attempts",
            attempt_count=retry_config.max_attempts,
            last_error=last_exception,
        )

    def update_default_config(self, config: ResilienceConfig) -> None:
        """Update the default configuration for this resilience manager."""
        self.default_config = config
        logger.info("Updated default resilience configuration")
