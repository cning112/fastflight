"""
Resilience manager that coordinates retry and circuit breaker patterns.
"""

import asyncio
import logging
from typing import Callable, Dict, Optional

from fastflight.exceptions import FastFlightRetryExhaustedError

from ..config import CircuitBreakerConfig, ResilienceConfig, RetryConfig
from ..types import T
from .circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)


class ResilienceManager:
    """
    Manages retry and circuit breaker policies using unified configuration.

    This class provides a simplified interface for applying resilience patterns
    to operations using a single configuration object.
    """

    def __init__(self, default_config: Optional[ResilienceConfig] = None):
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.default_config = default_config or ResilienceConfig.create_default()

    def get_circuit_breaker(self, name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
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
        self, func: Callable[..., T], *args, config: Optional[ResilienceConfig] = None, **kwargs
    ) -> T:
        """
        Execute a function with resilience patterns using unified configuration.

        Args:
            func: The function to execute with resilience protection.
            config: Optional configuration override. If None, uses default configuration.

        Returns:
            The result of the function execution.

        Raises:
            FastFlightRetryExhaustedError: When all retry attempts are exhausted.
            FastFlightCircuitOpenError: When the circuit breaker is open.
            Various exceptions: As raised by the wrapped function.
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
                return await wrapped_func(*args, **kwargs)
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
        last_exception: Optional[Exception] = None

        for attempt in range(1, retry_config.max_attempts + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)

            except Exception as e:
                last_exception = e

                if not retry_config.is_retryable_exception(e):
                    raise

                if attempt < retry_config.max_attempts:
                    delay = retry_config.calculate_delay(attempt)
                    logger.warning(
                        f"Operation failed (attempt {attempt}/{retry_config.max_attempts}), retrying in {delay:.2f}s: {e}"
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
