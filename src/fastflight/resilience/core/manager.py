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
from typing import Callable, Dict, Optional

from fastflight.exceptions import FastFlightRetryExhaustedError

from ..config import CircuitBreakerConfig, ResilienceConfig, RetryConfig
from ..types import T
from .circuit_breaker import CircuitBreaker
# Import Prometheus metrics for circuit breaker
from fastflight.metrics import (
    CIRCUIT_BREAKER_STATE_MAP,
    bouncer_circuit_breaker_failures_total,
    bouncer_circuit_breaker_state,
    bouncer_circuit_breaker_successes_total,
)

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
            cb = CircuitBreaker(name, config)
            self.circuit_breakers[name] = cb
            # Initialize state metric for the new circuit breaker
            current_state_str = cb.state.value # Assuming .value gives "closed", "open", "half-open"
            bouncer_circuit_breaker_state.labels(circuit_name=name, state=current_state_str).set(
                CIRCUIT_BREAKER_STATE_MAP.get(current_state_str, -1) # -1 for unknown/other
            )
            # Initialize counters for this CB to zero if not already present (Prometheus handles this)
            bouncer_circuit_breaker_failures_total.labels(circuit_name=name).inc(0)
            bouncer_circuit_breaker_successes_total.labels(circuit_name=name).inc(0)
        return self.circuit_breakers[name]

    async def execute_with_resilience(
        self, func: Callable[..., T], *args, config: Optional[ResilienceConfig] = None, **kwargs
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

            # This common wrapper handles metrics for both async and sync funcs called via the async CB
            async def common_circuit_wrapped_func_with_metrics(*a, **kw):
                cb_name = circuit_breaker.name
                try:
                    # The actual call to the function via the circuit breaker
                    # circuit_breaker.call is async and handles both async and sync original functions
                    result = await circuit_breaker.call(func, *a, **kw)
                    
                    # If circuit_breaker.call succeeds, it means the underlying func succeeded
                    # or the CB allowed the call and it succeeded.
                    bouncer_circuit_breaker_successes_total.labels(circuit_name=cb_name).inc()
                except Exception as cb_exc:
                    # This exception could be from the func itself (if CB is closed/half-open and func fails,
                    # leading to CB counting a failure) or CircuitBreakerOpen if CB is open.
                    if not isinstance(cb_exc, asyncio.exceptions.CancelledError): # Don't count cancellations
                         bouncer_circuit_breaker_failures_total.labels(circuit_name=cb_name).inc()
                    raise # Re-raise the exception
                finally:
                    # Always update the state gauge after a call, as the call might have changed the state
                    current_state_str = circuit_breaker.state.value
                    bouncer_circuit_breaker_state.labels(circuit_name=cb_name, state=current_state_str).set(
                        CIRCUIT_BREAKER_STATE_MAP.get(current_state_str, -1)
                    )
                return result # Return the result if no exception was raised or re-raised

            wrapped_func = common_circuit_wrapped_func_with_metrics

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
