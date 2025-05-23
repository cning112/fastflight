"""
Resilience patterns for FastFlight client operations.

This module implements retry mechanisms and circuit breaker patterns to enhance
system resilience and fault tolerance in distributed data processing environments.
"""

import asyncio
import dataclasses
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Type, TypeVar

from fastflight.exceptions import (
    FastFlightCircuitOpenError,
    FastFlightConnectionError,
    FastFlightRetryExhaustedError,
    FastFlightServerError,
    FastFlightTimeoutError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryStrategy(Enum):
    """Enumeration of available retry strategies."""

    FIXED_DELAY = "fixed_delay"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    JITTERED_EXPONENTIAL = "jittered_exponential"


@dataclass
class RetryConfig:
    """
    Configuration for retry behavior.

    This class defines the parameters that control how retry operations are performed,
    including the maximum number of attempts, delay strategies, and which exceptions
    should trigger retry attempts.
    """

    max_attempts: int = 3
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter_factor: float = 0.1
    retryable_exceptions: tuple[Type[Exception], ...] = (
        FastFlightConnectionError,
        FastFlightTimeoutError,
        FastFlightServerError,
    )

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate the delay before the next retry attempt.

        Args:
            attempt: The current attempt number (starting from 1).

        Returns:
            The delay in seconds before the next attempt.
        """
        assert attempt > 0, "Retry attempt must be positive"
        if self.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.base_delay
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.base_delay * attempt
        elif self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        elif self.strategy == RetryStrategy.JITTERED_EXPONENTIAL:
            base_delay = self.base_delay * (self.exponential_base ** (attempt - 1))
            jitter = base_delay * self.jitter_factor * (random.random() * 2 - 1)
            delay = base_delay + jitter
        else:
            delay = self.base_delay
        return min(delay, self.max_delay)

    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """
        Determine if an operation should be retried based on the exception and attempt count.

        Args:
            exception: The exception that occurred.
            attempt: The current attempt number.

        Returns:
            True if the operation should be retried, False otherwise.
        """
        if attempt >= self.max_attempts:
            logger.debug(f"Retry attempt {attempt} exceeds max {self.max_attempts}")
            return False
        return isinstance(exception, self.retryable_exceptions)


class CircuitState(Enum):
    """States of a circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """
    Configuration for circuit breaker behavior.

    This class defines the parameters that control circuit breaker operation,
    including failure thresholds, timeout durations, and recovery mechanisms.
    """

    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    success_threshold: int = 3
    timeout: float = 30.0
    monitored_exceptions: tuple[Type[Exception], ...] = (
        FastFlightConnectionError,
        FastFlightServerError,
        FastFlightTimeoutError,
    )


class CircuitBreaker:
    """
    Implementation of the circuit breaker pattern for fault tolerance.

    The circuit breaker monitors operation failures and temporarily stops
    executing operations when failure rates exceed configured thresholds,
    allowing time for the underlying service to recover.
    """

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Execute a function through the circuit breaker.

        Args:
            func: The function to execute.
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.

        Returns:
            The result of the function call.

        Raises:
            FastFlightCircuitOpenError: When the circuit is open.
            Various exceptions: As raised by the wrapped function.
        """
        async with self._lock:
            await self._check_state()

            if self.state == CircuitState.OPEN:
                raise FastFlightCircuitOpenError(
                    f"Circuit breaker '{self.name}' is open",
                    circuit_name=self.name,
                    retry_after=self.config.recovery_timeout,
                )

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            await self._on_success()
            return result

        except Exception as e:
            await self._on_failure(e)
            raise

    async def _check_state(self):
        """Check and update the circuit breaker state based on current conditions."""
        current_time = time.time()

        if self.state == CircuitState.OPEN:
            if self.last_failure_time and current_time - self.last_failure_time >= self.config.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                logger.info(f"Circuit breaker '{self.name}' transitioned to HALF_OPEN")

    async def _on_success(self):
        """Handle successful operation execution."""
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    logger.info(f"Circuit breaker '{self.name}' transitioned to CLOSED")
            elif self.state == CircuitState.CLOSED:
                self.failure_count = 0

    async def _on_failure(self, exception: Exception):
        """Handle failed operation execution."""
        if not isinstance(exception, self.config.monitored_exceptions):
            return
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitState.CLOSED:
                if self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitState.OPEN
                    logger.warning(
                        f"Circuit breaker '{self.name}' transitioned to OPEN after {self.failure_count} failures"
                    )
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker '{self.name}' transitioned back to OPEN")


@dataclass
class ResilienceConfig:
    """
    Comprehensive configuration for resilience patterns including retry and circuit breaker settings.

    This data class encapsulates all resilience-related configuration parameters,
    providing a clean and type-safe interface for configuring failure handling strategies.
    """

    # Retry configuration
    retry_config: Optional[RetryConfig] = None

    # Circuit breaker configuration
    circuit_breaker_config: Optional[CircuitBreakerConfig] = None
    circuit_breaker_name: Optional[str] = None
    enable_circuit_breaker: bool = True

    # Operation-specific settings
    operation_timeout: Optional[float] = None

    # Advanced configuration options
    custom_error_handlers: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def create_default(cls) -> "ResilienceConfig":
        """Create a ResilienceConfig with sensible default values for production use."""
        return cls(
            retry_config=RetryConfig(
                max_attempts=3, strategy=RetryStrategy.EXPONENTIAL_BACKOFF, base_delay=1.0, max_delay=16.0
            ),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=5, recovery_timeout=30.0, success_threshold=2
            ),
            enable_circuit_breaker=True,
        )

    @classmethod
    def create_for_high_availability(cls) -> "ResilienceConfig":
        """Create a ResilienceConfig optimized for high-availability scenarios."""
        return cls(
            retry_config=RetryConfig(
                max_attempts=5,
                strategy=RetryStrategy.JITTERED_EXPONENTIAL,
                base_delay=0.5,
                max_delay=8.0,
                jitter_factor=0.2,
            ),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=3, recovery_timeout=15.0, success_threshold=1
            ),
            enable_circuit_breaker=True,
        )

    @classmethod
    def create_for_batch_processing(cls) -> "ResilienceConfig":
        """Create a ResilienceConfig optimized for batch processing scenarios."""
        return cls(
            retry_config=RetryConfig(max_attempts=2, strategy=RetryStrategy.FIXED_DELAY, base_delay=5.0),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=10, recovery_timeout=60.0, success_threshold=3
            ),
            enable_circuit_breaker=True,
        )

    def with_retry_config(self, retry_config: RetryConfig) -> "ResilienceConfig":
        """Create a new ResilienceConfig with updated retry configuration."""
        return dataclasses.replace(self, retry_config=retry_config)

    def with_circuit_breaker_config(self, circuit_config: CircuitBreakerConfig) -> "ResilienceConfig":
        """Create a new ResilienceConfig with updated circuit breaker configuration."""
        return dataclasses.replace(self, circuit_breaker_config=circuit_config)

    def with_circuit_breaker_name(self, name: str) -> "ResilienceConfig":
        """Create a new ResilienceConfig with updated circuit breaker name."""
        return dataclasses.replace(self, circuit_breaker_name=name)

    def disable_circuit_breaker(self) -> "ResilienceConfig":
        """Create a new ResilienceConfig with circuit breaker disabled."""
        return dataclasses.replace(self, enable_circuit_breaker=False)


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
        if effective_config.enable_circuit_breaker and effective_config.circuit_breaker_name:
            circuit_breaker = self.get_circuit_breaker(
                effective_config.circuit_breaker_name, effective_config.circuit_breaker_config
            )

            async def wrapped_func(*a, **kw):
                return await circuit_breaker.call(func, *a, **kw)
        else:
            wrapped_func = func

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

                if not retry_config.should_retry(e, attempt):
                    raise

                if attempt < retry_config.max_attempts:
                    delay = retry_config.calculate_delay(attempt)
                    logger.warning(
                        f"Operation failed (attempt {attempt}/{retry_config.max_attempts}), retrying in {delay:.2f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"All retry attempts exhausted after {attempt} attempts")

        # This should not be reached, but included for completeness
        raise FastFlightRetryExhaustedError(
            f"Operation failed after {retry_config.max_attempts} attempts",
            attempt_count=retry_config.max_attempts,
            last_error=last_exception,
        )

    def update_default_config(self, config: ResilienceConfig) -> None:
        """Update the default configuration for this resilience manager."""
        self.default_config = config
        logger.info("Updated default resilience configuration")


# Remove global resilience manager instance - use instance-level managers instead
