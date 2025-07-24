"""
Circuit breaker implementation.
"""

import asyncio
import logging
import time
from collections.abc import Callable
from typing import cast

from fastflight.exceptions import FastFlightCircuitOpenError

from ..config.circuit_breaker import CircuitBreakerConfig
from ..types import CircuitState, T

logger = logging.getLogger(__name__)


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
        self.last_failure_time: float | None = None
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
            return cast(T, result)

        except Exception as e:
            await self._on_failure(e)
            raise

    async def _check_state(self):
        """Check and update the circuit breaker state based on current conditions."""
        current_time = time.time()

        if (
            self.state == CircuitState.OPEN
            and self.last_failure_time
            and current_time - self.last_failure_time >= self.config.recovery_timeout
        ):
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
