"""
Retry configuration model for defining retry strategies and limits.

Provides validation and delay calculation for various retry backoff strategies.
"""

import secrets

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from fastflight.exceptions import FastFlightConnectionError, FastFlightServerError, FastFlightTimeoutError

from ..types import RetryStrategy


class RetryConfig(BaseModel):
    """
    Configuration for retrying operations with validation and delay calculation.

    Example:
        >>> config = RetryConfig(
        ...     max_attempts=5,
        ...     strategy=RetryStrategy.JITTERED_EXPONENTIAL,
        ...     base_delay=0.5,
        ...     max_delay=10.0,
        ...     jitter_factor=0.2
        ... )

    Validation rules:
        - max_attempts: integer between 1 and 100
        - base_delay: float > 0 and <= 300 seconds
        - max_delay: float > 0, <= 3600 seconds, and >= base_delay
        - exponential_base: float > 1 and <= 10
        - jitter_factor: float between 0.0 and 1.0
    """

    model_config = ConfigDict(validate_assignment=True, use_enum_values=True, extra="forbid", frozen=True)

    max_attempts: int = Field(
        default=3, ge=1, le=100, description="Maximum number of retry attempts (must be between 1 and 100)"
    )

    strategy: RetryStrategy = Field(
        default=RetryStrategy.EXPONENTIAL_BACKOFF, description="Retry strategy to use for calculating delays"
    )

    base_delay: float = Field(
        default=1.0, gt=0.0, le=300.0, description="Base delay in seconds (must be positive and <= 300)"
    )

    max_delay: float = Field(
        default=60.0, gt=0.0, le=3600.0, description="Maximum delay in seconds (must be positive and <= 3600)"
    )

    exponential_base: float = Field(
        default=2.0, gt=1.0, le=10.0, description="Exponential base for backoff calculation"
    )

    jitter_factor: float = Field(
        default=0.1, ge=0.0, le=1.0, description="Jitter factor for randomized delays (0.0 to 1.0)"
    )

    retryable_exceptions: tuple[type[Exception], ...] = Field(
        default=(FastFlightConnectionError, FastFlightTimeoutError, FastFlightServerError),
        description="Tuple of exception types that should trigger retry",
    )

    @field_validator("max_delay")
    def validate_max_delay_greater_than_base(cls, v, info):
        """Ensure max_delay >= base_delay"""
        if info.data and "base_delay" in info.data:
            base_delay = info.data["base_delay"]
            if v < base_delay:
                raise ValueError(f"max_delay ({v}) must be >= base_delay ({base_delay})")
        return v

    @field_validator("retryable_exceptions")
    def validate_exception_types(cls, v):
        """Ensure all items in retryable_exceptions are Exception subclasses"""
        for exc_type in v:
            if not (isinstance(exc_type, type) and issubclass(exc_type, Exception)):
                raise ValueError(f"{exc_type} is not a valid Exception subclass")
        return v

    @computed_field
    def total_max_delay(self) -> float:
        """
        Compute the theoretical maximum total delay across all attempts.

        Returns:
            - Maximum possible total delay in seconds
        """
        if self.strategy == RetryStrategy.FIXED_DELAY:
            return self.base_delay * (self.max_attempts - 1)
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            n = self.max_attempts - 1
            return self.base_delay * n * (n + 1) / 2
        elif self.strategy in [RetryStrategy.EXPONENTIAL_BACKOFF, RetryStrategy.JITTERED_EXPONENTIAL]:
            total = 0.0
            for attempt in range(1, self.max_attempts):
                delay = min(self.base_delay * (self.exponential_base ** (attempt - 1)), self.max_delay)
                if self.strategy == RetryStrategy.JITTERED_EXPONENTIAL:
                    delay += delay * self.jitter_factor
                total += delay
            return total
        return self.max_delay * (self.max_attempts - 1)

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate the delay before the next retry attempt.

        Args:
            attempt: The current attempt number (starting from 1).

        Returns:
            - The delay in seconds before the next attempt.

        Raises:
            ValueError: If attempt is not positive.
        """
        if attempt <= 0:
            raise ValueError("Retry attempt must be positive")

        if self.strategy == RetryStrategy.FIXED_DELAY:
            delay = self.base_delay
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.base_delay * attempt
        elif self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        elif self.strategy == RetryStrategy.JITTERED_EXPONENTIAL:
            base_delay = self.base_delay * (self.exponential_base ** (attempt - 1))
            jitter = base_delay * self.jitter_factor * (secrets.SystemRandom().random() * 2 - 1)
            delay = base_delay + jitter
        else:
            delay = self.base_delay  # type: ignore[unreachable]

        return min(delay, self.max_delay)

    def is_retryable_exception(self, exception: Exception) -> bool:
        """
        Determine if an operation should be retried based on the exception.

        Args:
            exception: The exception that occurred.

        Returns:
            - True if the operation should be retried
            - False otherwise
        """
        return isinstance(exception, self.retryable_exceptions)

    def has_attempts_remaining(self, current_attempt: int) -> bool:
        """
        Check if there are remaining retry attempts.

        Args:
            current_attempt: The current attempt number (1-based).

        Returns:
            - True if more attempts are available
            - False otherwise
        """
        return current_attempt < self.max_attempts
