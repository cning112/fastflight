"""
Circuit breaker configuration model with validation and recovery time estimation.

Prevents cascading failures by controlling access to failing resources.
"""

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from fastflight.exceptions import FastFlightConnectionError, FastFlightServerError, FastFlightTimeoutError


class CircuitBreakerConfig(BaseModel):
    """
    Configuration for a circuit breaker with validation.

    The circuit breaker protects against cascading failures by controlling access to a resource based on failure rates.
    It transitions through four states:
        1. CLOSED: Normal operation, all calls allowed.
        2. OPEN: After reaching failure_threshold, all calls blocked.
        3. HALF_OPEN: After recovery_timeout, limited calls allowed to test recovery.
        4. CLOSED: After success_threshold successful calls in HALF_OPEN, circuit resets.

    Examples:
        >>> # Fast opening circuit breaker
        >>> config = CircuitBreakerConfig(failure_threshold=3, recovery_timeout=15.0)

        >>> # Tolerant circuit breaker for batch jobs
        >>> config = CircuitBreakerConfig(failure_threshold=10, recovery_timeout=60.0)

    Validation rules:
        - failure_threshold: integer between 1 and 1000
        - recovery_timeout: float > 0 and <= 3600 seconds
        - success_threshold: integer between 1 and 100
        - timeout: float > 0 and <= 300 seconds
    """

    model_config = ConfigDict(validate_assignment=True, extra="forbid", frozen=True)

    failure_threshold: int = Field(
        default=5, ge=1, le=1000, description="Number of failures before opening the circuit"
    )

    recovery_timeout: float = Field(
        default=60.0, gt=0.0, le=3600.0, description="Time in seconds before attempting recovery"
    )

    success_threshold: int = Field(
        default=3, ge=1, le=100, description="Number of successes needed to close the circuit"
    )

    timeout: float = Field(default=30.0, gt=0.0, le=300.0, description="Operation timeout in seconds")

    monitored_exceptions: tuple[type[Exception], ...] = Field(
        default=(FastFlightConnectionError, FastFlightServerError, FastFlightTimeoutError),
        description="Tuple of exception types monitored by the circuit breaker",
    )

    @field_validator("monitored_exceptions")
    def validate_monitored_exception_types(cls, v):
        """Ensure all monitored exceptions are Exception subclasses"""
        for exc_type in v:
            if not (isinstance(exc_type, type) and issubclass(exc_type, Exception)):
                raise ValueError(f"{exc_type} is not a valid Exception subclass")
        return v

    @computed_field
    def max_recovery_time(self) -> float:
        """Maximum time for full recovery cycle in seconds."""
        return self.recovery_timeout + (self.success_threshold * self.timeout)
