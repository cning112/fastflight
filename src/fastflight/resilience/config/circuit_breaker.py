"""
Circuit breaker configuration models.
"""

from typing import Type

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from fastflight.exceptions import FastFlightConnectionError, FastFlightServerError, FastFlightTimeoutError


class CircuitBreakerConfig(BaseModel):
    """
    Configuration for circuit breaker behavior with validation.

    This Pydantic model defines the parameters that control circuit breaker operation
    with comprehensive validation for all parameters.
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

    monitored_exceptions: tuple[Type[Exception], ...] = Field(
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
