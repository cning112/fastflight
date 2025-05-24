"""
Resilience configuration that combines retry and circuit breaker settings.
"""

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from ..types import RetryStrategy
from .circuit_breaker import CircuitBreakerConfig
from .retry import RetryConfig


class ResilienceConfig(BaseModel):
    """
    Comprehensive configuration for resilience patterns with validation.

    This Pydantic model encapsulates all resilience-related configuration parameters
    with comprehensive validation and business rules.
    """

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        frozen=False,  # Allow mutation for method chaining
    )

    retry_config: Optional[RetryConfig] = Field(
        default=None, description="Retry configuration, None to disable retries"
    )

    circuit_breaker_config: Optional[CircuitBreakerConfig] = Field(
        default=None, description="Circuit breaker configuration"
    )

    circuit_breaker_name: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=100,
        pattern=r"^[a-zA-Z0-9_-]+$",
        description="Circuit breaker name (alphanumeric, underscore, dash only)",
    )

    enable_circuit_breaker: bool = Field(default=True, description="Whether to enable circuit breaker functionality")

    operation_timeout: Optional[float] = Field(
        default=None, gt=0.0, le=3600.0, description="Operation timeout in seconds"
    )

    custom_error_handlers: Dict[str, Any] = Field(default_factory=dict, description="Custom error handlers by name")

    tags: Dict[str, str] = Field(default_factory=dict, description="Additional tags for monitoring and identification")

    @field_validator("circuit_breaker_name")
    def validate_circuit_breaker_name_if_enabled(cls, v, info):
        """Ensure circuit breaker name is provided if circuit breaker is enabled"""
        if info.data and info.data.get("enable_circuit_breaker", True) and v is None:
            if info.data.get("circuit_breaker_config") is not None:
                raise ValueError("circuit_breaker_name is required when circuit breaker is enabled")
        return v

    @computed_field
    def estimated_max_operation_time(self) -> float:
        """
        Estimate maximum operation time including all retries and circuit breaker recovery.

        Returns:
            Estimated maximum time in seconds
        """
        max_time = 0.0

        if self.retry_config:
            max_time += self.retry_config.total_max_delay

        if self.circuit_breaker_config and self.enable_circuit_breaker:
            max_time += self.circuit_breaker_config.max_recovery_time

        if self.operation_timeout:
            max_time += self.operation_timeout

        return max_time

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
            circuit_breaker_name="default_circuit",
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
            circuit_breaker_name="ha_circuit",
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
            circuit_breaker_name="batch_circuit",
            enable_circuit_breaker=True,
        )

    def with_retry_config(self, retry_config: RetryConfig) -> "ResilienceConfig":
        """Create a new ResilienceConfig with updated retry configuration."""
        return self.model_copy(update={"retry_config": retry_config})

    def with_circuit_breaker_config(self, circuit_config: CircuitBreakerConfig) -> "ResilienceConfig":
        """Create a new ResilienceConfig with updated circuit breaker configuration."""
        return self.model_copy(update={"circuit_breaker_config": circuit_config})

    def with_circuit_breaker_name(self, name: str) -> "ResilienceConfig":
        """Create a new ResilienceConfig with updated circuit breaker name."""
        return self.model_copy(update={"circuit_breaker_name": name})

    def disable_circuit_breaker(self) -> "ResilienceConfig":
        """Create a new ResilienceConfig with circuit breaker disabled."""
        return self.model_copy(update={"enable_circuit_breaker": False})
