"""
ResilienceConfig: Unified configuration for retry and circuit breaker behavior.

Includes factory methods for common usage patterns like high availability or batch processing.
"""

import sys
from typing import Any, cast

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from ..types import RetryStrategy
from .circuit_breaker import CircuitBreakerConfig
from .retry import RetryConfig


class ResilienceConfig(BaseModel):
    """
    Configuration combining retry and circuit breaker settings.

    Examples:
        >>> # Production default configuration
        >>> config = ResilienceConfig.create_default()

        >>> # High availability with aggressive retries and fast circuit breaker
        >>> config = ResilienceConfig.create_for_high_availability()

        >>> # Batch processing with conservative retries and tolerant circuit breaker
        >>> config = ResilienceConfig.create_for_batch_processing()

        >>> # Custom configuration combining retry and circuit breaker
        >>> config = ResilienceConfig(
        ...     retry_config=RetryConfig(max_attempts=5, base_delay=2.0),
        ...     circuit_breaker_name="my_service",
        ...     enable_circuit_breaker=True
        ... )

        >>> # Method chaining to customize configuration
        >>> config = (ResilienceConfig.create_default()
        ...           .with_retry_config(RetryConfig(max_attempts=10))
        ...           .with_circuit_breaker_name("custom_circuit"))

    Validation rules:
        - If circuit breaker is enabled, circuit_breaker_name must be provided.
        - retry_config and circuit_breaker_config are optional; None disables the respective feature.
        - operation_timeout must be positive and less than or equal to 3600 seconds if set.
        - circuit_breaker_name must be alphanumeric with underscores or dashes, length 1-100.
    """

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        frozen=False,  # Allow mutation for method chaining
    )

    retry_config: RetryConfig | None = Field(default=None, description="Retry configuration, None to disable retries")

    circuit_breaker_config: CircuitBreakerConfig | None = Field(
        default=None, description="Circuit breaker configuration"
    )

    circuit_breaker_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        pattern=r"^[a-zA-Z0-9_-]+$",
        description="Circuit breaker name (alphanumeric, underscore, dash only)",
    )

    enable_circuit_breaker: bool = Field(default=True, description="Whether to enable circuit breaker functionality")

    operation_timeout: float | None = Field(default=None, gt=0.0, le=3600.0, description="Operation timeout in seconds")

    custom_error_handlers: dict[str, Any] = Field(default_factory=dict, description="Custom error handlers by name")

    tags: dict[str, str] = Field(default_factory=dict, description="Additional tags for monitoring and identification")

    @field_validator("circuit_breaker_name")
    def validate_circuit_breaker_name_if_enabled(cls, v, info):
        """Ensure circuit breaker name is provided if circuit breaker is enabled"""
        if (
            info.data
            and info.data.get("enable_circuit_breaker", True)
            and v is None
            and info.data.get("circuit_breaker_config") is not None
        ):
            raise ValueError("circuit_breaker_name is required when circuit breaker is enabled")
        return v

    @computed_field
    def estimated_max_operation_time(self) -> float:
        """
        Estimate maximum operation time including retries and circuit breaker recovery.

        Returns:
            float: Estimated maximum time in seconds, sum of:
                - Total delay from retries (if configured)
                - Circuit breaker max recovery time (if enabled)
                - Operation timeout (if set)
        """
        max_time = 0.0

        if self.retry_config:
            # Type cast to ensure MyPy understands this is a float
            max_time += cast(float, self.retry_config.total_max_delay)

        if self.circuit_breaker_config and self.enable_circuit_breaker:
            # Type cast to ensure MyPy understands this is a float
            max_time += cast(float, self.circuit_breaker_config.max_recovery_time)

        if self.operation_timeout:
            max_time += self.operation_timeout

        return max_time

    @classmethod
    def create_noop(cls) -> "ResilienceConfig":
        return cls(
            retry_config=None, circuit_breaker_config=None, circuit_breaker_name=None, enable_circuit_breaker=False
        )

    @classmethod
    def create_default(cls) -> "ResilienceConfig":
        """Create a ResilienceConfig with default settings for general production use."""
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

    def with_retry_config(self, retry_config: RetryConfig) -> Self:
        """Return a new ResilienceConfig with updated retry configuration."""
        return self.model_copy(update={"retry_config": retry_config})

    def with_circuit_breaker_config(self, circuit_config: CircuitBreakerConfig) -> Self:
        """Return a new ResilienceConfig with updated circuit breaker configuration."""
        return self.model_copy(update={"circuit_breaker_config": circuit_config})

    def with_circuit_breaker_name(self, name: str) -> Self:
        """Return a new ResilienceConfig with updated circuit breaker name."""
        return self.model_copy(update={"circuit_breaker_name": name})

    def disable_circuit_breaker(self) -> Self:
        """Return a new ResilienceConfig with circuit breaker functionality disabled."""
        return self.model_copy(update={"enable_circuit_breaker": False})
