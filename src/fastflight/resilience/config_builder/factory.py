"""
Resilience configuration factory for creating common configuration patterns.
"""

from fastflight.resilience.config.resilience import ResilienceConfig
from fastflight.resilience.types import RetryStrategy

from .builder import ResilienceConfigBuilder
from .types import ResiliencePreset


class ResilienceConfigFactory:
    """
    Factory class providing high-level APIs for creating resilience configurations.
    This simplifies client code usage and provides common configuration patterns.
    """

    @staticmethod
    def create_for_cli(
        preset: ResiliencePreset,
        retry_max_attempts: int | None = None,
        retry_strategy: RetryStrategy | None = None,
        retry_base_delay: float | None = None,
        retry_max_delay: float | None = None,
        circuit_breaker_failure_threshold: int | None = None,
        circuit_breaker_recovery_timeout: float | None = None,
        circuit_breaker_success_threshold: int | None = None,
        operation_timeout: float | None = None,
        enable_circuit_breaker: bool = True,
        circuit_breaker_name: str | None = None,
    ) -> ResilienceConfig | None:
        """
        Convenient method for creating resilience config for CLI.

        This method replaces the original create_resilience_config function
        with complexity C(19), reducing it to A(1) by delegating to Builder.
        """
        return (
            ResilienceConfigBuilder(preset)
            .with_retry_settings(
                max_attempts=retry_max_attempts,
                strategy=retry_strategy,
                base_delay=retry_base_delay,
                max_delay=retry_max_delay,
            )
            .with_circuit_breaker_settings(
                failure_threshold=circuit_breaker_failure_threshold,
                recovery_timeout=circuit_breaker_recovery_timeout,
                success_threshold=circuit_breaker_success_threshold,
            )
            .with_general_settings(
                operation_timeout=operation_timeout,
                enable_circuit_breaker=enable_circuit_breaker,
                circuit_breaker_name=circuit_breaker_name,
            )
            .build()
        )

    @staticmethod
    def create_preset(preset: ResiliencePreset) -> ResilienceConfig | None:
        """Create preset configuration without any overrides. Complexity: A(1)"""
        return ResilienceConfigBuilder(preset).build()

    @staticmethod
    def create_for_production() -> ResilienceConfig | None:
        """Create recommended configuration for production. Complexity: A(1)"""
        return (
            ResilienceConfigBuilder(ResiliencePreset.HIGH_AVAILABILITY)
            .with_retry_settings(max_attempts=5, strategy=RetryStrategy.EXPONENTIAL_BACKOFF)
            .with_circuit_breaker_settings(failure_threshold=3, recovery_timeout=30)
            .with_general_settings(operation_timeout=60)
            .build()
        )

    @staticmethod
    def create_for_development() -> ResilienceConfig | None:
        """Create configuration suitable for development. Complexity: A(1)"""
        return (
            ResilienceConfigBuilder(ResiliencePreset.DEFAULT)
            .with_retry_settings(max_attempts=2)
            .with_circuit_breaker_settings(failure_threshold=5)
            .build()
        )

    @staticmethod
    def create_for_testing() -> ResilienceConfig | None:
        """Create minimal configuration for testing. Complexity: A(1)"""
        return ResilienceConfigBuilder(ResiliencePreset.DISABLED).build()
