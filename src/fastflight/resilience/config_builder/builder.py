"""
Resilience configuration builder using Builder pattern.

This module replaces the complex create_resilience_config function
with a clean, testable Builder pattern implementation.
"""

from typing import Any

from fastflight.resilience.config.circuit_breaker import CircuitBreakerConfig
from fastflight.resilience.config.resilience import ResilienceConfig
from fastflight.resilience.config.retry import RetryConfig
from fastflight.resilience.types import RetryStrategy

from .types import ResiliencePreset


class ResilienceConfigBuilder:
    """
    Builder for creating resilience configurations.

    This class decomposes the original complex function (complexity C19)
    into multiple simple methods, each with A/B level complexity (1-4).

    Example usage:
        config = (ResilienceConfigBuilder(ResiliencePreset.HIGH_AVAILABILITY)
                  .with_retry_settings(max_attempts=5)
                  .with_circuit_breaker_settings(failure_threshold=3)
                  .build())
    """

    def __init__(self, preset: ResiliencePreset):
        """Initialize builder with a preset. Complexity: A(1)"""
        self.preset = preset
        self._base_config = self._create_base_config()
        self._retry_overrides: dict[str, Any] = {}
        self._circuit_breaker_overrides: dict[str, Any] = {}
        self._general_overrides: dict[str, Any] = {}

    def _create_base_config(self) -> ResilienceConfig | None:
        """Create base configuration from preset. Complexity: A(3)"""
        if self.preset == ResiliencePreset.DISABLED:
            return None

        # Using strategy pattern instead of large if-elif chains
        preset_factories = {
            ResiliencePreset.DEFAULT: ResilienceConfig.create_default,
            ResiliencePreset.HIGH_AVAILABILITY: ResilienceConfig.create_for_high_availability,
            ResiliencePreset.BATCH_PROCESSING: ResilienceConfig.create_for_batch_processing,
        }

        factory = preset_factories.get(self.preset, ResilienceConfig.create_default)
        return factory()

    def with_retry_settings(
        self,
        max_attempts: int | None = None,
        strategy: RetryStrategy | None = None,
        base_delay: float | None = None,
        max_delay: float | None = None,
    ) -> "ResilienceConfigBuilder":
        """Configure retry settings. Complexity: A(2)"""
        # Collect only non-None values to simplify logic
        overrides = {
            k: v
            for k, v in {
                "max_attempts": max_attempts,
                "strategy": strategy,
                "base_delay": base_delay,
                "max_delay": max_delay,
            }.items()
            if v is not None
        }
        self._retry_overrides.update(overrides)
        return self

    def with_circuit_breaker_settings(
        self,
        failure_threshold: int | None = None,
        recovery_timeout: float | None = None,
        success_threshold: int | None = None,
    ) -> "ResilienceConfigBuilder":
        """Configure circuit breaker settings. Complexity: A(2)"""
        overrides = {
            k: v
            for k, v in {
                "failure_threshold": failure_threshold,
                "recovery_timeout": recovery_timeout,
                "success_threshold": success_threshold,
            }.items()
            if v is not None
        }
        self._circuit_breaker_overrides.update(overrides)
        return self

    def with_general_settings(
        self,
        operation_timeout: float | None = None,
        enable_circuit_breaker: bool | None = None,
        circuit_breaker_name: str | None = None,
    ) -> "ResilienceConfigBuilder":
        """Configure general settings. Complexity: A(2)"""
        overrides = {
            k: v
            for k, v in {
                "operation_timeout": operation_timeout,
                "enable_circuit_breaker": enable_circuit_breaker,
                "circuit_breaker_name": circuit_breaker_name,
            }.items()
            if v is not None
        }
        self._general_overrides.update(overrides)
        return self

    def build(self) -> ResilienceConfig | None:
        """Build the final configuration. Complexity: B(4)"""
        if self._base_config is None:
            return None

        config = self._base_config

        # Apply different types of overrides separately
        if self._retry_overrides:
            config = self._apply_retry_overrides(config)

        if self._circuit_breaker_overrides:
            config = self._apply_circuit_breaker_overrides(config)

        if self._general_overrides:
            config = self._apply_general_overrides(config)

        return config

    def _apply_retry_overrides(self, config: ResilienceConfig) -> ResilienceConfig:
        """Apply retry overrides. Complexity: A(2)"""
        retry_config = config.retry_config or RetryConfig()
        updated_retry = retry_config.model_copy(update=self._retry_overrides)
        return config.model_copy(update={"retry_config": updated_retry})

    def _apply_circuit_breaker_overrides(self, config: ResilienceConfig) -> ResilienceConfig:
        """Apply circuit breaker overrides. Complexity: A(2)"""
        cb_config = config.circuit_breaker_config or CircuitBreakerConfig()
        updated_cb = cb_config.model_copy(update=self._circuit_breaker_overrides)
        return config.model_copy(update={"circuit_breaker_config": updated_cb})

    def _apply_general_overrides(self, config: ResilienceConfig) -> ResilienceConfig:
        """Apply general overrides. Complexity: A(1)"""
        return config.model_copy(update=self._general_overrides)
