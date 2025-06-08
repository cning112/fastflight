"""
Tests for the ResilienceConfigBuilder.

These tests verify that the modern resilience configuration system works correctly.
"""

import pytest

from fastflight.resilience.config_builder.builder import ResilienceConfigBuilder
from fastflight.resilience.config_builder.factory import ResilienceConfigFactory
from fastflight.resilience.config_builder.types import ResiliencePreset
from fastflight.resilience.types import RetryStrategy


class TestResilienceConfigBuilder:
    """Test the Builder pattern implementation."""

    def test_disabled_preset_returns_none(self):
        """Test that disabled preset returns None. Complexity: A(1)"""
        config = ResilienceConfigBuilder(ResiliencePreset.DISABLED).build()
        assert config is None

    def test_default_preset_creates_config(self):
        """Test that default preset creates a valid config. Complexity: A(1)"""
        config = ResilienceConfigBuilder(ResiliencePreset.DEFAULT).build()
        assert config is not None
        assert config.retry_config is not None
        assert config.circuit_breaker_config is not None

    def test_builder_with_retry_settings(self):
        """Test retry settings configuration. Complexity: A(1)"""
        config = ResilienceConfigBuilder(ResiliencePreset.DEFAULT).with_retry_settings(max_attempts=5).build()

        assert config.retry_config.max_attempts == 5

    def test_builder_with_circuit_breaker_settings(self):
        """Test circuit breaker settings configuration. Complexity: A(1)"""
        config = (
            ResilienceConfigBuilder(ResiliencePreset.DEFAULT).with_circuit_breaker_settings(failure_threshold=3).build()
        )

        assert config.circuit_breaker_config.failure_threshold == 3

    def test_builder_with_general_settings(self):
        """Test general settings configuration. Complexity: A(1)"""
        config = ResilienceConfigBuilder(ResiliencePreset.DEFAULT).with_general_settings(operation_timeout=60).build()

        assert config.operation_timeout == 60

    def test_method_chaining(self):
        """Test method chaining works correctly. Complexity: A(1)"""
        config = (
            ResilienceConfigBuilder(ResiliencePreset.DEFAULT)
            .with_retry_settings(max_attempts=5, strategy=RetryStrategy.EXPONENTIAL_BACKOFF)
            .with_circuit_breaker_settings(failure_threshold=3, recovery_timeout=30)
            .with_general_settings(operation_timeout=60, enable_circuit_breaker=True)
            .build()
        )

        assert config.retry_config.max_attempts == 5
        assert config.retry_config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF
        assert config.circuit_breaker_config.failure_threshold == 3
        assert config.circuit_breaker_config.recovery_timeout == 30
        assert config.operation_timeout == 60
        assert config.enable_circuit_breaker is True

    def test_none_values_ignored(self):
        """Test that None values are properly ignored. Complexity: A(1)"""
        config = (
            ResilienceConfigBuilder(ResiliencePreset.DEFAULT)
            .with_retry_settings(max_attempts=None, strategy=RetryStrategy.FIXED_DELAY)
            .build()
        )

        # max_attempts should remain default, strategy should be updated
        assert config.retry_config.strategy == RetryStrategy.FIXED_DELAY
        # max_attempts should retain the default value from the preset
        default_config = ResilienceConfigBuilder(ResiliencePreset.DEFAULT).build()
        assert config.retry_config.max_attempts == default_config.retry_config.max_attempts

    def test_high_availability_preset(self):
        """Test high availability preset configuration. Complexity: A(1)"""
        config = ResilienceConfigBuilder(ResiliencePreset.HIGH_AVAILABILITY).build()
        assert config is not None
        # Should have more aggressive settings
        assert config.retry_config.max_attempts >= 3
        assert config.circuit_breaker_config.failure_threshold <= 5

    def test_batch_processing_preset(self):
        """Test batch processing preset configuration. Complexity: A(1)"""
        config = ResilienceConfigBuilder(ResiliencePreset.BATCH_PROCESSING).build()
        assert config is not None
        # Should have more tolerant settings
        assert config.circuit_breaker_config.failure_threshold >= 5


class TestResilienceConfigIntegration:
    """Test the integration between Builder and Factory patterns."""

    def test_factory_vs_builder_equivalence(self):
        """Test that factory and builder produce equivalent results."""
        # Test parameters
        preset = ResiliencePreset.DEFAULT
        retry_max_attempts = 5
        circuit_breaker_failure_threshold = 3
        operation_timeout = 60

        # Create config using factory
        factory_config = ResilienceConfigFactory.create_for_cli(
            preset=preset,
            retry_max_attempts=retry_max_attempts,
            circuit_breaker_failure_threshold=circuit_breaker_failure_threshold,
            operation_timeout=operation_timeout,
        )

        # Create equivalent config using builder
        builder_config = (
            ResilienceConfigBuilder(preset)
            .with_retry_settings(max_attempts=retry_max_attempts)
            .with_circuit_breaker_settings(failure_threshold=circuit_breaker_failure_threshold)
            .with_general_settings(operation_timeout=operation_timeout)
            .build()
        )

        # They should be identical
        assert factory_config.retry_config.max_attempts == builder_config.retry_config.max_attempts
        assert (
            factory_config.circuit_breaker_config.failure_threshold
            == builder_config.circuit_breaker_config.failure_threshold
        )
        assert factory_config.operation_timeout == builder_config.operation_timeout

    def test_disabled_preset_consistency(self):
        """Test that disabled preset is handled consistently."""
        factory_config = ResilienceConfigFactory.create_preset(ResiliencePreset.DISABLED)
        builder_config = ResilienceConfigBuilder(ResiliencePreset.DISABLED).build()

        assert factory_config is None
        assert builder_config is None


if __name__ == "__main__":
    pytest.main([__file__])
