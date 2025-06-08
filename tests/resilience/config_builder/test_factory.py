"""
Tests for ResilienceConfigFactory.
"""

import pytest

from fastflight.resilience.config_builder.factory import ResilienceConfigFactory
from fastflight.resilience.config_builder.types import ResiliencePreset
from fastflight.resilience.types import RetryStrategy


class TestResilienceConfigFactory:
    """Test the Factory pattern implementation."""

    def test_create_preset(self):
        """Test preset creation without overrides. Complexity: A(1)"""
        config = ResilienceConfigFactory.create_preset(ResiliencePreset.DEFAULT)
        assert config is not None
        assert config.retry_config is not None

    def test_create_disabled_preset(self):
        """Test disabled preset returns None. Complexity: A(1)"""
        config = ResilienceConfigFactory.create_preset(ResiliencePreset.DISABLED)
        assert config is None

    def test_create_for_production(self):
        """Test production configuration. Complexity: A(1)"""
        config = ResilienceConfigFactory.create_for_production()
        assert config is not None
        assert config.retry_config.max_attempts == 5
        assert config.retry_config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF
        assert config.circuit_breaker_config.failure_threshold == 3
        assert config.circuit_breaker_config.recovery_timeout == 30
        assert config.operation_timeout == 60

    def test_create_for_development(self):
        """Test development configuration. Complexity: A(1)"""
        config = ResilienceConfigFactory.create_for_development()
        assert config is not None
        assert config.retry_config.max_attempts == 2
        assert config.circuit_breaker_config.failure_threshold == 5

    def test_create_for_testing(self):
        """Test testing configuration returns None. Complexity: A(1)"""
        config = ResilienceConfigFactory.create_for_testing()
        assert config is None

    def test_create_for_cli_all_parameters(self):
        """Test CLI factory with all parameters. Complexity: A(1)"""
        config = ResilienceConfigFactory.create_for_cli(
            preset=ResiliencePreset.DEFAULT,
            retry_max_attempts=10,
            retry_strategy=RetryStrategy.LINEAR_BACKOFF,
            retry_base_delay=2.0,
            retry_max_delay=120.0,
            circuit_breaker_failure_threshold=7,
            circuit_breaker_recovery_timeout=45.0,
            circuit_breaker_success_threshold=3,
            operation_timeout=90.0,
            enable_circuit_breaker=True,
            circuit_breaker_name="test-breaker",
        )

        assert config is not None
        assert config.retry_config.max_attempts == 10
        assert config.retry_config.strategy == RetryStrategy.LINEAR_BACKOFF
        assert config.retry_config.base_delay == 2.0
        assert config.retry_config.max_delay == 120.0
        assert config.circuit_breaker_config.failure_threshold == 7
        assert config.circuit_breaker_config.recovery_timeout == 45.0
        assert config.circuit_breaker_config.success_threshold == 3
        assert config.operation_timeout == 90.0
        assert config.enable_circuit_breaker is True
        assert config.circuit_breaker_name == "test-breaker"

    def test_create_for_cli_partial_parameters(self):
        """Test CLI factory with only some parameters. Complexity: A(1)"""
        config = ResilienceConfigFactory.create_for_cli(
            preset=ResiliencePreset.HIGH_AVAILABILITY, retry_max_attempts=3, circuit_breaker_failure_threshold=2
        )

        assert config is not None
        assert config.retry_config.max_attempts == 3
        assert config.circuit_breaker_config.failure_threshold == 2
        # Other values should remain from the HIGH_AVAILABILITY preset
        ha_config = ResilienceConfigFactory.create_preset(ResiliencePreset.HIGH_AVAILABILITY)
        assert config.retry_config.strategy == ha_config.retry_config.strategy
        assert config.circuit_breaker_config.recovery_timeout == ha_config.circuit_breaker_config.recovery_timeout


if __name__ == "__main__":
    pytest.main([__file__])
