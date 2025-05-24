"""
Tests for resilience configuration models using Pydantic validation.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

import pytest
from pydantic import ValidationError

from fastflight.resilience import CircuitBreakerConfig, ResilienceConfig, RetryConfig, RetryStrategy


class TestRetryConfig:
    """Test RetryConfig Pydantic model validation."""

    def test_valid_configuration(self):
        config = RetryConfig(max_attempts=5, base_delay=1.0, max_delay=10.0)
        assert config.max_attempts == 5
        assert config.base_delay == 1.0
        assert config.max_delay == 10.0

    def test_negative_max_attempts_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            RetryConfig(max_attempts=-1)
        assert "greater than or equal to 1" in str(exc_info.value)

    def test_negative_base_delay_rejected(self):
        with pytest.raises(ValidationError):
            RetryConfig(base_delay=-1.0)

    def test_max_delay_less_than_base_delay_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            RetryConfig(base_delay=10.0, max_delay=5.0)
        assert "max_delay" in str(exc_info.value)

    def test_computed_field_calculation(self):
        config = RetryConfig(max_attempts=3, strategy=RetryStrategy.FIXED_DELAY, base_delay=2.0)
        expected_total = 2.0 * (3 - 1)  # 4.0 seconds
        assert abs(config.total_max_delay - expected_total) < 0.001


class TestCircuitBreakerConfig:
    """Test CircuitBreakerConfig Pydantic model validation."""

    def test_valid_configuration(self):
        config = CircuitBreakerConfig(failure_threshold=5, recovery_timeout=30.0)
        assert config.failure_threshold == 5
        assert config.recovery_timeout == 30.0

    def test_zero_failure_threshold_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            CircuitBreakerConfig(failure_threshold=0)
        assert "greater than or equal to 1" in str(exc_info.value)

    def test_negative_recovery_timeout_rejected(self):
        with pytest.raises(ValidationError):
            CircuitBreakerConfig(recovery_timeout=-1.0)

    def test_computed_field_calculation(self):
        config = CircuitBreakerConfig(recovery_timeout=30.0, success_threshold=2, timeout=10.0)
        expected_max_recovery = 30.0 + (2 * 10.0)  # 50.0 seconds
        assert abs(config.max_recovery_time - expected_max_recovery) < 0.001


class TestResilienceConfig:
    """Test ResilienceConfig Pydantic model validation."""

    def test_default_factory_method(self):
        config = ResilienceConfig.create_default()
        assert config.retry_config is not None
        assert config.circuit_breaker_config is not None
        assert config.enable_circuit_breaker is True
        assert config.retry_config.max_attempts == 3
        assert config.circuit_breaker_config.failure_threshold == 5

    def test_high_availability_factory_method(self):
        config = ResilienceConfig.create_for_high_availability()
        assert config.retry_config.max_attempts == 5
        assert config.retry_config.strategy == RetryStrategy.JITTERED_EXPONENTIAL
        assert config.circuit_breaker_config.failure_threshold == 3

    def test_invalid_circuit_breaker_name_pattern(self):
        with pytest.raises(ValidationError) as exc_info:
            ResilienceConfig(
                circuit_breaker_name="invalid name with spaces!", circuit_breaker_config=CircuitBreakerConfig()
            )
        assert "String should match pattern" in str(exc_info.value)

    def test_method_chaining(self):
        base_config = ResilienceConfig.create_default()
        chained_config = base_config.with_retry_config(RetryConfig(max_attempts=10)).with_circuit_breaker_name(
            "test_circuit"
        )
        assert chained_config.retry_config.max_attempts == 10
        assert chained_config.circuit_breaker_name == "test_circuit"
        # Original config unchanged
        assert base_config.retry_config.max_attempts == 3
