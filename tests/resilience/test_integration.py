"""
Integration tests for resilience patterns working together.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))


import pytest

from fastflight.exceptions import FastFlightCircuitOpenError, FastFlightConnectionError, FastFlightRetryExhaustedError
from fastflight.resilience import CircuitBreakerConfig, ResilienceConfig, ResilienceManager, RetryConfig


class TestResilienceIntegration:
    """Test retry and circuit breaker working together."""

    @pytest.fixture
    def manager(self):
        return ResilienceManager()

    @pytest.mark.asyncio
    async def test_retry_only_configuration(self, manager):
        call_count = 0

        async def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise FastFlightConnectionError("Temporary failure")
            return f"success_after_{call_count}_attempts"

        config = ResilienceConfig(
            retry_config=RetryConfig(max_attempts=5, base_delay=0.01), enable_circuit_breaker=False
        )

        result = await manager.execute_with_resilience(flaky_func, config=config)
        assert result == "success_after_3_attempts"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_circuit_breaker_only_configuration(self, manager):
        config = ResilienceConfig(
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=1),
            circuit_breaker_name="test_circuit",
            enable_circuit_breaker=True,
            retry_config=None,
        )

        async def failing_func():
            raise FastFlightConnectionError("Always fails")

        # First call triggers circuit breaker
        with pytest.raises(FastFlightConnectionError):
            await manager.execute_with_resilience(failing_func, config=config)

        # Second call rejected by open circuit
        with pytest.raises(FastFlightCircuitOpenError):
            await manager.execute_with_resilience(failing_func, config=config)

    @pytest.mark.asyncio
    async def test_combined_retry_and_circuit_breaker(self, manager):
        config = ResilienceConfig(
            retry_config=RetryConfig(max_attempts=2, base_delay=0.01),
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3),
            circuit_breaker_name="combined_circuit",
            enable_circuit_breaker=True,
        )

        call_count = 0

        async def always_failing_func():
            nonlocal call_count
            call_count += 1
            raise FastFlightConnectionError("Always fails")

        # Should exhaust retries before hitting circuit breaker threshold
        with pytest.raises(FastFlightRetryExhaustedError):
            await manager.execute_with_resilience(always_failing_func, config=config)

        assert call_count == 2  # Retried once

    def test_config_factory_methods_work_together(self, manager):
        # Test different factory configurations
        default_config = ResilienceConfig.create_default()
        ha_config = ResilienceConfig.create_for_high_availability()
        batch_config = ResilienceConfig.create_for_batch_processing()

        # Verify they have expected properties
        assert default_config.retry_config.max_attempts == 3
        assert ha_config.retry_config.max_attempts == 5
        assert batch_config.retry_config.max_attempts == 2

        assert default_config.circuit_breaker_config.failure_threshold == 5
        assert ha_config.circuit_breaker_config.failure_threshold == 3
        assert batch_config.circuit_breaker_config.failure_threshold == 10
