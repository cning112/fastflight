"""
Tests for enhanced error handling, retry mechanisms, and circuit breaker patterns.
"""

import pytest

from fastflight.exceptions import (
    FastFlightCircuitOpenError,
    FastFlightConnectionError,
    FastFlightError,
    FastFlightRetryExhaustedError,
    FastFlightTimeoutError,
)
from fastflight.resilience import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    ResilienceConfig,
    ResilienceManager,
    RetryConfig,
    RetryStrategy,
)


class TestRetryConfig:
    """Test retry configuration and delay calculations."""

    def test_fixed_delay_strategy(self):
        config = RetryConfig(strategy=RetryStrategy.FIXED_DELAY, base_delay=2.0)

        assert config.calculate_delay(1) == 2.0
        assert config.calculate_delay(2) == 2.0
        assert config.calculate_delay(3) == 2.0

    def test_exponential_backoff_strategy(self):
        config = RetryConfig(strategy=RetryStrategy.EXPONENTIAL_BACKOFF, base_delay=1.0, exponential_base=2.0)

        assert config.calculate_delay(1) == 1.0  # 1.0 * 2^0
        assert config.calculate_delay(2) == 2.0  # 1.0 * 2^1
        assert config.calculate_delay(3) == 4.0  # 1.0 * 2^2

    def test_has_attempts_remaining(self):
        config = RetryConfig(max_attempts=3)
        assert config.has_attempts_remaining(1) is True
        assert config.has_attempts_remaining(2) is True
        assert config.has_attempts_remaining(3) is False

    def test_should_retry_logic(self):
        config = RetryConfig(retryable_exceptions=(FastFlightConnectionError, FastFlightTimeoutError))

        # Should retry for retryable exceptions
        assert config.should_retry(FastFlightConnectionError("test"))
        assert config.should_retry(FastFlightTimeoutError("test"))

        # Should not retry for non-retryable exceptions
        assert not config.should_retry(ValueError("test"))


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    @pytest.fixture
    def circuit_config(self):
        return CircuitBreakerConfig(failure_threshold=2, recovery_timeout=1.0, success_threshold=1)

    @pytest.fixture
    def circuit_breaker(self, circuit_config):
        return CircuitBreaker("test_circuit", circuit_config)

    @pytest.mark.asyncio
    async def test_initial_state_is_closed(self, circuit_breaker):
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_successful_call(self, circuit_breaker):
        async def successful_func():
            return "success"

        result = await circuit_breaker.call(successful_func)
        assert result == "success"
        assert circuit_breaker.state == CircuitState.CLOSED


class TestFastFlightExceptions:
    """Test the exception hierarchy and error handling utilities."""

    def test_exception_hierarchy(self):
        # Test that all custom exceptions inherit from FastFlightError
        exceptions = [
            FastFlightConnectionError("test"),
            FastFlightTimeoutError("test"),
            FastFlightCircuitOpenError("test", "circuit"),
            FastFlightRetryExhaustedError("test", 3),
        ]

        for exc in exceptions:
            assert isinstance(exc, FastFlightError)

    def test_exception_attributes(self):
        # Test FastFlightTimeoutError specific attributes
        timeout_exc = FastFlightTimeoutError("Timeout occurred", timeout_duration=30.0)
        assert timeout_exc.timeout_duration == 30.0
        assert timeout_exc.message == "Timeout occurred"

        # Test FastFlightCircuitOpenError specific attributes
        circuit_exc = FastFlightCircuitOpenError("Circuit is open", "test_circuit", retry_after=60.0)
        assert circuit_exc.circuit_name == "test_circuit"
        assert circuit_exc.retry_after == 60.0


class TestResilienceConfig:
    """Test the unified ResilienceConfig data class."""

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
        assert config.circuit_breaker_config.recovery_timeout == 15.0

    def test_batch_processing_factory_method(self):
        config = ResilienceConfig.create_for_batch_processing()

        assert config.retry_config.max_attempts == 2
        assert config.retry_config.strategy == RetryStrategy.FIXED_DELAY
        assert config.circuit_breaker_config.failure_threshold == 10
        assert config.circuit_breaker_config.recovery_timeout == 60.0

    def test_fluent_api_methods(self):
        base_config = ResilienceConfig.create_default()

        # Test with_retry_config
        new_retry = RetryConfig(max_attempts=10)
        config_with_retry = base_config.with_retry_config(new_retry)
        assert config_with_retry.retry_config.max_attempts == 10
        assert base_config.retry_config.max_attempts == 3  # Original unchanged

        # Test with_circuit_breaker_name
        config_with_name = base_config.with_circuit_breaker_name("test_circuit")
        assert config_with_name.circuit_breaker_name == "test_circuit"

        # Test disable_circuit_breaker
        config_disabled = base_config.disable_circuit_breaker()
        assert config_disabled.enable_circuit_breaker is False
        assert base_config.enable_circuit_breaker is True  # Original unchanged

    def test_method_chaining(self):
        config = (
            ResilienceConfig.create_default()
            .with_retry_config(RetryConfig(max_attempts=5))
            .with_circuit_breaker_name("chained_circuit")
            .disable_circuit_breaker()
        )

        assert config.retry_config.max_attempts == 5
        assert config.circuit_breaker_name == "chained_circuit"
        assert config.enable_circuit_breaker is False


class TestResilienceManagerWithConfig:
    """Test ResilienceManager with unified configuration."""

    @pytest.fixture
    def manager(self):
        return ResilienceManager()

    @pytest.mark.asyncio
    async def test_execute_with_config_retry_only(self, manager):
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
    async def test_execute_with_config_circuit_breaker_only(self, manager):
        config = ResilienceConfig(
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=1),
            circuit_breaker_name="test_circuit",
            enable_circuit_breaker=True,
            retry_config=None,  # No retry
        )

        async def failing_func():
            raise FastFlightConnectionError("Always fails")

        # First call should trigger circuit breaker to open
        with pytest.raises(FastFlightConnectionError):
            await manager.execute_with_resilience(failing_func, config=config)

        # Second call should be rejected by open circuit
        with pytest.raises(FastFlightCircuitOpenError):
            await manager.execute_with_resilience(failing_func, config=config)

    @pytest.mark.asyncio
    async def test_execute_with_config_both_patterns(self, manager):
        config = ResilienceConfig(
            retry_config=RetryConfig(max_attempts=2, base_delay=0.01),
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3),
            circuit_breaker_name="combined_circuit",
            enable_circuit_breaker=True,
        )

        call_count = 0

        async def func():
            nonlocal call_count
            call_count += 1
            raise FastFlightConnectionError("Always fails")

        # Should exhaust retries before circuit breaker opens
        with pytest.raises(FastFlightRetryExhaustedError):
            await manager.execute_with_resilience(func, config=config)

        assert call_count == 2  # Should have retried once

    def test_update_default_config(self, manager):
        new_config = ResilienceConfig.create_for_high_availability()
        manager.update_default_config(new_config)

        assert manager.default_config == new_config
        assert manager.default_config.retry_config.max_attempts == 5


if __name__ == "__main__":
    pytest.main([__file__])
