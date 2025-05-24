"""
Tests for retry logic and behavior.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

import pytest

from fastflight.exceptions import FastFlightConnectionError, FastFlightTimeoutError
from fastflight.resilience import RetryConfig, RetryStrategy


class TestRetryBehavior:
    """Test retry logic and decision-making."""

    def test_should_retry_with_retryable_exception(self):
        config = RetryConfig()
        assert config.is_retryable_exception(FastFlightConnectionError("test"))
        assert config.is_retryable_exception(FastFlightTimeoutError("test"))
        assert not config.is_retryable_exception(ValueError("test"))

    def test_has_attempts_remaining(self):
        config = RetryConfig(max_attempts=3)
        assert config.has_attempts_remaining(1) is True
        assert config.has_attempts_remaining(2) is True
        assert config.has_attempts_remaining(3) is False


class TestRetryDelayCalculation:
    """Test delay calculation for different retry strategies."""

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

    def test_linear_backoff_strategy(self):
        config = RetryConfig(strategy=RetryStrategy.LINEAR_BACKOFF, base_delay=1.0)
        assert config.calculate_delay(1) == 1.0  # 1.0 * 1
        assert config.calculate_delay(2) == 2.0  # 1.0 * 2
        assert config.calculate_delay(3) == 3.0  # 1.0 * 3

    def test_delay_respects_max_delay(self):
        config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF, base_delay=1.0, max_delay=5.0, exponential_base=2.0
        )
        # Should be capped at max_delay
        assert config.calculate_delay(10) == 5.0

    def test_invalid_attempt_number_raises_error(self):
        config = RetryConfig()
        with pytest.raises(ValueError, match="Retry attempt must be positive"):
            config.calculate_delay(0)
        with pytest.raises(ValueError, match="Retry attempt must be positive"):
            config.calculate_delay(-1)
