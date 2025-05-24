"""
Tests for circuit breaker functionality.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

import asyncio

import pytest

from fastflight.exceptions import FastFlightCircuitOpenError, FastFlightConnectionError
from fastflight.resilience import CircuitBreaker, CircuitBreakerConfig, CircuitState


class TestCircuitBreakerBasics:
    """Test basic circuit breaker functionality."""

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
    async def test_successful_call_keeps_circuit_closed(self, circuit_breaker):
        async def successful_func():
            return "success"

        result = await circuit_breaker.call(successful_func)
        assert result == "success"
        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_circuit_opens_after_failure_threshold(self, circuit_breaker):
        async def failing_func():
            raise FastFlightConnectionError("Always fails")

        # First failure
        with pytest.raises(FastFlightConnectionError):
            await circuit_breaker.call(failing_func)
        assert circuit_breaker.state == CircuitState.CLOSED

        # Second failure should open circuit
        with pytest.raises(FastFlightConnectionError):
            await circuit_breaker.call(failing_func)
        assert circuit_breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_open_circuit_rejects_calls(self, circuit_breaker):
        # Force circuit open by exceeding failure threshold
        async def failing_func():
            raise FastFlightConnectionError("Always fails")

        for _ in range(2):
            with pytest.raises(FastFlightConnectionError):
                await circuit_breaker.call(failing_func)

        # Now circuit should reject calls
        with pytest.raises(FastFlightCircuitOpenError):
            await circuit_breaker.call(failing_func)


class TestCircuitBreakerRecovery:
    """Test circuit breaker recovery behavior."""

    @pytest.mark.asyncio
    async def test_circuit_transitions_to_half_open_after_timeout(self):
        config = CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.1, success_threshold=1)
        circuit = CircuitBreaker("recovery_test", config)

        # Open the circuit
        async def failing_func():
            raise FastFlightConnectionError("Fail")

        with pytest.raises(FastFlightConnectionError):
            await circuit.call(failing_func)
        assert circuit.state == CircuitState.OPEN

        # Wait for recovery timeout
        await asyncio.sleep(0.2)

        # First successful call should transition to CLOSED
        async def success_func():
            return "ok"

        result = await circuit.call(success_func)
        assert result == "ok"
        # With success_threshold=1, should close after one success
        assert circuit.state == CircuitState.CLOSED
