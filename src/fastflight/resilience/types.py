"""
Shared types and enums for resilience patterns.
"""

from enum import Enum
from typing import TypeVar

T = TypeVar("T")


class RetryStrategy(str, Enum):
    """Enumeration of available retry strategies."""

    FIXED_DELAY = "fixed_delay"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    JITTERED_EXPONENTIAL = "jittered_exponential"


class CircuitState(Enum):
    """States of a circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
