"""
Resilience configuration types and enums.
"""

from enum import Enum


class ResiliencePreset(str, Enum):
    """Available resilience configuration presets."""

    DISABLED = "disabled"
    DEFAULT = "default"
    HIGH_AVAILABILITY = "high_availability"
    BATCH_PROCESSING = "batch_processing"
