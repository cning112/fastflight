"""
Resilience configuration builders and factories.

This module provides modern configuration management specifically for resilience features:
- Builder pattern for flexible configuration creation
- Factory pattern for common configuration scenarios
- Type definitions for resilience presets
"""

from .builder import ResilienceConfigBuilder
from .factory import ResilienceConfigFactory
from .types import ResiliencePreset

__all__ = ["ResilienceConfigBuilder", "ResilienceConfigFactory", "ResiliencePreset"]
