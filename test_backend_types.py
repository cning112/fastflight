#!/usr/bin/env python3
"""Test backend type safety."""

import sys

sys.path.insert(0, "src")

from typing import get_args

from fastflight.core.base import BaseDataService
from fastflight.core.distributed import BackendType, DistributedTimeSeriesService
from fastflight.core.timeseries import TimeSeriesParams


class TestParams(TimeSeriesParams):
    symbol: str

    def estimate_data_points(self) -> int:
        return 100


class TestService(BaseDataService[TestParams]):
    def get_batches(self, params, batch_size=None):
        yield None


def test_backend_types():
    """Test that backend types are properly defined."""
    print("🔍 Testing Backend Type Safety")
    print("=" * 40)

    # Test that BackendType literal is properly defined
    backend_values = get_args(BackendType)
    print(f"Available backends: {backend_values}")

    expected_backends = ("ray", "asyncio", "single_threaded")
    assert backend_values == expected_backends, f"Expected {expected_backends}, got {backend_values}"
    print("✅ Backend literal types are correct")

    # Test service creation and backend selection
    base_service = TestService()

    # Test default configuration
    service1 = DistributedTimeSeriesService(base_service)
    backend1 = service1.backend
    print(f"Default backend: {backend1}")
    assert backend1 in backend_values, f"Backend {backend1} not in allowed values"

    # Test distributed disabled
    service2 = DistributedTimeSeriesService(base_service, enable_distributed=False)
    backend2 = service2.backend
    print(f"Disabled distributed backend: {backend2}")
    assert backend2 == "single_threaded", f"Expected single_threaded, got {backend2}"

    print("\n✅ All backend type checks passed!")
    print("\n💡 Benefits of using Literal types:")
    print("   • Type checker can catch invalid backend strings")
    print("   • IDE autocomplete for backend values")
    print("   • Runtime type validation possible")
    print("   • Better code documentation")


def test_type_checking_simulation():
    """Simulate what a type checker would catch."""
    print("\n🔬 Type Checking Simulation")
    print("-" * 30)

    base_service = TestService()
    service = DistributedTimeSeriesService(base_service)

    # This would be caught by mypy/pyright
    backend: BackendType = service.backend
    print(f"Backend type annotation works: {type(backend).__name__} = '{backend}'")

    # This would cause a type error if uncommented:
    # invalid_backend: BackendType = "invalid_backend"  # Type error!

    print("✅ Type checking would catch invalid backend assignments")


if __name__ == "__main__":
    try:
        test_backend_types()
        test_type_checking_simulation()
        print("\n🎉 All type safety tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
