import json

import pyarrow as pa
import pytest
from pytest_asyncio.plugin import AsyncGenerator

from fastflight.data_service_base import BaseDataService, BaseParams, RegistryManager


# Sample Params class
class SampleParams(BaseParams):
    data_type = "sample_params"
    some_field: str


# Sample Data Service
class SampleDataService(BaseDataService[SampleParams]):
    async def aget_batches(
        self, params: SampleParams, batch_size: int | None = None
    ) -> AsyncGenerator[pa.RecordBatch, None]:
        yield pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], ["sample_column"])


@pytest.fixture(autouse=True)
def clear_registry():
    """Ensures that the RegistryManager is cleared before each test to prevent contamination."""
    RegistryManager._params_registry.clear()
    RegistryManager._service_registry.clear()
    RegistryManager._data_type_registry.clear()


@pytest.fixture(autouse=True)
def setup_registry():
    """Ensures that SampleParams and SampleDataService are registered before tests."""
    RegistryManager.register_service(SampleParams, SampleDataService)


# ---------------------- TESTS FOR BaseParams ----------------------


def test_baseparams_serialization():
    """Test serialization and deserialization of BaseParams."""
    params = SampleParams(some_field="test_value")

    serialized = params.to_bytes()
    assert isinstance(serialized, bytes)

    deserialized = BaseParams.from_bytes(serialized)
    assert isinstance(deserialized, SampleParams)
    assert deserialized.some_field == "test_value"


def test_baseparams_to_json():
    """Test converting BaseParams to JSON format."""
    params = SampleParams(some_field="test_value")
    json_data = params.to_json()

    assert json_data["data_type"] == "sample_params"
    assert json_data["some_field"] == "test_value"


def test_baseparams_from_bytes_invalid():
    """Test deserialization with invalid data."""
    with pytest.raises(json.JSONDecodeError):
        BaseParams.from_bytes(b"invalid_json")


def test_baseparams_lookup_unregistered():
    """Test lookup of an unregistered BaseParams class."""
    assert RegistryManager.lookup_params("non_existent") is None


# ---------------------- TESTS FOR RegistryManager ----------------------


def test_register_service():
    """Test registering a BaseDataService."""
    RegistryManager.register_service(SampleParams, SampleDataService)

    assert RegistryManager.lookup_service("sample_params") is SampleDataService
    assert RegistryManager.lookup_service(SampleParams.qual_name()) is SampleDataService


def test_register_duplicate_service():
    """Test duplicate registration of a service should raise an error."""
    RegistryManager.register_service(SampleParams, SampleDataService)
    RegistryManager.register_service(SampleParams, SampleDataService)


def test_lookup_unregistered_service():
    """Test lookup of an unregistered service."""
    assert RegistryManager.lookup_service("non_existent") is None


def test_register_service_with_unregistered_params():
    """Test that registering a service should also register the params."""
    RegistryManager.register_service(SampleParams, SampleDataService)

    assert RegistryManager.lookup_params("sample_params") is SampleParams


def test_unregister_service():
    """Test unregistering BaseDataService."""
    RegistryManager.register_service(SampleParams, SampleDataService)
    RegistryManager.unregister_service("sample_params")

    assert RegistryManager.lookup_service("sample_params") is None
    assert RegistryManager.lookup_service(SampleParams.qual_name()) is None


def test_unregister_nonexistent():
    """Test unregistering a non-existent service or params does nothing."""
    RegistryManager.unregister_service("non_existent")
    RegistryManager.unregister("non_existent")  # Should not raise an error


def test_register_service_without_alias():
    """Test registering a service without an alias still allows lookup by data_type."""
    RegistryManager.register_service(SampleParams, SampleDataService)
    assert RegistryManager.lookup_service("sample_params") is SampleDataService


# ---------------------- TESTS FOR BaseDataService ----------------------


def test_basedataservice_abstract_methods():
    """Test that BaseDataService cannot be instantiated directly."""
    with pytest.raises(TypeError):
        BaseDataService()  # Abstract class should not be instantiated


@pytest.mark.asyncio
async def test_sampledataservice_aget_batches():
    """Test that SampleDataService returns a valid RecordBatch."""
    service = SampleDataService()
    params = SampleParams(some_field="test")

    async for batch in service.aget_batches(params):
        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_columns == 1
        assert batch.column(0).to_pylist() == [1, 2, 3]
