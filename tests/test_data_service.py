import json
from typing import Any, AsyncIterable, Iterable

import pyarrow as pa
import pytest
from pyarrow import RecordBatch

from fastflight.core.base import BaseDataService, BaseParams


# Sample Params class
class SampleParams(BaseParams):
    some_field: str


# Sample Data Service
class SampleDataService(BaseDataService[SampleParams]):
    def get_batches(self, params: SampleParams, batch_size: int | None = None) -> Iterable[RecordBatch | Any]:
        yield pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], ["sample_column"])


def test_sampledataservice_get_batches():
    """Test that SampleDataService returns a valid RecordBatch in sync mode."""
    service = SampleDataService()
    params = SampleParams(some_field="test")

    batches = list(service.get_batches(params))
    assert len(batches) == 1
    assert isinstance(batches[0], pa.RecordBatch)
    assert batches[0].num_columns == 1
    assert batches[0].column(0).to_pylist() == [1, 2, 3]


# Sample Params class
class SampleParamsAsync(BaseParams):
    some_field: str


# Sample Data Service
class SampleDataServiceAsync(BaseDataService[SampleParamsAsync]):
    async def aget_batches(
        self, params: SampleParamsAsync, batch_size: int | None = None
    ) -> AsyncIterable[RecordBatch | Any]:
        async def gen():
            yield pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], ["sample_column"])

        return gen()


@pytest.mark.asyncio
async def test_sampledataservice_aget_batches():
    """Test that SampleDataService returns a valid RecordBatch asynchronously."""
    service = SampleDataServiceAsync()
    params = SampleParamsAsync(some_field="test")

    batches = []
    async for batch in await service.aget_batches(params):
        batches.append(batch)

    assert len(batches) == 1
    assert isinstance(batches[0], pa.RecordBatch)
    assert batches[0].num_columns == 1
    assert batches[0].column(0).to_pylist() == [1, 2, 3]


# Test duplicate param registration raises
def test_duplicate_param_registration_raises():
    class MyParams(BaseParams):
        foo: str

    # Register once (should succeed)
    class MyService(BaseDataService[MyParams]):
        def get_batches(self, params, batch_size=None):
            yield pa.RecordBatch.from_arrays([pa.array([1])], ["col"])

    # Register again (should raise ValueError)
    with pytest.raises(ValueError):
        BaseDataService._register(MyParams, MyService)


# Test duplicate service registration raises
def test_duplicate_service_registration_raises():
    class MyParams2(BaseParams):
        bar: str

    class MyService2(BaseDataService[MyParams2]):
        def get_batches(self, params, batch_size=None):
            yield pa.RecordBatch.from_arrays([pa.array([1])], ["col"])

    # Try to register again for the same param class
    with pytest.raises(ValueError):
        BaseDataService._register(MyParams2, MyService2)


# Test from_bytes unknown param class raises
def test_from_bytes_unknown_param_class_raises():
    # Create a JSON bytes blob with an unknown _params_class
    bad_json = {"param_type": "nonexistent_module.NonexistentParams", "foo": "bar"}
    blob = json.dumps(bad_json).encode("utf-8")
    with pytest.raises(ValueError):
        BaseParams.from_bytes(blob)


# Test from_bytes missing param class raises
def test_from_bytes_missing_param_class_raises():
    # Create a JSON bytes blob with missing fqn
    bad_json = {"foo": "bar"}
    blob = json.dumps(bad_json).encode("utf-8")
    with pytest.raises(KeyError):
        BaseParams.from_bytes(blob)
