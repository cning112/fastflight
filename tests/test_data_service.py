from typing import Any, AsyncIterable, Iterable, Type

import pyarrow as pa
import pytest
from pyarrow import RecordBatch

from fastflight.data_services import BaseDataService, BaseParams


# Sample Params class
class SampleParams(BaseParams):
    some_field: str

    @classmethod
    def default_service_class(cls) -> Type["BaseDataService"]:
        return SampleDataService


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

    @classmethod
    def default_service_class(cls) -> Type["BaseDataService"]:
        return SampleDataServiceAsync


# Sample Data Service
class SampleDataServiceAsync(BaseDataService[SampleParams]):
    async def aget_batches(
        self, params: SampleParams, batch_size: int | None = None
    ) -> AsyncIterable[RecordBatch | Any]:
        def gen():
            yield pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], ["sample_column"])

        return gen()


@pytest.mark.asyncio
async def test_sampledataservice_aget_batches():
    """Test that SampleDataService returns a valid RecordBatch asynchronously."""
    service = SampleDataServiceAsync()
    params = SampleParamsAsync(some_field="test")

    batches = []
    async for batch in service.aget_batches(params):
        batches.append(batch)

    assert len(batches) == 1
    assert isinstance(batches[0], pa.RecordBatch)
    assert batches[0].num_columns == 1
    assert batches[0].column(0).to_pylist() == [1, 2, 3]
