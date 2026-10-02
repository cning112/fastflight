from collections.abc import Iterable

import pyarrow as pa
import pytest

from fastflight.core.base import BaseDataService
from fastflight.demo_services.duckdb_demo import DuckDBDataService, DuckDBParams


def _collect_sync(
    service: DuckDBDataService, params: DuckDBParams, batch_size: int | None = None
) -> list[pa.RecordBatch]:
    return list(service.get_batches(params, batch_size))


async def _collect_async(
    service: DuckDBDataService, params: DuckDBParams, batch_size: int | None = None
) -> list[pa.RecordBatch]:
    return [batch async for batch in service.aget_batches(params, batch_size)]


def _column_values(batches: Iterable[pa.RecordBatch], column: str) -> list[int]:
    values: list[int] = []
    for batch in batches:
        values.extend(batch.column(batch.schema.get_field_index(column)).to_pylist())
    return values


def test_duckdb_params_registration() -> None:
    """DuckDBParams is registered under its canonical fully qualified name."""
    assert DuckDBParams.fqn() == "fastflight.demo_services.duckdb_demo.DuckDBParams"
    assert BaseDataService.lookup(DuckDBParams.fqn()) is DuckDBDataService


def test_sync_full_scan_yields_all_rows() -> None:
    service = DuckDBDataService()
    params = DuckDBParams(query="SELECT * FROM range(0, 100) AS t(i)")

    batches = _collect_sync(service, params)

    assert sum(batch.num_rows for batch in batches) == 100
    assert sorted(_column_values(batches, "i")) == list(range(100))


def test_batch_slicing_splits_large_batch() -> None:
    service = DuckDBDataService()
    params = DuckDBParams(query="SELECT * FROM range(0, 100) AS t(i)")

    batches = _collect_sync(service, params, batch_size=3)

    assert len(batches) == 34
    assert all(batch.num_rows > 0 for batch in batches)
    assert all(batch.num_rows <= 3 for batch in batches)
    assert batches[0].num_rows == 3
    assert batches[-1].num_rows == 1
    assert _column_values(batches, "i") == list(range(100))


def test_query_parameters_are_applied() -> None:
    service = DuckDBDataService()
    params = DuckDBParams(query="SELECT * FROM range(0, 100) AS t(i) WHERE i > ?", parameters=[95])

    batches = _collect_sync(service, params)

    assert _column_values(batches, "i") == [96, 97, 98, 99]


def test_empty_result_yields_no_batches() -> None:
    service = DuckDBDataService()
    params = DuckDBParams(query="SELECT * FROM range(0, 100) AS t(i) WHERE i > 1000")

    batches = _collect_sync(service, params)

    assert batches == []


@pytest.mark.asyncio
async def test_async_matches_sync() -> None:
    service = DuckDBDataService()
    params = DuckDBParams(query="SELECT * FROM range(0, 100) AS t(i)")

    sync_batches = _collect_sync(service, params)
    async_batches = await _collect_async(service, params)

    assert _column_values(async_batches, "i") == _column_values(sync_batches, "i")
    assert sum(batch.num_rows for batch in async_batches) == 100


def test_aggregation_single_table_path() -> None:
    service = DuckDBDataService()
    params = DuckDBParams(query="SELECT count(*) AS n, sum(i) AS s FROM range(0, 100) AS t(i)")

    batches = _collect_sync(service, params)

    assert len(batches) == 1
    row = batches[0].to_pylist()[0]
    assert row["n"] == 100
    assert row["s"] == 4950
