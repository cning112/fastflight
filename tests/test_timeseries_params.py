import json
from datetime import datetime, timezone

from fastflight import BaseParams
from fastflight.core.timeseries import TimeSeriesParams


def test_timeseries_params_basic():
    """Test basic TimeSeriesParams functionality."""
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 2, tzinfo=timezone.utc)

    params = TimeSeriesParams(start_time=start, end_time=end)

    assert params.start_time == start
    assert params.end_time == end
    assert params.fqn() == "fastflight.core.timeseries.TimeSeriesParams"


def test_timeseries_params_serialization():
    """Test that TimeSeriesParams can be serialized and deserialized."""
    start = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

    params = TimeSeriesParams(start_time=start, end_time=end)

    # Test serialization
    json_data = json.loads(params.to_json_bytes())
    assert "param_type" in json_data
    assert json_data["param_type"] == params.fqn()

    # Test deserialization
    # Need to register TimeSeriesParams first to make it deserializable
    BaseParams._register(TimeSeriesParams)
    serialized_bytes = params.to_json_bytes()
    deserialized = TimeSeriesParams.from_bytes(serialized_bytes)

    assert deserialized.start_time == start
    assert deserialized.end_time == end
