from datetime import datetime, timezone

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
    json_data = params.to_json()
    assert "param_type" in json_data
    assert json_data["param_type"] == params.fqn()

    # Test deserialization
    serialized_bytes = params.to_bytes()
    deserialized = TimeSeriesParams.from_bytes(serialized_bytes)

    assert deserialized.start_time == start
    assert deserialized.end_time == end
