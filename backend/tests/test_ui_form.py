import pytest
from pydantic import TypeAdapter

from my_fastapi.routers.ui_form import TenorBusDay, TenorYearMonth

tym = TypeAdapter(TenorYearMonth)
tb = TypeAdapter(TenorBusDay)


@pytest.mark.parametrize(
    "value, valid",
    [
        ("1y", True),
        ("1y4M", True),
        ("-1Y", True),
        ("-1Y4M", True),
        ("1y1y", False),
        ("1m1m", False),
        (" 01y", False),
        ("-1y04M", False),
        ("-1M4Y", False),
        ("1M4Y", False),
        ("Y", False),
        ("1", False),
        ("-1", False),
        ("-M", False),
    ],
)
def test_tenor_year_month(value, valid):
    if valid:
        assert tym.validate_python(value)
    else:
        with pytest.raises(ValueError):
            tym.validate_python(value)


@pytest.mark.parametrize(
    "value, valid",
    [("1b", True), ("-1B", True), ("B", False), ("-B", False), ("1", False)],
)
def test_tenor_bus_day(value, valid):
    if valid:
        assert tb.validate_python(value)
    else:
        with pytest.raises(ValueError):
            tb.validate_python(value)
