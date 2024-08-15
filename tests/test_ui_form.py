import pytest
from pydantic import TypeAdapter
from starlette.testclient import TestClient

from my_fastapi.internal.ui_form.types import TenorBusDay, TenorYearMonth
from my_fastapi.main import app

tym = TypeAdapter(TenorYearMonth)
tb = TypeAdapter(TenorBusDay)

client = TestClient(app)
client.base_url = client.base_url.join("/api")


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


@pytest.mark.parametrize("value, valid", [("1b", True), ("-1B", True), ("B", False), ("-B", False), ("1", False)])
def test_tenor_bus_day(value, valid):
    if valid:
        assert tb.validate_python(value)
    else:
        with pytest.raises(ValueError):
            tb.validate_python(value)


@pytest.mark.parametrize(
    "value, expected_errors",
    [
        ({"start": "1y"}, [{"loc": ["body", "end"], "msg": "Field required", "type": "missing"}]),
        (
            {"start": "1y", "end": 1},
            [
                {
                    "loc": ["body", "end", "date"],
                    "msg": "Datetimes provided to dates should have zero time - e.g. be exact dates",
                }
            ],
        ),
    ],
)
def test_invalid_ui_form(value, expected_errors):
    response = client.post("/ui_form/validate", json=value)
    assert response.status_code == 422
    actual_errors = response.json()["errors"]

    for ee in expected_errors:
        assert any(e for e in actual_errors if all(e[key] == v for key, v in ee.items()))


@pytest.mark.parametrize(
    "value, expected_validated, expected_feedback",
    [
        ({"start": "-2y", "end": "-1y"}, {"start": "-2Y", "end": "-1Y"}, []),
        (
            {"start": "2024-01-01", "end": "1y"},
            {"start": "2024-01-01", "end": "1Y"},
            [{"field": "end", "msg": "`end` must be a date if `start` is a date"}],
        ),
    ],
)
def test_partial_ui_form(value, expected_validated, expected_feedback):
    response = client.post("/ui_form/validate", json=value)
    assert response.status_code == 200
    actual_validated = response.json()["validated"]
    actual_feedback = response.json()["feedback"]
    assert actual_validated == expected_validated
    assert actual_feedback == expected_feedback
