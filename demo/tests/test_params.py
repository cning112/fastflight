from demo.flight_service.models.params import SqlParams
from fastflight.services.base_params import BaseParams


def test_base_ticket_serialization_and_deserialization():
    ticket = SqlParams(query="select 1 as a")
    b = ticket.to_bytes()
    new_params = BaseParams.from_bytes(b)
    assert new_params == ticket
    assert new_params.to_bytes() == b
