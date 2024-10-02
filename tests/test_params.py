from fastflight.data_service_base import BaseParams
from fastflight.data_services.sql_service import SQLParams


def test_base_ticket_serialization_and_deserialization():
    ticket = SQLParams(query="select 1 as a", connection_string="sqlite:///example.db")
    b = ticket.to_bytes()
    new_params = BaseParams.from_bytes(b)
    assert new_params == ticket
    assert new_params.to_bytes() == b
