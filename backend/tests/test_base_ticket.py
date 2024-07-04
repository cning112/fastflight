from my_fastapi.internal.data_server.models.base_ticket import BaseTicket
from my_fastapi.internal.data_server.models.tickets import SQLQueryTicket


def test_base_ticket_serialization_and_deserialization():
    ticket = SQLQueryTicket(query="select 1 as a")
    b = ticket.to_bytes()
    new_ticket = BaseTicket.from_bytes(b)
    assert new_ticket == ticket
    assert new_ticket.to_bytes() == b
