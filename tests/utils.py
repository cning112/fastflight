import threading
import time
import unittest

import pandas as pd
import pyarrow as pa
from pyarrow import flight as flight


class SimpleFlightServer(flight.FlightServerBase):
    def __init__(self, location: str, data_map: dict, simulate_error: bool = False):
        """
        Initialize the Flight server with a mapping from ticket bytes to pyarrow.Table.
        :param location: Server location.
        :param data_map: A dictionary mapping ticket bytes to pyarrow.Table.
        :param simulate_error: Whether to simulate an error in do_get.
        """
        super().__init__(location)
        self._data_map = data_map
        self._default_table = next(iter(data_map.values())) if data_map else None
        self.simulate_error = simulate_error

    def set_data_map(self, data_map: dict):
        """Update the data mapping for the server."""
        self._data_map = data_map
        self._default_table = next(iter(data_map.values())) if data_map else None

    def set_simulate_error(self, simulate_error: bool):
        """Enable or disable error simulation."""
        self.simulate_error = simulate_error

    def do_get(self, context, ticket):
        """
        Return a RecordBatchStream for the table corresponding to the ticket.
        If simulate_error is True, raise a RuntimeError.
        """
        if self.simulate_error:
            raise RuntimeError("Simulated server error")
        table = self._data_map.get(ticket.ticket, self._default_table)
        return flight.RecordBatchStream(table)


class FlightServerTestCase(unittest.IsolatedAsyncioTestCase):
    """
    Shared test case class that starts a Flight server before all tests and shuts it down after all tests.
    Child classes can override or modify the server configuration (data map or error simulation) per test.
    """

    @classmethod
    def setUpClass(cls):
        cls.initial_data = cls.get_server_data()
        cls.location = "grpc://0.0.0.0:18181"
        # Initialize the Flight server with default data.
        cls.server = SimpleFlightServer(cls.location, cls.initial_data)
        cls.server_thread = threading.Thread(target=cls.server.serve, daemon=True)
        cls.server_thread.start()
        # Allow some time for the server to start.
        time.sleep(1)
        cls.client = flight.FlightClient(cls.location)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.server.shutdown()
        cls.server_thread.join(timeout=2)

    def setUp(self):
        # Reset the server configuration for each test.
        self.server.set_data_map(self.initial_data)
        self.server.set_simulate_error(False)

    @classmethod
    def get_server_data(cls) -> dict:
        # By default, create a simple DataFrame and convert it to an Arrow Table.
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        table = pa.Table.from_pandas(df)
        return {b"dummy": table}

    @classmethod
    def get_stream_reader(cls, ticket: bytes) -> flight.FlightStreamReader:
        return cls.client.do_get(flight.Ticket(ticket))
