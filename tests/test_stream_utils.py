import unittest

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from fastflight.utils.stream_utils import write_arrow_data_to_stream

from .utils import FlightServerTestCase


# Test case class that inherits from the shared FlightServerTestCase.
class TestWriteArrowDataToStream(FlightServerTestCase):
    async def test_write_arrow_data_to_stream_default(self):
        reader = self.get_stream_reader(b"dummy")
        stream = await write_arrow_data_to_stream(reader)
        result_data = []
        async for data in stream:
            result_data.append(data)

        self.assertGreater(len(result_data), 0)

        # Parse the first IPC bytes and verify the table equals the expected default table.
        ipc_reader = pa.ipc.open_stream(pa.BufferReader(result_data[0]))
        received_table = ipc_reader.read_all()
        self.assertTrue(received_table.equals(self.initial_data[b"dummy"]))

    async def test_write_arrow_data_to_stream_custom_data(self):
        """
        Test write_arrow_data_to_stream with custom server data for a specific ticket.
        """
        # Create custom data.
        df = pd.DataFrame({"col1": [10, 20, 30], "col2": ["x", "y", "z"]})
        custom_table = pa.Table.from_pandas(df)
        # Update the server data mapping to use a new ticket.
        self.server.set_data_map({b"custom": custom_table})

        reader = self.get_stream_reader(b"custom")
        stream = await write_arrow_data_to_stream(reader)
        result_data = []
        async for data in stream:
            result_data.append(data)

        self.assertGreater(len(result_data), 0)

        ipc_reader = pa.ipc.open_stream(pa.BufferReader(result_data[0]))
        received_table = ipc_reader.read_all()
        self.assertTrue(received_table.equals(custom_table))

    async def test_write_arrow_data_to_stream_simulated_error(self):
        """
        Test write_arrow_data_to_stream when the server is set to simulate an error.
        """
        self.server.set_simulate_error(True)
        client = flight.FlightClient(self.location)
        ticket = flight.Ticket(b"dummy")
        # Expect that calling do_get will raise an error.
        with self.assertRaises(flight.FlightServerError):
            _ = client.do_get(ticket)


if __name__ == "__main__":
    unittest.main()
