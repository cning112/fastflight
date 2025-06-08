import asyncio
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
from pyarrow import flight

from fastflight.utils.stream_utils import write_arrow_data_to_stream
from tests.base_cases import FlightServerTestCase


class TestWriteArrowDataToStream(unittest.TestCase):
    """Test cases for write_arrow_data_to_stream function."""

    def test_write_arrow_data_to_stream(self):
        """Test converting FlightStreamReader to an async generator of bytes."""

        async def test():
            # Create a mock FlightStreamReader
            mock_reader = MagicMock(spec=flight.FlightStreamReader)

            # Set up the mock to return a sequence of chunks and then StopIteration
            chunks = []

            # Create a couple of record batches
            data = [{"id": i, "name": f"name_{i}"} for i in range(3)]
            df = pd.DataFrame(data)
            record_batch = pa.RecordBatch.from_pandas(df)

            # Create mock chunks with the record batch
            for _ in range(2):
                chunk_mock = MagicMock()
                chunk_mock.data = record_batch
                chunks.append(chunk_mock)

            # Configure the mock to return chunks and then raise StopIteration
            mock_reader.read_chunk.side_effect = [*chunks, StopIteration]

            # Test the function
            result = []
            stream = await write_arrow_data_to_stream(mock_reader)
            async for data in stream:
                result.append(data)

            # We should have 2 chunks of data
            self.assertEqual(len(result), 2)
            # Each chunk should be bytes
            for chunk in result:
                self.assertIsInstance(chunk, bytes)

        # Run the async test
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(test())
        finally:
            loop.close()

    @patch("asyncio.to_thread")
    def test_write_arrow_data_error_handling(self, mock_to_thread):
        """Test error handling in write_arrow_data_to_stream."""

        async def test():
            # Set up to_thread to raise an exception
            mock_to_thread.side_effect = ValueError("Test error")

            # Create a mock FlightStreamReader
            mock_reader = MagicMock(spec=flight.FlightStreamReader)

            # Test the function with error
            with self.assertRaises(ValueError):
                stream = await write_arrow_data_to_stream(mock_reader)
                # Consume stream to trigger error
                async for _ in stream:
                    pass

        # Run the async test
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(test())
        finally:
            loop.close()


class TestWriteArrowDataToStreamWithFlightServer(FlightServerTestCase):
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
