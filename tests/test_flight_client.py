import unittest

import pyarrow as pa

from fastflight import FastFlightError
from fastflight.client import FastFlightBouncer
from tests.base_cases import FlightServerTestCase


# We assume that FlightServerTestCase is defined as in the previous refactoring,
# which starts a Flight server before all tests and shuts it down afterward.
class TestFlightClient(FlightServerTestCase):
    async def test_aget_stream(self):
        """
        Test the aget_stream method to ensure it returns an AsyncIterable[bytes] that produces
        valid Arrow IPC data, which can be parsed back into an Arrow table matching the server data.
        """
        # Create a FastFlightBouncer instance using the server's location.
        async with FastFlightBouncer(self.location) as manager:
            # Use a dummy ticket corresponding to the default data in the server.
            ticket = b"dummy"
            # Call aget_stream and await to get the async generator.
            stream = manager.aget_stream(ticket)
            result_chunks = []
            # Iterate over the async generator.
            async for chunk in stream:
                result_chunks.append(chunk)
            # Verify that at least one data chunk is produced.
            self.assertGreater(len(result_chunks), 0)
            # Parse the first chunk as an IPC stream and verify it equals the expected table.
            ipc_reader = pa.ipc.open_stream(pa.BufferReader(result_chunks[0]))
            received_table = ipc_reader.read_all()
            self.assertTrue(received_table.equals(self.get_server_data()[b"dummy"]))

    async def test_connection_returned_on_failure(self):
        bouncer = FastFlightBouncer(self.location, client_pool_size=1)

        async def fail_callback(reader):
            raise RuntimeError("Simulated failure")

        with self.assertRaises(FastFlightError):
            await bouncer.aget_stream_reader_with_callback(b"dummy", fail_callback)

        assert bouncer._connection_pool.queue.qsize() == 1


if __name__ == "__main__":
    unittest.main()
