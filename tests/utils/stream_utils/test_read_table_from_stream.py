import asyncio
import unittest
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa

from fastflight.utils.stream_utils import (
    read_dataframe_from_arrow_stream,
    read_table_from_arrow_stream,
    write_arrow_data_to_stream,
)


class TestTableFunctions(unittest.TestCase):
    """Test cases for Arrow table reading functions."""

    def test_read_table_from_arrow_stream(self):
        """Test reading a table from an iterable of bytes."""
        # Create Arrow table data
        data = pd.DataFrame({"id": [1, 2, 3], "name": ["one", "two", "three"]})
        table = pa.Table.from_pandas(data)

        # Write table to IPC format
        sink = pa.BufferOutputStream()
        writer = pa.ipc.RecordBatchStreamWriter(sink, table.schema)
        writer.write_table(table)
        writer.close()
        buf = sink.getvalue()

        # Split buffer into chunks
        chunks = [buf.to_pybytes()[i : i + 10] for i in range(0, len(buf.to_pybytes()), 10)]

        # Test read_table_from_arrow_stream
        result_table = read_table_from_arrow_stream(chunks)

        # Verify result
        self.assertEqual(result_table.num_rows, 3)
        self.assertEqual(result_table.num_columns, 2)
        self.assertEqual(result_table.column_names, ["id", "name"])

    def test_read_dataframe_from_arrow_stream(self):
        """Test reading a DataFrame from an iterable of bytes."""
        # Create Arrow table data
        data = pd.DataFrame({"id": [1, 2, 3], "name": ["one", "two", "three"]})
        table = pa.Table.from_pandas(data)

        # Write table to IPC format
        sink = pa.BufferOutputStream()
        writer = pa.ipc.RecordBatchStreamWriter(sink, table.schema)
        writer.write_table(table)
        writer.close()
        buf = sink.getvalue()

        # Split buffer into chunks
        chunks = [buf.to_pybytes()[i : i + 10] for i in range(0, len(buf.to_pybytes()), 10)]

        # Test read_dataframe_from_arrow_stream
        result_df = read_dataframe_from_arrow_stream(chunks)

        # Verify result
        self.assertEqual(len(result_df), 3)
        self.assertEqual(list(result_df.columns), ["id", "name"])
        pd.testing.assert_frame_equal(result_df, data)

    def test_write_arrow_stream_multiple_batches(self):
        """Ensure write_arrow_data_to_stream produces a continuous Arrow stream."""
        batch1 = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], ["value"])
        batch2 = pa.RecordBatch.from_arrays([pa.array([4, 5, 6])], ["value"])

        class StubReader:
            def __init__(self, batches: list[pa.RecordBatch]):
                self._batches = batches
                self._index = 0
                self.schema = batches[0].schema

            def read_chunk(self):
                if self._index >= len(self._batches):
                    raise StopIteration
                batch = self._batches[self._index]
                self._index += 1
                return SimpleNamespace(data=batch, app_metadata=None)

        async def collect_chunks():
            reader = StubReader([batch1, batch2])
            stream = await write_arrow_data_to_stream(reader)
            return [chunk async for chunk in stream]

        chunks = asyncio.run(collect_chunks())
        result_df = read_dataframe_from_arrow_stream(chunks)

        self.assertEqual(len(result_df), 6)
        pd.testing.assert_series_equal(
            result_df["value"], pd.Series([1, 2, 3, 4, 5, 6], name="value"), check_names=True
        )


if __name__ == "__main__":
    unittest.main()
