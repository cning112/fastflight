import unittest

import pandas as pd
import pyarrow as pa

from fastflight.utils.stream_utils import read_dataframe_from_arrow_stream, read_table_from_arrow_stream


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


if __name__ == "__main__":
    unittest.main()
