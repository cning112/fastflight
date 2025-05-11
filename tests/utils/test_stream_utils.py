import asyncio
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
from pyarrow import flight

from fastflight.utils.stream_utils import (
    AsyncToSyncConverter,
    IterableBytesIO,
    read_dataframe_from_arrow_stream,
    read_record_batches_from_stream,
    read_table_from_arrow_stream,
    write_arrow_data_to_stream,
)


class TestAsyncToSyncConverter(unittest.TestCase):
    """Test cases for AsyncToSyncConverter class."""

    def setUp(self):
        """Set up test environment before each test."""
        self.converter = AsyncToSyncConverter()

    def tearDown(self):
        """Clean up after each test."""
        self.converter.close()
        # self.loop.close()

    def test_init_without_loop(self):
        """Test initialization without an event loop."""
        converter = AsyncToSyncConverter()
        self.assertIsNotNone(converter.loop)
        self.assertIsNotNone(converter.loop_thread)
        converter.close()

    def test_run_coroutine(self):
        """Test running a coroutine with run_coroutine."""

        async def test_coro():
            return "test_result"

        result = self.converter.run_coroutine(test_coro())
        self.assertEqual(result, "test_result")

    def test_syncify_async_iter_basic(self):
        """Test basic functionality of syncify_async_iter."""

        async def test_gen():
            for i in range(5):
                yield i

        result = list(self.converter.syncify_async_iter(test_gen()))
        self.assertEqual(result, [0, 1, 2, 3, 4])

    def test_syncify_async_iter_with_exception(self):
        """Test syncify_async_iter with an exception in the async iterator."""

        async def test_gen_with_error():
            yield 0
            yield 1
            raise ValueError("Test error")
            yield 2  # This won't be reached

        with self.assertRaises(ValueError):
            list(self.converter.syncify_async_iter(test_gen_with_error()))

    def test_syncify_async_iter_empty(self):
        """Test syncify_async_iter with an empty iterator."""

        async def empty_gen():
            if False:  # This condition is never met
                yield 1

        result = list(self.converter.syncify_async_iter(empty_gen()))
        self.assertEqual(result, [])

    def test_context_manager(self):
        """Test using the converter as a context manager."""
        with AsyncToSyncConverter() as converter:

            async def test_coro():
                return "test_context_manager"

            result = converter.run_coroutine(test_coro())
            self.assertEqual(result, "test_context_manager")

    def test_awaitable_returning_async_iterable(self):
        """Test syncify_async_iter with an awaitable that returns an async iterable."""

        async def get_async_iterable():
            async def inner_gen():
                for i in range(3):
                    yield i

            return inner_gen()

        result = list(self.converter.syncify_async_iter(get_async_iterable()))
        self.assertEqual(result, [0, 1, 2])


class TestReadRecordBatchesFromStream(unittest.TestCase):
    """Test cases for read_record_batches_from_stream function."""

    def test_read_record_batches(self):
        """Test reading record batches from an async stream."""

        async def test():
            # Create test data
            test_data = [{"col1": i, "col2": f"value_{i}"} for i in range(5)]

            # Create an async generator
            async def gen():
                for item in test_data:
                    yield item

            # Run the function under test
            batches = []
            async for batch in read_record_batches_from_stream(gen(), batch_size=2):
                batches.append(batch)

            # Verify results
            self.assertEqual(len(batches), 3)

            # First batch should have 2 rows
            self.assertEqual(batches[0].num_rows, 2)
            # Second batch should have 2 rows
            self.assertEqual(batches[1].num_rows, 2)
            # Third batch should have 1 row (remainder)
            self.assertEqual(batches[2].num_rows, 1)

            # Verify schema
            expected_fields = ["col1", "col2"]
            for batch in batches:
                self.assertEqual([field.name for field in batch.schema], expected_fields)

        # Run the async test
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(test())
        finally:
            loop.close()

    def test_read_record_batches_empty_stream(self):
        """Test reading record batches from an empty async stream."""

        async def test():
            # Create an empty async generator
            async def gen():
                if False:  # This condition is never met
                    yield {"col1": 1, "col2": "value"}

            # Run the function under test
            batches = []
            async for batch in read_record_batches_from_stream(gen()):
                batches.append(batch)

            # Verify results
            self.assertEqual(len(batches), 0)

        # Run the async test
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(test())
        finally:
            loop.close()


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
            mock_reader.read_chunk.side_effect = chunks + [StopIteration]

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


class TestIterableBytesIO(unittest.TestCase):
    """Test cases for IterableBytesIO class."""

    def test_read_all(self):
        """Test reading all data from IterableBytesIO."""
        data = [b"chunk1", b"chunk2", b"chunk3"]
        stream = IterableBytesIO(data)

        # Read all data
        result = stream.read()
        expected = b"chunk1chunk2chunk3"
        self.assertEqual(result, expected)

    def test_read_with_size(self):
        """Test reading specific size chunks from IterableBytesIO."""
        data = [b"chunk1", b"chunk2", b"chunk3"]
        stream = IterableBytesIO(data)

        # Read 5 bytes
        result1 = stream.read(5)
        self.assertEqual(result1, b"chunk")

        # Read 5 more bytes
        result2 = stream.read(5)
        self.assertEqual(result2, b"1chun")

        # Read remaining bytes
        result3 = stream.read()
        self.assertEqual(result3, b"k2chunk3")

    def test_readable(self):
        """Test readable method of IterableBytesIO."""
        stream = IterableBytesIO([])
        self.assertTrue(stream.readable())


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
