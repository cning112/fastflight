import asyncio
import unittest

from fastflight.utils.stream_utils import read_record_batches_from_stream


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
