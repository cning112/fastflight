import unittest

from fastflight.utils.stream_utils import IterableBytesIO


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
