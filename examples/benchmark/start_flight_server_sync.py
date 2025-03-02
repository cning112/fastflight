import itertools
import time
from typing import Iterator

import pyarrow as pa
import pyarrow.flight
from mock_data_service import TABLE, MockDataParams
from pyarrow import RecordBatchReader


class FlightServerSync(pyarrow.flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def generate_batches(self, rows_per_batch, delay) -> Iterator[pa.RecordBatch]:
        batches = TABLE.to_batches(rows_per_batch)
        for b in batches:
            # simulate an I/O wait time
            time.sleep(delay)
            yield b

    def do_get(self, context, ticket):
        params = MockDataParams.from_bytes(ticket.ticket)
        batches = self.generate_batches(params.rows_per_batch, params.delay_per_row * params.rows_per_batch)
        first = next(batches)
        record_batch_reader = RecordBatchReader.from_batches(first.schema, itertools.chain([first], batches))
        return pyarrow.flight.RecordBatchStream(record_batch_reader)


LOC = "grpc://0.0.0.0:8816"

if __name__ == "__main__":
    server = FlightServerSync(LOC)
    server.serve()
