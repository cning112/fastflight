import pyarrow.flight
from mock_data_service import TABLE, MockDataParams
from pyarrow import RecordBatchReader


class FlightServerSync(pyarrow.flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_get(self, context, ticket):
        batch_size = MockDataParams.from_bytes(ticket.ticket).batch_size
        batches = TABLE.to_batches(batch_size)
        record_batch_reader = RecordBatchReader.from_batches(batches[0].schema, batches)
        return pyarrow.flight.RecordBatchStream(record_batch_reader)


if __name__ == "__main__":
    location = "grpc://0.0.0.0:8816"
    server = FlightServerSync(location)
    server.serve()
