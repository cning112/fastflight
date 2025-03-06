import asyncio

from mock_data_service import MockDataParams

from fastflight.client import FastFlightClient

if __name__ == "__main__":
    data_params = MockDataParams(rows_per_batch=5_000, delay_per_row=1e-6)

    LOC = "grpc://localhost:8815"
    client = FastFlightClient(LOC)

    # get data in an async way
    async def main():
        reader = await client.aget_stream_reader(data_params)
        for i, batch in enumerate(reader):
            print(f"Async: read batch {i} from grpc\n", batch.data)

    asyncio.run(main())

    # get data in a sync way
    df = client.get_pd_dataframe(data_params)
    print("Sync: read dataframe from grpc\n", df)
