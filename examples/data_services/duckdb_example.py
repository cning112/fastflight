import tempfile

import pandas as pd

from fastflight.data_services.duckdb_service import DuckDBParams
from fastflight.flight_client import FlightClientManager

LOC = "grpc://0.0.0.0:8815"

if __name__ == "__main__":
    with FlightClientManager(LOC) as client:
        with tempfile.TemporaryDirectory() as tmpdir:
            pd.DataFrame({"column1": [1, 2, 3, 4, 5]}).to_csv(f"{tmpdir}/data.csv", index=False)

            duck_params = DuckDBParams(
                database_path=":memory:",
                query=f"SELECT * FROM read_csv_auto('{tmpdir}/data.csv') WHERE column1 > ?",
                parameters=[2],
            )

            df = client.read_pd_dataframe(duck_params)
            print(df)
            # output:
            #   column1
            # 0       3
            # 1       4
            # 2       5
