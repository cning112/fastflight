from mock_data_service import MockDataService

from fastflight.flight_server import start_flight_server

LOC = "grpc://0.0.0.0:8815"

if __name__ == "__main__":
    # Explicitly import for data service registration
    __services__ = [MockDataService]

    start_flight_server(LOC)
