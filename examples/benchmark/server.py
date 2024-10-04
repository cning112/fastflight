from fastflight.flight_server import start_flight_server

from .mock_data_service import MockDataService

if __name__ == "__main__":
    # Explicitly import for data service registration
    __services__ = [MockDataService]

    loc = "grpc://0.0.0.0:8815"
    start_flight_server(loc)
