from examples.data_services.mock_data import MockDataService
from fastflight.flight_server import start_flight_server

if __name__ == "__main__":
    # Explicitly import for data service registration
    __services__ = [MockDataService]

    loc = "grpc://0.0.0.0:8815"
    start_flight_server(loc)
