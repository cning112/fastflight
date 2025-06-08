import argparse

from fastflight.server import FastFlightServer
from fastflight.utils.custom_logging import setup_logging

DEFAULT_LOC = "grpc://127.0.0.1:8815"

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Start a FastFlight server")
    parser.add_argument(
        "--server", type=str, default=DEFAULT_LOC, help=f"GRPC server location (default: {DEFAULT_LOC})"
    )
    args = parser.parse_args()

    setup_logging(log_file=None, console_log_level="INFO")

    # Import mock data services to register them
    from mock_data_service import MockDataService, MockDataServiceAsync, prepare_static_data

    services = [MockDataService, MockDataServiceAsync]
    prepare_static_data()

    FastFlightServer.start_instance(args.server)
