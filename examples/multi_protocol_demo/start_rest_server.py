#!/usr/bin/env python3
import os
import site
import socket
import sys
from pathlib import Path

# Add examples folder
site.addsitedir(str(Path(__file__).parent.parent))

import uvicorn

from fastflight.fastapi_integration import create_app
from fastflight.utils.custom_logging import setup_logging

setup_logging(log_file=None)


def is_port_available(port: int, host: str = "localhost") -> bool:
    """Check if a port is available for binding"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((host, port))
            return True
    except OSError:
        return False


def find_available_port(preferred_port: int, host: str = "localhost") -> int:
    """Find an available port starting from the preferred port"""
    port = preferred_port
    while port < preferred_port + 100:  # Try up to 100 ports
        if is_port_available(port, host):
            return port
        port += 1
    raise RuntimeError(f"No available port found starting from {preferred_port}")


if __name__ == "__main__":
    # Get ports from environment variables with defaults
    preferred_rest_port = int(os.getenv("REST_PORT", "8000"))
    flight_port = os.getenv("FLIGHT_PORT", "8815")

    # Find an available port for REST server
    try:
        rest_port = find_available_port(preferred_rest_port)
        if rest_port != preferred_rest_port:
            print(f"⚠️  Preferred REST port {preferred_rest_port} was not available, using {rest_port}")
            # Update environment for other processes
            os.environ["REST_PORT"] = str(rest_port)
    except RuntimeError as e:
        print(f"❌ {e}")
        sys.exit(1)

    flight_location = f"grpc://localhost:{flight_port}"

    print(f"Starting REST server at http://localhost:{rest_port}")
    print(f"Connecting to FastFlight server at {flight_location}")

    app = create_app(
        module_paths=["fastflight.demo_services", "multi_protocol_demo.demo_services"],
        route_prefix="/fastflight",
        flight_location=flight_location,
    )

    uvicorn.run(app, host="0.0.0.0", port=rest_port)
