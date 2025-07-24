#!/usr/bin/env python3
import os
import site
import socket
import sys
from pathlib import Path

from fastflight.server import FastFlightServer
from fastflight.utils.custom_logging import setup_logging
from fastflight.utils.registry_check import import_all_modules_in_package

# Set up environment like PyCharm
os.environ.setdefault("PYTHONIOENCODING", "UTF-8")
os.environ.setdefault("PYTHONUNBUFFERED", "1")
print(os.environ)

# Add examples folder
site.addsitedir(str(Path(__file__).parent.parent))

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
    # Load demo services from main package
    import_all_modules_in_package("fastflight.demo_services")
    # Load local demo services (CSV, SQLite)
    import_all_modules_in_package("multi_protocol_demo.demo_services")

    # Get port from environment variable, default to 8815
    preferred_port = int(os.getenv("FLIGHT_PORT", "8815"))

    # Find an available port
    try:
        flight_port = find_available_port(preferred_port)
        if flight_port != preferred_port:
            print(f"⚠️  Preferred port {preferred_port} was not available, using {flight_port}")
            # Update environment for other processes
            os.environ["FLIGHT_PORT"] = str(flight_port)
    except RuntimeError as e:
        print(f"❌ {e}")
        sys.exit(1)

    flight_url = f"grpc://localhost:{flight_port}"

    print(f"Starting FastFlight server at {flight_url}")
    FastFlightServer.start_instance(flight_url)
