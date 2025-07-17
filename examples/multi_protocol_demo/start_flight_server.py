#!/usr/bin/env python3
"""
Start FastFlight server for demo
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastflight.server import FastFlightServer
from fastflight.utils.custom_logging import setup_logging
from fastflight.utils.registry_check import import_all_modules_in_package

setup_logging(log_file=None)

if __name__ == "__main__":
    # Load demo services
    import_all_modules_in_package("multi_protocol_demo.demo_services")

    print("Starting FastFlight server at grpc://localhost:8815")
    FastFlightServer.start_instance("grpc://localhost:8815")
