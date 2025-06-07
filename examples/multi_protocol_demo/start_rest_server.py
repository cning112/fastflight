#!/usr/bin/env python3
"""
Start REST server for demo
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import uvicorn

from fastflight.fastapi_integration import create_app

if __name__ == "__main__":
    print("Starting REST server at http://localhost:8000")

    app = create_app(
        module_paths=["multi_protocol_demo.demo_services"],
        route_prefix="/fastflight",
        flight_location="grpc://localhost:8815",
    )

    uvicorn.run(app, host="0.0.0.0", port=8000)
