# FastFlight
FastAPI + Arrow Flight Server

Introduction

This project integrates a FastAPI server with an embedded Arrow Flight server, offering a dual-protocol solution for handling both HTTP REST and gRPC requests efficiently.

* FastAPI Server: Provides a robust and high-performance HTTP REST service.
* Arrow Flight Server: Embedded within the FastAPI application, it directly handles gRPC requests, enabling fast and scalable data retrieval.
* REST to Flight Integration: A specialized REST endpoint forwards data requests to the Arrow Flight server, streaming the data back to the client seamlessly.

## Arrow Flight Server
The flight server can also run independently, see
[README](src/fastflight/internal/data_service/README.md)

## Better logging
See `src/fastflight/utils/custom_logging.py`

## Development Settings
1. Create a venv
2. `pip install -r requirements.txt`
3. `uvicorn fastflight.main:app --reload --app-dir src`

