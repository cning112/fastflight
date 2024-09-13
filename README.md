# FastFlight

FastAPI + Arrow Flight Server

Introduction

This project integrates a FastAPI server with an embedded Arrow Flight server, offering a dual-protocol solution for
handling both HTTP REST and gRPC requests efficiently.

* FastAPI Server: Provides a robust and high-performance HTTP REST service.
* Arrow Flight Server: Embedded within the FastAPI application, it directly handles gRPC requests, enabling fast and
  scalable data retrieval.
* REST to Flight Integration: A specialized REST endpoint forwards data requests to the Arrow Flight server, streaming
  the data back to the client seamlessly.

## How does it work?

### With FastAPI

1. Run the flight server and the FastAPI server in separate processes
2. Test by posting query params to the /fastflight/ endpoint

### Without FastAPI

1. RUn the flight server
2. Test by posting flight ticket to the flight server endpoint.
3. Or, use the flight_client to help with the data retrieval.

## How to add a new data source type?

1. Add a new data source type to the enum in `models/data_source`
2. Add a new params class in `models/params`. Make sure the new params class is registered with the new data source type
3. Add a new data service to handle the new params

## Better logging

See `src/fastflight/utils/custom_logging.py`

## Development Settings

1. Create a venv
2. `pip install -r requirements.txt`
3. `uvicorn fastflight.main:app --reload --app-dir src`
