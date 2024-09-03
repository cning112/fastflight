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

Assuming the flight server is running, a user can create a client helper and use it to get data from various data
sources.
See the example in `client.client_helpers.py`

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
