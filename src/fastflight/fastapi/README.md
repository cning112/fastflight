# FastFlight FastAPI Integration

This module demonstrates how to expose **FastFlight's Arrow Flight** capabilities via **FastAPI**. The FastAPI layer
allows clients to interact with the Arrow Flight server over HTTP, enabling efficient data retrieval in Arrow format
through API calls.

## Key Features

- **HTTP API Interface**: Use FastAPI to provide HTTP-based endpoints for Arrow Flight queries.
- **Streaming Support**: Stream Arrow data from the Flight server to clients via HTTP.
- **Lifespan Management**: Properly manage the lifecycle of the Flight client within FastAPI.

## API Endpoints

- **POST /**:
    - Streams Arrow Flight data based on the request payload.
    - Example request:
      ```bash
      curl -X POST http://localhost:8000/fastflight/ -d '<ticket_bytes>'
      ```

## Usage

### Starting the FastAPI Server:

1. Ensure that the **Flight server** is running.
2. Start the FastAPI server:
   ```bash
   uvicorn demo.main:app --reload
   ```

### Lifespan Management

FastFlight uses `lifespan.py` to manage the lifecycle of the Flight client, ensuring that the Flight client is properly
initialized and closed with the FastAPI application.