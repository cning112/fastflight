# FastFlight

**FastFlight** is a high-performance framework designed for scalable data retrieval and transfer. It leverages **Apache
Arrow's** columnar format, **asynchronous I/O**, and **gRPC** to optimize both data transfer and processing. The
framework allows users to easily create custom data services and define structured, typed requests using **Pydantic**
models.

---

## Features

- **Customizable Data Services**: Easily create custom data services for specific sources through a pluggable
  architecture.
- **Structured Requests**: Define requests as **Pydantic** models, allowing structured and type-safe request handling.
- **Efficient Data Transfer**: Uses **Apache Arrow Flight** to enhance data transfer performance compared to JDBC/ODBC.
- **Asynchronous I/O**: Optimized I/O-bound tasks with asynchronous processing and optional synchronous interfaces.
- **Optional FastAPI Integration**: Expose Flight functionalities via FastAPI for low-latency HTTP access.

---

## Benefits of FastFlight

- **Typed Requests**: Use **Pydantic** models for validated and structured requests, tied to specific data services.
- **High Performance**: Efficient large-scale data handling with Apache Arrow and gRPC streaming.
- **Modular Architecture**: Scale easily with custom data services for various workflows.
- **Flexible Processing**: Asynchronous and synchronous options to fit different workloads.

---

## Core Components

- **Flight Server**: Handles requests and streams data using Apache Arrow.
- **Flight Client**: Fetches data from the server and deserializes it into **Pandas DataFrames** or **PyArrow Tables**.
- **Base Data Service**: Foundation for creating custom data services.
- **Request Models**: Structured requests using **Pydantic**, ensuring validation and flexibility.

---

## Installation

You can install FastFlight using `pip`:

```bash
pip install fastflight
```

---

## Basic Usage

1. Start the Flight server:
   ```bash
   python src/fastflight/flight_server.py
   ```

2. Fetch data with the Flight client:
   ```python
   from fastflight.flight_client import FlightClientManager
   # Connect to Flight server and fetch data
   ```

---

## FastAPI Integration

For more details on FastAPI integration, refer to the [FastAPI Integration Guide](./src/fastflight/fastapi/README.md).

---

## Technical Details

Refer to the [Technical Documentation](./docs/TECHNICAL_DETAILS.md) for in-depth discussions on architecture, typed
requests, asynchronous I/O, and key components.

---

## License

This project is licensed under the MIT License.