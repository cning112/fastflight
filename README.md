# FastFlight

**FastFlight** is a high-performance framework for fetching and transferring data from multiple data sources using *
*Apache Arrow Flight**. It focuses on improving data transfer efficiency, particularly in scenarios requiring
large-scale data retrieval, by using Arrow's columnar data format and gRPC for efficient streaming.

---

## Features

- **Pluggable Data Services**: Easily add support for different data sources (SQL, cloud, data lakes) through a modular
  service architecture.
- **Efficient Data Transfer**: By using **Arrow Flight**, FastFlight reduces the overhead of traditional data transfer
  methods (JDBC/ODBC), improving performance across networks.
- **Asynchronous and Synchronous Support**: The project supports both asynchronous data processing and synchronous
  interfaces, making it flexible for various use cases.
- **FastAPI Integration (Optional)**: Serve data via HTTP APIs with minimal latency using FastAPI, allowing you to
  expose your Flight server functionalities through a web interface.

---

## Benefits of Using Arrow Flight

- **High Performance**: Arrow Flight leverages the **columnar format** of Apache Arrow, which allows for more efficient
  data transport, especially in large datasets.
- **Low Latency**: The use of **gRPC streaming** ensures efficient network communication, reducing round-trip time and
  enabling real-time data transfer.
- **Pluggable Architecture**: Easy to integrate new data sources by extending base classes for params and data services.

---

## Core Components

- **Flight Server**: Manages incoming requests, dispatches to the appropriate data service, and streams results using
  Apache Arrow.
- **Flight Client**: Fetches data from the Flight server and deserializes it into user-friendly formats like **Pandas
  DataFrames** or **PyArrow Tables**.
- **Data Services**: Modular system for retrieving data from various data sources with support for asynchronous data
  streaming.

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/cning112/fastflight
   cd fastflight
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## Basic Usage

1. Start the Flight server:
   ```bash
   python src/fastflight/flight_server.py
   ```
2. Use the Flight client to fetch data:
   ```python
   from fastflight.flight_client import FlightClientManager
   # Connect to Flight server and fetch data
   ```

---

## FastAPI Integration

For more information on how to integrate with FastAPI, refer to
the [FastAPI Integration Guide](./src/fastflight/fastapi/README.md).

---

## Technical Details

For in-depth technical discussions on the architecture, data service design, and utility functions (e.g.,
`AsyncToSyncConverter`, streaming utils), refer to the [Technical Documentation](./docs/TECHNICAL_DETAILS.md).

---

## License

This project is licensed under the MIT License.