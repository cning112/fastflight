from fastflight.flight_client import FlightClientManager

# Technical Details

This document provides in-depth technical explanations of the key components and architecture of the **FastFlight**
project. The focus is on how the system uses **Apache Arrow Flight** for high-performance data transfer and the modular
architecture for handling various data sources.

---

## 1. **Pluggable Data Services**

### Overview:

FastFlight uses a **pluggable data service architecture** that allows for easy integration of different data sources (
SQL databases, data lakes, cloud storage). Each data source is represented by a subclass of `BaseDataService`, and the
corresponding parameters are defined as subclasses of `BaseParams`.

### Key Concepts:

- **`BaseParams`**: Handles serialization and deserialization of query parameters, enabling efficient data transfer
  between the client and the server.
    - **Registration**: Data source types are registered using `BaseParams.register`, allowing the system to dynamically
      load the correct parameter class based on a request.

- **`BaseDataService`**: Defines the core interface for interacting with data sources. This includes an asynchronous
  method `aget_batches`, which fetches data in batches using Arrow's `RecordBatch` format.
    - **Example Service**: The `DemoDataService` shows how to implement a custom service for a specific data source.

### Example Implementation:

The pluggable architecture allows for extending FastFlight easily by creating new subclasses of `BaseParams` and
`BaseDataService` to handle different data sources such as SQL, cloud, or local files.

```python
@BaseParams.register("SQL")
class SQLParams(BaseParams):
    # SQL-specific query parameters
    pass


@BaseDataService.register("SQL")
class SQLDataService(BaseDataService[SQLParams]):
    # Fetch data from a SQL database and return RecordBatches
    pass
```

---

## 2. **Flight Server Architecture**

### Overview:

The **Flight server** in FastFlight handles user requests, dispatches calls to the appropriate data service, and returns
data to the client using **Arrow Flight** via **gRPC**.

### Key Components:

- **`FlightServer`**: The server processes client requests, extracts parameters from the `Ticket` object, and calls the
  appropriate data service to retrieve data.
    - **`do_get` Method**: This is where the server retrieves the data from a data service and streams the results back
      to the client using a `RecordBatchReader`.

- **Asynchronous Data Fetching**: The `FlightServer` leverages the `AsyncToSyncConverter` to convert asynchronous data
  fetching into synchronous data streaming for Flight's gRPC interface.

### Example Workflow:

1. Client sends a request with a **Ticket**.
2. `FlightServer.do_get` method extracts the parameters from the ticket.
3. The server dispatches the call to the registered data service, which fetches data asynchronously in batches.
4. Data is streamed back to the client using Arrow's **RecordBatchStream**.

---

## 3. **Flight Client Design**

### Overview:

The **Flight client** is designed to interact with the Flight server, fetch data, and deserialize it into user-friendly
formats like **Pandas DataFrames** or **PyArrow Tables**.

### Key Components:

- **`FlightClientManager`**: Manages a pool of Flight clients for concurrent requests. This ensures efficient resource
  usage
  and allows multiple requests to be handled concurrently.

- **Data Fetching Methods**:
    - **`aget_stream_reader`**: Asynchronously fetches data from the server and returns a **FlightStreamReader**.
    - **`aread_pa_table`**: Asynchronously fetches data and converts it to a **PyArrow Table**.
    - **`aread_pd_df`**: Fetches data from the server and converts it into a **Pandas DataFrame**.

### Example:

```python
from fastflight.flight_client import FlightClientManager

client = FlightClientManager("grpc://localhost:8815")
ticket = b"<ticket bytes>"
data_frame = client.read_pd_df(ticket)
```

The client can work in both synchronous and asynchronous environments, allowing for flexible usage in different
application contexts.

---

## 4. **Utility Functions**

### `AsyncToSyncConverter`

The `AsyncToSyncConverter` class converts asynchronous iterables into synchronous ones by managing an **asyncio event
loop**. This is useful when integrating asynchronous data fetching (used by `BaseDataService`) with Arrow Flight's
synchronous gRPC interface.

#### Key Methods:

- **`syncify_async_iter`**: Converts an async iterable into a synchronous iterator.
- **`run_coroutine`**: Submits a coroutine to the event loop and retrieves the result synchronously.

### `stream_arrow_data`

Streams **Arrow IPC** data from a `FlightStreamReader` into an asynchronous byte generator. This method enables
efficient, large-scale data transfer between the Flight server and the client.

---

## 5. **FastAPI Integration (Optional)**

### Overview:

The FastAPI integration is an optional feature of FastFlight that allows exposing the data services via an HTTP API. It
utilizes FastAPI’s asynchronous capabilities to handle requests efficiently.

### Components:

- **`api_router.py`**: Defines FastAPI routes that handle client requests, forward them to the Flight server, and stream
  the resulting data back.
- **`lifespan.py`**: Manages the lifecycle of Flight clients within the FastAPI application.

For more details, see the separate [FastAPI README](../src/fastflight/fastapi/README.md).

---

## 6. **Performance Benefits of Arrow Flight**

Using **Apache Arrow Flight** significantly enhances the performance of data transfer compared to traditional methods
like JDBC/ODBC:

- **Columnar Data Format**: Arrow’s columnar format is optimized for in-memory analytics, reducing serialization
  overhead and improving I/O performance.
- **gRPC Streaming**: Flight leverages gRPC for efficient network communication, providing lower latency and higher
  throughput for large datasets.
- **Zero-Copy Data Transfer**: Arrow Flight minimizes data copying between processes, further improving performance,
  especially in distributed systems.

---

## Conclusion

FastFlight is a flexible and high-performance framework designed for efficient data transfer using Arrow Flight. The
modular design allows it to adapt to various data sources, making it an ideal solution for projects that need to handle
large-scale data retrieval and transfer efficiently.