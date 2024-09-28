# Technical Details

This document provides in-depth technical explanations of the key components and architecture of the **FastFlight**
project. It focuses on how the system uses **Apache Arrow Flight** for high-performance data transfer and its modular
design for integrating various data sources.

---

## 1. **Pluggable Data Services**

### Overview:

FastFlight employs a **pluggable data service architecture** that allows users to easily integrate various data
sources (e.g., SQL databases, data lakes, cloud storage). Each data source is represented by a subclass of
`BaseDataService`, while parameters are defined through subclasses of `BaseParams`.

### Key Concepts:

- **`BaseParams`**: Manages serialization and deserialization of query parameters, enabling efficient data transfer
  between the client and server.
    - **Registration**: Data source types are registered using `BaseParams.register`, allowing the system to dynamically
      load the correct parameter class based on the request.

- **`BaseDataService`**: Defines the interface for interacting with data sources, including the asynchronous
  `aget_batches` method, which fetches data in Arrow's `RecordBatch` format.
    - **Custom Services**: Users can easily implement their own data services for specific data sources by subclassing
      `BaseDataService` and `BaseParams`.

### Example Implementation:

The pluggable architecture allows for easy extension by creating new subclasses for specific data sources, such as SQL,
cloud, or file systems.

```python
@BaseParams.register("SQL")
class SQLParams(BaseParams):
    query: str  # SQL-specific query parameter
    limit: int


@BaseDataService.register("SQL")
class SQLDataService(BaseDataService[SQLParams]):
    async def aget_batches(self, params: SQLParams, batch_size: int = 100):
        # Fetch data from a SQL database and return RecordBatches
        pass
```

---

## 2. **Typed Requests with Pydantic**

### Overview:

FastFlight uses **Pydantic** models to define structured, typed requests, ensuring validated and type-safe data queries.
Each request type is associated with a specific data service implementation, ensuring that appropriate parameters are
passed to the relevant service.

### Key Concepts:

- **Request Validation**: Each data service has an associated request type, defined as a Pydantic model. These request
  types are validated and serialized for efficient data transfer.
- **Custom Request Models**: Developers can define request models based on their data service requirements, providing
  flexibility and control over the request/response flow.

### Example:

```python
@BaseParams.register("SQL")
class SQLParams(BaseParams):
    query: str
    limit: int


@BaseDataService.register("SQL")
class SQLDataService(BaseDataService[SQLParams]):
    async def aget_batches(self, params: SQLParams, batch_size: int = 100):
        # Fetch and return data in RecordBatch format
        pass
```

---

## 3. **Flight Server Architecture**

### Overview:

The **Flight Server** manages incoming requests, invokes the appropriate data service, and returns data to the client
via **Arrow Flight** using **gRPC**.

### Key Components:

- **`FlightServer`**: Processes client requests, extracts parameters from `Ticket` objects, and uses registered data
  services to retrieve data asynchronously.
    - **`do_get`**: The core method that dispatches requests to data services and streams the results back to the client
      using `RecordBatchReader`.

- **Asynchronous Data Fetching**: Data is fetched asynchronously using the `AsyncToSyncConverter`, which converts async
  operations into synchronous ones for compatibility with gRPC.

### Example Workflow:

1. Client sends a request with a **Ticket**.
2. `FlightServer.do_get` extracts the request parameters.
3. The server dispatches the request to the appropriate data service, which retrieves data asynchronously in batches.
4. Data is streamed back to the client using Arrow's **RecordBatchStream**.

---

## 4. **Flight Client Design**

### Overview:

The **Flight client** interacts with the Flight server, fetching data and converting it into formats like **Pandas
DataFrames** or **PyArrow Tables**.

### Key Components:

- **`FlightClientManager`**: Manages a pool of Flight clients to handle multiple concurrent requests efficiently.
- **Data Fetching Methods**:
    - **`aget_stream_reader`**: Fetches data asynchronously and returns a **FlightStreamReader**.
    - **`aread_pa_table`**: Fetches and converts data into a **PyArrow Table**.
    - **`aread_pd_df`**: Fetches and converts data into a **Pandas DataFrame**.

### Example:

```python
from fastflight.flight_client import FlightClientManager

client = FlightClientManager("grpc://localhost:8815")
ticket = b"<ticket bytes>"
data_frame = client.read_pd_df(ticket)
```

---

## 5. **Utility Functions**

### `AsyncToSyncConverter`

This utility class converts asynchronous iterators into synchronous ones, managing an **asyncio event loop**. It ensures
compatibility between async data fetching (used by `BaseDataService`) and Arrow Flight’s synchronous gRPC interface.

#### Key Methods:

- **`syncify_async_iter`**: Converts an asynchronous iterable into a synchronous iterator.
- **`run_coroutine`**: Submits a coroutine to the event loop and retrieves the result synchronously.

### `stream_arrow_data`

This function streams **Arrow IPC** data from a `FlightStreamReader` into an asynchronous byte generator. It enables
efficient, large-scale data transfer between the Flight server and the client.

---

## 6. **FastAPI Integration (Optional)**

### Overview:

FastFlight offers optional **FastAPI integration**, which exposes data services via HTTP APIs. FastAPI’s asynchronous
capabilities ensure efficient handling of requests, making it an ideal interface for web-based access.

### Components:

- **`api_router.py`**: Defines FastAPI routes to handle client requests, forward them to the Flight server, and stream
  the resulting data back.
- **`lifespan.py`**: Manages the lifecycle of Flight clients within a FastAPI application.

For more details, see the separate [FastAPI README](../src/fastflight/fastapi/README.md).

---

## 7. **Performance Benefits of Arrow Flight**

Using **Apache Arrow Flight** provides significant performance improvements over traditional data transfer methods like
JDBC/ODBC:

- **Columnar Data Format**: Apache Arrow’s columnar format is optimized for in-memory analytics, reducing serialization
  overhead and improving I/O performance.
- **gRPC Streaming**: Arrow Flight uses gRPC for efficient network communication, providing low-latency, high-throughput
  data transfer.
- **Zero-Copy Data Transfer**: Arrow Flight minimizes data copying between processes, improving performance in
  distributed systems.

---

## Conclusion

FastFlight is a flexible and high-performance framework designed for efficient data transfer using Arrow Flight. Its
modular design allows it to integrate various data sources easily, making it ideal for projects that require large-scale
data retrieval and transfer.