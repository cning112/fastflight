# **FastFlight Technical Documentation**

## **📌 Overview**

FastFlight is an extension of **Apache Arrow Flight**, designed to **simplify high-performance data transfers** while
providing enhanced **usability, integration, and developer-friendly features**.

This document provides a detailed introduction to FastFlight’s **core architecture, data flow, performance optimization
strategies, deployment recommendations, and extension methods**, helping developers gain a deeper understanding of its
design principles.

---

## **🚀 FastFlight Architecture**

FastFlight consists of the following core components:

1. **Flight Server**: Built on Apache Arrow Flight, providing high-throughput, low-latency data transfer services.
2. **Ticket Mechanism**: Uses parameterized Ticket (`param_type` mechanism) to support structured data requests and
   improve
   readability.
3. **Asynchronous Streaming**: Based on Python `asyncio`, enabling efficient `async for` data stream consumption.
4. **FastAPI Adapter Layer**: Provides REST API compatibility, supporting HTTP client access.
5. **CLI Tool**: Enables quick startup and management of Flight Server instances.

### **🔹 FastFlight Architecture Diagram**

```text
+----------------------------+
|       Client Application   |
|   FastFlightBouncer        |
|   - Connection Pooling     |
|   - Retry & Circuit Breaker|
|   - Error Handling         |
+----------------------------+
           ▲
           |  gRPC (pooled)
           ▼
+----------------------------+
|      FastFlight Server     |
| - Handles Flight Requests  |
| - Uses Parameterized Ticket|
| - Service Registration     |
| - Streams Data Efficiently |
+----------------------------+
           ▲
           | Data fetching
           ▼
+----------------------------+
|    Data Services Layer     |
|  - DuckDB Service          |
|  - Custom Data Services    |
|  - BaseDataService API     |
+----------------------------+
           ▲
           | JDBC/ODBC/API
           ▼
+----------------------------+
|      Data Sources          |
|  (SQL, NoSQL, Files, etc.) |
+----------------------------+
```

---

## **🔀 Data Flow Design**

FastFlight adopts a **structured Ticket mechanism**, avoiding the opacity of native Arrow Flight, which only supports
byte (`bytes`) transmission. The data flow is as follows:

> **Note:** To enable the parameter-to-service bindings, you must import the modules that register these bindings. This
> import is required for registration purposes, not for discovering parameter classes.

1️⃣ **Client sends a parameterized Ticket request**

```python
from fastflight import FastFlightBouncer
from fastflight.demo_services.duckdb_demo import DuckDBParams

# Client sends a parameterized Ticket request
params = DuckDBParams(
    database_path="example.duckdb",
    query="select * from financial_data where date >= ? and date <= ?",
    parameters=["2024-01-01T00:00:00Z", "2024-01-31T00:00:00Z"]
)

# The params serialize to JSON with param_type for service routing
params.to_json()
{
    "database_path": "example.duckdb",
    "query": "select * from financial_data where date >= ? and date <= ?",
    "parameters": ["2024-01-01T00:00:00Z", "2024-01-31T00:00:00Z"],
    "param_type": "fastflight.demo_services.duckdb_demo.DuckDBParams"
}

# Use FastFlightBouncer for connection pooling and resilience
bouncer = FastFlightBouncer("grpc://localhost:8815")
table = bouncer.get_pa_table(params)  # Synchronous
# or
table = await bouncer.aget_pa_table(params)  # Asynchronous
```

2️⃣ **Flight Server uses the `param_type` field to identify the ticket type and match the appropriate data service to
process
the request**

3️⃣ **Flight Server use the data service to get data from the data source (e.g., SQL DB) and converts the data into
Apache Arrow format**

4️⃣ **Data is streamed back using Arrow Flight gRPC**

5️⃣ **Client consumes the stream data via either synchronous or asynchronous methods**

---

## **⚡ Performance Optimization Analysis**

FastFlight offers significant performance advantages over traditional REST API / JDBC / ODBC approaches:

| Method                        | Protocol | Data Format           | Supports Streaming? | Use Case               |
|-------------------------------|----------|-----------------------|---------------------|------------------------|
| **REST API**                  | HTTP     | JSON / CSV            | ❌ No                | Lightweight API calls  |
| **JDBC / ODBC**               | TCP      | Row-Based Data        | ❌ No                | Database queries       |
| **Arrow Flight (FastFlight)** | gRPC     | Columnar Data (Arrow) | ✅ Yes               | Large-scale data flows |

FastFlight leverages **columnar storage format (Apache Arrow)**, avoiding the overhead of traditional JSON/CSV parsing,
and supports **zero-copy transmission**, significantly improving data throughput.

---

## **🏗️ Best Deployment Strategies**

To fully utilize FastFlight’s high-throughput capabilities, **database-affinity deployment** is recommended:

✅ **Deploy Flight Server close to the database** to reduce JDBC/ODBC remote call latency.  
✅ **Use Arrow Flight as an API layer** to avoid the overhead of JSON parsing in traditional REST APIs.  
✅ **Enable streaming transmission** to allow clients to fetch data on demand instead of loading it all at once.

---

## **📖 Related Documentation**

- **[CLI Guide](./docs/CLI_USAGE.md)** – FastFlight command-line tool usage instructions.
- **[FastAPI Integration Guide](./src/fastflight/fastapi_integration/README.md)** – How to expose Arrow Flight as a REST
  API.
- **[Error Handling Guide](./docs/ERROR_HANDLING.md)** – Comprehensive error handling and resilience patterns.

---

## **📌 Summary**

- **FastFlight provides a more efficient data transfer solution compared to REST API / JDBC**, making it suitable for
  large-scale data queries.
- **The `param_type` mechanism optimizes Ticket processing, making requests structured, readable, and extensible**.
- **Supports asynchronous streaming data consumption, improving throughput and reducing memory usage**.
- **Ideal for financial analytics, log processing, and high-concurrency data scenarios**.

🚀 **Start using FastFlight now to optimize your data transfer efficiency!**
