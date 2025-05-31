[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/cning112/fastflight)

# **FastFlight** 🚀

**FastFlight** is a framework built on **Apache Arrow Flight**, designed to simplify **high-performance data transfers**
while improving **usability, integration, and developer experience**.

It addresses common **challenges** with native Arrow Flight, such as **opaque request formats, debugging difficulties,
complex async management, and REST API incompatibility**. **FastFlight** makes it easier to adopt Arrow Flight in
existing systems.

## **✨ Key Advantages**

✅ **Typed Param Classes** – All data requests are defined via structured, type-safe parameter classes. Easy to debug and
validate.  
✅ **Service Binding via `param_type`** – Clean and explicit mapping from param class → data service. Enables dynamic
routing and REST support.  
✅ **Async & Streaming Ready** – `async for` support with non-blocking batch readers. Ideal for high-throughput
systems.  
✅ **REST + Arrow Flight** – Use FastAPI to expose Arrow Flight services as standard REST endpoints (e.g., `/stream`).  
✅ **Plug-and-Play Data Sources** – Includes a DuckDB demo example to help you get started quickly—extending to other
sources (SQL, CSV, etc.) is straightforward.  
✅ **Built-in Registry & Validation** – Automatic binding discovery and safety checks. Fail early if service is
missing.  
✅ **Pandas / PyArrow Friendly** – Streamlined APIs for transforming results into pandas DataFrame or Arrow Table.  
✅ **CLI-First** – Unified command line to launch, test, and inspect services.

**FastFlight is ideal for high-throughput data systems, real-time querying, log analysis, and financial applications.**

---

## **🚀 Quick Start**

### **1️⃣ Install FastFlight**

```bash
pip install "fastflight[all]"
```

or use `uv`

```bash
uv add "fastflight[all]"
```

### **2️⃣ Start the Server**

```bash
# Start both FastFlight and REST API servers
fastflight start-all --flight-location grpc://0.0.0.0:8815 --rest-host 0.0.0.0 --rest-port 8000
```

This launches both gRPC and REST servers, allowing you to use REST APIs while streaming data via Arrow Flight.

### **3️⃣ Test with Demo Service**

```bash
# Example REST API call to DuckDB demo service
curl -X POST "http://localhost:8000/fastflight/stream" \
  -H "Content-Type: application/json" \
  -d '{
    "param_type": "fastflight.demo_services.duckdb_demo.DuckDBParams",
    "database_path": ":memory:",
    "query": "SELECT 1 as test_column",
    "parameters": []
  }'
```

---

## **🎯 Using the CLI**

FastFlight provides a command-line interface (CLI) for easy management of **Arrow Flight and REST API servers**.

### **Start Individual Services**

```bash
# Start only the FastFlight server
fastflight start-flight-server --flight-location grpc://0.0.0.0:8815

# Start only the REST API server
fastflight start-rest-server --rest-host 0.0.0.0 --rest-port 8000 --flight-location grpc://0.0.0.0:8815
```

### **Start Both Services**

```bash
fastflight start-all --flight-location grpc://0.0.0.0:8815 --rest-host 0.0.0.0 --rest-port 8000
```

**Important**: When using the `/stream` REST endpoint, ensure the `param_type` field is included in the request body for proper service routing.

---

## **🐳 Docker Deployment**

### **Quick Start with Docker Compose**

```bash
# Development setup (both servers in one container)
docker-compose --profile dev up

# Production setup (separated services)
docker-compose up

# Background mode
docker-compose up -d
```

### **Manual Docker Commands**

```bash
# Run both servers
docker run -p 8000:8000 -p 8815:8815 fastflight:latest start-all

# Run only FastFlight server
docker run -p 8815:8815 fastflight:latest start-flight-server

# Run only REST API server
docker run -p 8000:8000 fastflight:latest start-rest-server
```

See **[Docker Guide](./docs/DOCKER.md)** for complete deployment options and configuration.

---

## **💡 Usage Examples**

### **Python Client Example**

```python
from fastflight import FastFlightBouncer
from fastflight.demo_services.duckdb_demo import DuckDBParams

# Create client
client = FastFlightBouncer("grpc://localhost:8815")

# Define query parameters
params = DuckDBParams(
    database_path=":memory:",
    query="SELECT 1 as test_column, 'hello' as message",
    parameters=[]
)

# Fetch data as Arrow Table
table = client.get_pa_table(params)
print(f"Received {len(table)} rows")

# Convert to Pandas DataFrame
df = table.to_pandas()
print(df)
```

### **Async Streaming Example**

```python
import asyncio
from fastflight import FastFlightBouncer

async def stream_data():
    client = FastFlightBouncer("grpc://localhost:8815")
    
    async for batch in client.aget_record_batches(params):
        print(f"Received batch with {batch.num_rows} rows")
        # Process batch incrementally

asyncio.run(stream_data())
```

---

## **📖 Documentation**

- **[CLI Guide](./docs/CLI_USAGE.md)** – Detailed CLI usage instructions
- **[Docker Deployment](./docs/DOCKER.md)** – Container deployment and Docker Compose guide
- **[Error Handling](./docs/ERROR_HANDLING.md)** – Comprehensive error handling and resilience patterns
- **[Technical Details](./TECHNICAL_DETAILS.md)** – In-depth implementation details and architecture
- **[FastAPI Integration](./src/fastflight/fastapi/README.md)** – REST API integration guide

---

## **🛠 Extending FastFlight**

Create custom data services by extending `BaseDataService`:

```python
from fastflight.core.base import BaseDataService, BaseParams
import pyarrow as pa

class CustomParams(BaseParams):
    source_path: str
    filter_condition: str

class CustomDataService(BaseDataService[CustomParams]):
    def get_batches(self, params: CustomParams, batch_size: int | None = None):
        # Your custom data fetching logic here
        yield pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3])], 
            ["custom_column"]
        )
```

---

## **🛠 Future Plans**

✅ **Structured Ticket System** (Completed)  
✅ **Async & Streaming Support** (Completed)  
✅ **REST API Adapter** (Completed)  
✅ **CLI Support** (Completed)  
✅ **Enhanced Error Handling & Resilience** (Completed)  
🔄 **Support for More Data Sources (SQL, NoSQL, Kafka)** (In Progress)  
🔄 **Performance Benchmarking Tools** (In Progress)  
🔄 **Production Monitoring & Observability** (Planned)

Contributions are welcome! If you have suggestions or improvements, feel free to submit an Issue or PR. 🚀

---

## **📜 License**

This project is licensed under the **MIT License**.

---

**🚀 Ready to accelerate your data transfers? Get started today!**