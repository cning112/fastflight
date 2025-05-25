[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/cning112/fastflight)

# **FastFlight** 🚀

**FastFlight** is a framework built on **Apache Arrow Flight**, designed to simplify **high-performance data transfers**
while improving **usability, integration, and developer experience**. Now with **intelligent time series processing**
and **distributed computing** support.

It addresses common **challenges** with native Arrow Flight, such as **opaque request formats, debugging difficulties,
complex async management, and REST API incompatibility**. **FastFlight** makes it easier to adopt Arrow Flight in
existing systems.

## **✨ Key Advantages**

✅ **Typed Param Classes** – All data requests are defined via structured, type-safe parameter classes. Easy to debug and
validate.  
✅ **Time Series Intelligence** – Smart partitioning and optimization for time series workloads.  
✅ **Distributed Processing** – Scale horizontally using Ray clusters for large datasets.  
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

**FastFlight is ideal for high-throughput data systems, real-time querying, log analysis, financial applications, and
time series analytics.**

## **🆕 Time Series & Distributed Processing**

FastFlight now includes advanced time series capabilities:

### **Smart Partitioning**

```python
from fastflight import TimeSeriesParams, OptimizationHint, optimize_time_series_query


# Define your time series parameters
class StockDataParams(TimeSeriesParams):
    symbol: str
    interval: str = "1min"


# Automatic intelligent partitioning
params = StockDataParams(symbol="AAPL", start_time=..., end_time=...)
partitions = params.get_optimal_partitions(max_workers=8)
```

### **Query Optimization**

```python
# Real-time queries (low latency)
hint = OptimizationHint.for_real_time()
partitions = optimize_time_series_query(params, hint)

# Analytics queries (high throughput)  
hint = OptimizationHint.for_analytics()
partitions = optimize_time_series_query(params, hint)
```

### **Distributed Processing**

```python
# Scale across Ray cluster
from fastflight import DistributedTimeSeriesService

distributed_service = DistributedTimeSeriesService(base_service)
async for batch in distributed_service.aget_batches(params):
    process_batch(batch)
```

See **[Time Series Guide](./docs/TIME_SERIES_DISTRIBUTED.md)** for complete documentation.

## **🚀 Quick Start**

### **1️⃣ Install FastFlight**

```bash
# Basic installation
pip install "fastflight[default]"

# With distributed processing support
pip install "fastflight[all]"

# Or use uv
uv add "fastflight[all]"
```

## **🐳 Docker Deployment**

```bash
# Quick start with Docker Compose
docker-compose --profile dev up

# Or run manually
docker run -p 8000:8000 -p 8815:8815 fastflight:latest start-all
```

See **[Docker Guide](./docs/DOCKER.md)** for complete deployment options.

## **🎯 Using the CLI**

FastFlight provides a command-line interface (CLI) for easy management of **Arrow Flight and FastAPI servers**.

### **Start the FastFlight Server**

```bash
fastflight start-fast-flight-server --location grpc://0.0.0.0:8815
```

### **Start the FastAPI Server**

```bash
fastflight start-fastapi --host 0.0.0.0 --port 8000 --fast-flight-route-prefix /fastflight --flight-location grpc://0.0.0.0:8815
```

### **Start Both Servers**

```bash
fastflight start-all --api-host 0.0.0.0 --api-port 8000 --fast-flight-route-prefix /fastflight --flight-location grpc://0.0.0.0:8815 --module-paths fastflight.demo_services.duckdb_demo
```

## **📖 Additional Documentation**

- **[Time Series & Distributed Processing](./docs/TIME_SERIES_DISTRIBUTED.md)** – Smart partitioning and Ray
  integration.
- **[CLI Guide](./docs/CLI_USAGE.md)** – Detailed CLI usage instructions.
- **[Docker Deployment](./docs/DOCKER.md)** – Container deployment and Docker Compose guide.
- **[FastAPI Integration Guide](./src/fastflight/fastapi/README.md)** – Learn how to expose Arrow Flight via FastAPI.
- **[Technical Documentation](./docs/TECHNICAL_DETAILS.md)** – In-depth implementation details.

## **🛠 Future Plans**

✅ **Structured Ticket System** (Completed)  
✅ **Async & Streaming Support** (Completed)  
✅ **REST API Adapter** (Completed)  
✅ **CLI Support** (Completed)  
✅ **Time Series Processing** (Completed)  
✅ **Distributed Computing** (Completed)  
🔄 **Support for More Data Sources (SQL, NoSQL, Kafka)** (In Progress)  
🔄 **Enhanced Debugging & Logging Tools** (In Progress)  
🔄 **Real-time Stream Processing** (Planned)

Contributions are welcome! If you have suggestions or improvements, feel free to submit an Issue or PR. 🚀

## **📜 License**

This project is licensed under the **MIT License**.

**🚀 Ready to accelerate your data transfers? Get started today!**
