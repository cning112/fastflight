# **FastFlight** 🚀

**FastFlight** is a framework built on **Apache Arrow Flight**, designed to simplify **high-performance data transfers**
while improving **usability, integration, and developer experience**.

It addresses common **challenges** with native Arrow Flight, such as **opaque request formats, debugging difficulties,
complex async management, and REST API incompatibility**. **FastFlight** makes it easier to adopt Arrow Flight in
existing
systems.

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

---

## **🎯 Using the CLI**

FastFlight provides a command-line interface (CLI) for easy management of **Arrow Flight and FastAPI servers**.

### **Start the FastFlight Server**

```bash
fastflight start-fast-flight-server --location grpc://0.0.0.0:8815
```

**Options:**

- `--location` (optional): gRPC server address (default: `grpc://0.0.0.0:8815`).

---

### **Start the FastAPI Server**

```bash
fastflight start-fastapi --host 0.0.0.0 --port 8000 --fast-flight-route-prefix /fastflight --flight-location grpc://0.0.0.0:8815
```

**Options:**

- `--host` (optional): FastAPI server host (default: `0.0.0.0`).
- `--port` (optional): FastAPI server port (default: `8000`).
- `--fast-flight-route-prefix` (optional): API route prefix (default: `/fastflight`).
- `--flight-location` (optional): Arrow Flight server address (default: `grpc://0.0.0.0:8815`).
- `--module_paths` (optional): Comma-separated list of module paths to scan for custom data parameter and service
- classes (default: `fastflight.demo_services`).

**Note**: When using the `/stream` REST endpoint to stream data, make sure the `param_type` field is embedded in the
request body. It's critical for the server to route the request to the correct data service. For example, for the
default demo services, the `param_type` should be `fastflight.demo_services.duckdb_demo.DuckDBParams`.

---

### **Start Both FastFlight and FastAPI Servers**

```bash
fastflight start-all --api-host 0.0.0.0 --api-port 8000 --fast-flight-route-prefix /fastflight --flight-location grpc://0.0.0.0:8815 --module-paths fastflight.demo_services.duckdb_demo
```

This launches both gRPC and REST servers, allowing you to use REST APIs while streaming data via Arrow Flight.

---

## **📖 Additional Documentation**

- **[CLI Guide](./docs/CLI_USAGE.md)** – Detailed CLI usage instructions.
- **[FastAPI Integration Guide](./src/fastflight/fastapi/README.md)** – Learn how to expose Arrow Flight via FastAPI.
- **[Technical Documentation](./docs/TECHNICAL_DETAILS.md)** – In-depth implementation details.

---

## **🛠 Future Plans**

✅ **Structured Ticket System** (Completed)  
✅ **Async & Streaming Support** (Completed)  
✅ **REST API Adapter** (Completed)  
✅ **CLI Support** (Completed)  
🔄 **Support for More Data Sources (SQL, NoSQL, Kafka)** (In Progress)  
🔄 **Enhanced Debugging & Logging Tools** (In Progress)

Contributions are welcome! If you have suggestions or improvements, feel free to submit an Issue or PR. 🚀

---

## **📜 License**

This project is licensed under the **MIT License**.

---

**🚀 Ready to accelerate your data transfers? Get started today!**
