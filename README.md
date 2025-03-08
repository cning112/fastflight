# **FastFlight** 🚀

**FastFlight** is a framework built on **Apache Arrow Flight**, designed to simplify **high-performance data transfers**
while improving **usability, integration, and developer experience**.

It addresses common **challenges** with native Arrow Flight, such as **opaque request formats, debugging difficulties,
complex async management, and REST API incompatibility**. **FastFlight makes it easier to adopt Arrow Flight in existing
systems.

## **✨ Key Advantages**

✅ **Parameterized Ticket System** – Structured, type-safe API for better readability and debugging.  
✅ **Dynamic Data Service Registration** – Automatically discover and register custom parameter classes and their
associated data services using a built-in discovery mechanism.  
✅ **Enhanced Async & Streaming Support** – Simplified stream handling with `async for` capabilities.  
✅ **Seamless REST API Integration** – Enables **FastAPI** to bridge REST clients with Arrow Flight.  
✅ **Modular & Extensible** – Custom data sources and easy integration into existing pipelines.  
✅ **Pandas & PyArrow Compatible** – Optimized data retrieval for analytics and ML workflows.  
✅ **Built-in CLI** – Start servers and execute queries effortlessly via command line.

**FastFlight is ideal for high-throughput data systems, real-time querying, log analysis, and financial applications.**

---

## **🚀 Quick Start**

### **1️⃣ Install FastFlight**

```bash
pip install fastflight
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
- `--module_paths` (optional): Comma-separated list of module paths to scan for custom parameter classes (default:
  None).

**Note**: With the latest design update, FastFlight automatically discovers custom parameter classes (extending
BaseParams)
and registers the corresponding data services. Simply pass the module paths using the --module_paths option when
starting the FastAPI server.
---

### **Start Both FastFlight and FastAPI Servers**

```bash
fastflight start-all --api-host 0.0.0.0 --api-port 8000 --fast-flight-route-prefix /fastflight --flight-location grpc://0.0.0.0:8815
```

This command launches **both FastFlight and FastAPI servers** as separate processes.

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
