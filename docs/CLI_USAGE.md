# **FastFlight CLI Usage Guide**

## **📌 Overview**

FastFlight provides a command-line interface (CLI) to simplify starting and managing the **FastFlight Server** and **FastAPI Server**. This CLI allows users to **quickly launch servers, test connectivity, and manage debugging options** without writing additional code.

## **🚀 Installation**

Ensure you have FastFlight installed:

```bash
pip install "fastflight[all]"
```

Once installed, the `fastflight` command becomes available.

---

## **🎯 Available CLI Commands**

All CLI commands now use **consistent option syntax** with `--location` across all commands for maximum usability and predictability.

### **1️⃣ Start the FastFlight Server**

```bash
fastflight start-fast-flight-server --location grpc://0.0.0.0:8815
```

**Options:**

- `--location` (optional): Specify the gRPC server address (default: `grpc://0.0.0.0:8815`).

### **2️⃣ Start the FastAPI Server**

```bash
fastflight start-fastapi --host 0.0.0.0 --port 8000 --fast-flight-route-prefix /fastflight --location grpc://0.0.0.0:8815 --module-paths fastflight.demo_services
```

**Options:**

- `--host` (optional): Set FastAPI server host (default: `0.0.0.0`).
- `--port` (optional): Set FastAPI server port (default: `8000`).
- `--fast-flight-route-prefix` (optional): API route prefix (default: `/fastflight`).
- `--location` (optional): Address of the Arrow Flight server (default: `grpc://0.0.0.0:8815`).
- `--module-paths` (optional): Comma-separated list of module paths to scan for custom data parameter and service classes (default: `fastflight.demo_services`).

### **3️⃣ Start Both FastFlight and FastAPI Servers**

```bash
fastflight start-all --api-host 0.0.0.0 --api-port 8000 --fast-flight-route-prefix /fastflight --location grpc://0.0.0.0:8815 --module-paths fastflight.demo_services
```

**Options:**

- `--api-host` (optional): FastAPI server host (default: `0.0.0.0`).
- `--api-port` (optional): FastAPI server port (default: `8000`).
- `--fast-flight-route-prefix` (optional): API route prefix (default: `/fastflight`).
- `--location` (optional): Address of the Arrow Flight server (default: `grpc://0.0.0.0:8815`).
- `--module-paths` (optional): Comma-separated list of module paths to scan for parameter classes (default: `fastflight.demo_services`).

This command launches **both FastFlight and FastAPI servers** as separate processes and supports `Ctrl+C` termination.

**Important**: The `--module-paths` option is crucial for loading custom data services. When using the `/stream` REST endpoint, ensure the `param_type` field in the request body matches the fully qualified class name from your loaded modules.

---

## **✨ Key Improvements**

All commands now use **consistent option syntax** with `--` flags:

- ✅ **Consistent**: All parameters use `--flag value` format
- ✅ **Clear**: `--location` is used consistently across all commands  
- ✅ **Predictable**: Users don't need to remember different parameter formats
- ✅ **Self-documenting**: `--help` shows clear option descriptions

---

## **🔍 Checking Installed CLI Commands**

To list all available CLI commands, run:

```bash
fastflight --help
```

For help on a specific command, run:

```bash
fastflight <command> --help
```

Example:

```bash
fastflight start-fastapi --help
```

---

## **💡 Usage Examples**

### **Development Setup**

```bash
# Start both servers with demo services (using defaults)
fastflight start-all

# Start both servers with custom location
fastflight start-all --location grpc://0.0.0.0:8815

# Test the setup with a simple request
curl -X POST "http://localhost:8000/fastflight/stream" \
  -H "Content-Type: application/json" \
  -d '{
    "param_type": "fastflight.demo_services.duckdb_demo.DuckDBParams",
    "database_path": ":memory:",
    "query": "SELECT 1 as test_column, '\''hello'\'' as message",
    "parameters": []
  }'
```

### **Production Setup**

```bash
# Start FastFlight server on dedicated port
fastflight start-fast-flight-server --location grpc://0.0.0.0:8815

# Start FastAPI server on another machine/container
fastflight start-fastapi \
  --host 0.0.0.0 \
  --port 8000 \
  --location grpc://flight-server:8815 \
  --module-paths your_company.data_services,fastflight.demo_services
```

### **Custom Data Services**

```bash
# Load custom modules for specialized data services
fastflight start-all \
  --module-paths myproject.services,external_package.data_services \
  --api-port 8080 \
  --location grpc://0.0.0.0:8815
```

### **Multiple Module Loading**

```bash
# Load multiple module paths for different service types
fastflight start-fastapi \
  --module-paths "fastflight.demo_services,mycompany.sql_services,mycompany.nosql_services" \
  --fast-flight-route-prefix /api/v1/data
```

### **Minimal Commands (Using Defaults)**

```bash
# Simplest possible commands using all defaults
fastflight start-fast-flight-server  # Uses grpc://0.0.0.0:8815
fastflight start-fastapi             # Uses host 0.0.0.0, port 8000
fastflight start-all                 # Uses all defaults
```

---

## **🛠 Troubleshooting**

### **Command not found?**

- Ensure FastFlight is installed: `pip install "fastflight[all]"`
- If installed globally, try: `python -m fastflight --help`
- Check if the CLI is in your PATH: `which fastflight`

### **Port already in use?**

- Stop any existing process using the port:
  ```bash
  lsof -i :8000  # Check processes on port 8000
  kill -9 <PID>  # Replace <PID> with the actual process ID
  ```
- Or use a different port:
  ```bash
  fastflight start-fastapi --port 8080
  ```

### **Module Loading Issues**

- Ensure your custom modules are in PYTHONPATH:
  ```bash
  export PYTHONPATH="${PYTHONPATH}:/path/to/your/modules"
  fastflight start-all --module-paths your_module
  ```
- Check that your data service classes are properly registered:
  ```python
  # In your custom module
  from fastflight.core.base import BaseDataService, BaseParams
  
  class YourParams(BaseParams):
      # Your parameters
      pass
  
  class YourService(BaseDataService[YourParams]):
      # Your implementation
      pass
  ```

### **Connection Issues**

- Verify server connectivity:
  ```bash
  # Test FastFlight server
  telnet localhost 8815
  
  # Test FastAPI server
  curl http://localhost:8000/fastflight/registered_data_types
  ```

### **Service Registration Problems**

- Check registered services via FastAPI:
  ```bash
  curl http://localhost:8000/fastflight/registered_data_types
  ```
- Ensure module imports are working:
  ```bash
  python -c "import your_module; print('Module loaded successfully')"
  ```

---

## **🔧 Advanced Configuration**

### **Environment Variables**

You can set default values using environment variables:

```bash
export FASTFLIGHT_HOST=0.0.0.0
export FASTFLIGHT_PORT=8815
export FASTAPI_HOST=0.0.0.0
export FASTAPI_PORT=8000
export FASTFLIGHT_MODULE_PATHS=fastflight.demo_services,mycompany.services
```

### **Logging Configuration**

```bash
# Enable debug logging
export FASTFLIGHT_LOG_LEVEL=DEBUG
fastflight start-all

# Log to file
fastflight start-all 2>&1 | tee fastflight.log
```

### **Health Checks**

```bash
# Check if services are running
curl http://localhost:8000/fastflight/health
curl http://localhost:8000/fastflight/registered_data_types

# Check specific service endpoints
curl -X POST http://localhost:8000/fastflight/validate \
  -H "Content-Type: application/json" \
  -d '{"param_type": "fastflight.demo_services.duckdb_demo.DuckDBParams"}'
```

---

## **📌 Command Reference**

| Command                        | Description                               | Key Options                    |
|--------------------------------|-------------------------------------------|--------------------------------|
| `start-fast-flight-server`     | Start the FastFlight gRPC server         | `--location`                   |
| `start-fastapi`                | Start the FastAPI server as a proxy      | `--host`, `--port`, `--location`, `--module-paths` |
| `start-all`                    | Start both FastFlight and FastAPI servers| All options from above        |
| `--help`                       | Show help for any command                | N/A                            |

### **Consistent Options Across Commands**

- **`--location`**: Flight server address (used by all commands)
- **`--host`**: Server host (FastAPI commands)
- **`--port`** / **`--api-port`**: Server port
- **`--module-paths`**: Custom service modules to load
- **`--fast-flight-route-prefix`**: API route prefix

FastFlight CLI now provides a **consistent, predictable interface** for managing high-performance data transfer servers.

**🚀 Get started now and supercharge your data transfers!**