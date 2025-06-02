# **FastFlight CLI Usage Guide**

## **📌 Overview**

FastFlight provides a command-line interface (CLI) to simplify starting and managing the **FastFlight Server** and **REST API Server**. This CLI allows users to **quickly launch servers, test connectivity, and manage debugging options** without writing additional code.

## **🚀 Installation**

Ensure you have FastFlight installed:

```bash
pip install "fastflight[all]"
```

Once installed, the `fastflight` command becomes available.

---

## **🎯 Available CLI Commands**

All CLI commands now use **consistent option syntax** with `--flight-location` for Flight server and separate `--rest-host`/`--rest-port` for REST API server.

### **1️⃣ Start the FastFlight Server**

```bash
fastflight start-flight-server --flight-location grpc://0.0.0.0:8815
```

**Options:**

- `--flight-location` (optional): Specify the gRPC server address (default: `grpc://0.0.0.0:8815`).

### **2️⃣ Start the REST API Server**

```bash
fastflight start-rest-server --rest-host 0.0.0.0 --rest-port 8000 --rest-prefix /fastflight --flight-location grpc://0.0.0.0:8815 --modules fastflight.demo_services
```

**Options:**

- `--rest-host` (optional): Set REST API server host (default: `0.0.0.0`).
- `--rest-port` (optional): Set REST API server port (default: `8000`).
- `--rest-prefix` (optional): API route prefix (default: `/fastflight`).
- `--flight-location` (optional): Address of the Arrow Flight server (default: `grpc://0.0.0.0:8815`).
- `--modules` (optional): Comma-separated list of module paths to scan for custom data parameter and service classes (default: `fastflight.demo_services`).

### **3️⃣ Start Both FastFlight and REST API Servers**

```bash
fastflight start-all --flight-location grpc://0.0.0.0:8815 --rest-host 0.0.0.0 --rest-port 8000 --rest-prefix /fastflight --modules fastflight.demo_services
```

**Options:**

- `--flight-location` (optional): Address of the Arrow Flight server (default: `grpc://0.0.0.0:8815`).
- `--rest-host` (optional): REST API server host (default: `0.0.0.0`).
- `--rest-port` (optional): REST API server port (default: `8000`).
- `--rest-prefix` (optional): API route prefix (default: `/fastflight`).
- `--modules` (optional): Comma-separated list of module paths to scan for parameter classes (default: `fastflight.demo_services`).

This command launches **both FastFlight and REST API servers** as separate processes and supports `Ctrl+C` termination.

**Important**: The `--modules` option is crucial for loading custom data services. When using the `/stream` REST endpoint, ensure the `param_type` field in the request body matches the fully qualified class name from your loaded modules.

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
fastflight start-rest-server --help
```

---

## **💡 Usage Examples**

### **Development Setup**

```bash
# Start both servers with demo services (using defaults)
fastflight start-all

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
fastflight start-flight-server --flight-location grpc://0.0.0.0:8815

# Start REST API server on another machine/container
fastflight start-rest-server \
  --rest-host 0.0.0.0 \
  --rest-port 8000 \
  --flight-location grpc://flight-server:8815 \
  --modules foo.bar.services,fastflight.demo_services
```

### **Custom Data Services**

```bash
# Load custom modules for specialized data services
fastflight start-all \
  --modules myproject.services,external_package.data_services \
  --rest-port 8080 \
  --flight-location grpc://0.0.0.0:8815
```

### **Multiple Module Loading**

```bash
# Load multiple module paths for different service types
fastflight start-rest-server \
  --modules "fastflight.demo_services,mycompany.sql_services,mycompany.nosql_services" \
  --rest-prefix /api/v1/data
```

### **Minimal Commands (Using Defaults)**

```bash
# Simplest possible commands using all defaults
fastflight start-flight-server  # Uses grpc://0.0.0.0:8815
fastflight start-rest-server    # Uses host 0.0.0.0, port 8000
fastflight start-all            # Uses all defaults
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
  fastflight start-rest-server --rest-port 8080
  ```

### **Module Loading Issues**

- Ensure your custom modules are in PYTHONPATH:
  ```bash
  export PYTHONPATH="${PYTHONPATH}:/path/to/your/modules"
  fastflight start-all --modules your_module
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
  
  # Test REST API server
  curl http://localhost:8000/fastflight/registered_data_types
  ```

### **Service Registration Problems**

- Check registered services via REST API:
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
export FASTFLIGHT_LOCATION=grpc://0.0.0.0:8815
export FASTFLIGHT_REST_HOST=0.0.0.0
export FASTFLIGHT_REST_PORT=8000
export FASTFLIGHT_MODULES=fastflight.demo_services,mycompany.services
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

| Command                  | Description                               | Key Options                    |
|--------------------------|-------------------------------------------|--------------------------------|
| `start-flight-server`    | Start the FastFlight gRPC server         | `--flight-location`            |
| `start-rest-server`      | Start the REST API server as a proxy     | `--rest-host`, `--rest-port`, `--flight-location`, `--modules` |
| `start-all`              | Start both FastFlight and REST API servers| All options from above        |
| `--help`                 | Show help for any command                | N/A                            |

### **Consistent Options Across Commands**

- **`--flight-location`**: Flight server gRPC address (used by all commands)
- **`--rest-host`**: REST API server host 
- **`--rest-port`**: REST API server port
- **`--modules`**: Custom service modules to load
- **`--rest-prefix`**: API route prefix

### **Legacy Command Support**

For backward compatibility, the old command names are still supported but will show deprecation warnings:

```bash
# Legacy commands (deprecated)
fastflight start-fast-flight-server  # Use start-flight-server instead
fastflight start-fastapi             # Use start-rest-server instead
```

FastFlight CLI now provides a **consistent, predictable interface** for managing high-performance data transfer servers with improved naming conventions that clearly distinguish between gRPC Flight services and REST API services.

**🚀 Get started now and supercharge your data transfers!**