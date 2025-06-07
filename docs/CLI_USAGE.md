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
- `--modules` (optional): Module paths to scan for parameter classes (default: `fastflight.demo_services`).

### **2️⃣ Start the REST API Server**

```bash
fastflight start-rest-server --rest-host 0.0.0.0 --rest-port 8000 --rest-prefix /fastflight --flight-location grpc://0.0.0.0:8815 --modules fastflight.demo_services
```

**Options:**

- `--rest-host` (optional): Set REST API server host (default: `0.0.0.0`).
- `--rest-port` (optional): Set REST API server port (default: `8000`).
- `--rest-prefix` (optional): API route prefix (default: `/fastflight`).
- `--flight-location` (optional): Address of the Arrow Flight server (default: `grpc://0.0.0.0:8815`).
- `--modules` (optional): Module paths to scan for custom data parameter and service classes (default: `fastflight.demo_services`).
- `--resilience-preset` (optional): Resilience configuration preset (default: `default`).

### **3️⃣ Start Both FastFlight and REST API Servers**

```bash
fastflight start-all --flight-location grpc://0.0.0.0:8815 --rest-host 0.0.0.0 --rest-port 8000 --rest-prefix /fastflight --modules fastflight.demo_services
```

**Options:**

- `--flight-location` (optional): Address of the Arrow Flight server (default: `grpc://0.0.0.0:8815`).
- `--rest-host` (optional): REST API server host (default: `0.0.0.0`).
- `--rest-port` (optional): REST API server port (default: `8000`).
- `--rest-prefix` (optional): API route prefix (default: `/fastflight`).
- `--modules` (optional): Module paths to scan for parameter classes (default: `fastflight.demo_services`).
- `--resilience-preset` (optional): Resilience configuration preset (default: `default`).

This command launches **both FastFlight and REST API servers** as separate processes and supports `Ctrl+C` termination.

**Important**: The `--modules` option is crucial for loading custom data services. When using the `/stream` REST endpoint, ensure the `type` field in the request body contains the fully qualified class name from your loaded modules.

---

## **🛡️ Resilience Configuration**

FastFlight includes built-in resilience features for the REST API server's internal client. View available presets:

```bash
# List all resilience presets
fastflight list-resilience-presets

# Show detailed configuration for a preset
fastflight show-resilience-config --preset high_availability
```

**Available Resilience Presets:**

- `disabled`: No resilience features
- `default`: Balanced settings for general production use
- `high_availability`: Aggressive retries for critical services
- `batch_processing`: Conservative settings for batch jobs

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
    "type": "fastflight.demo_services.duckdb_demo.DuckDBParams",
    "database_path": ":memory:",
    "query": "SELECT 1 as test_column, '\''hello'\'' as message",
    "parameters": []
  }'
```

### **Production Setup with High Availability**

```bash
# Start FastFlight server on dedicated port
fastflight start-flight-server --flight-location grpc://0.0.0.0:8815

# Start REST API server with high availability resilience
fastflight start-rest-server \
  --rest-host 0.0.0.0 \
  --rest-port 8000 \
  --flight-location grpc://flight-server:8815 \
  --resilience-preset high_availability \
  --modules fastflight.demo_services
```

### **Custom Data Services**

```bash
# Load custom modules for specialized data services
fastflight start-all \
  --modules myproject.services,external_package.data_services \
  --rest-port 8080 \
  --flight-location grpc://0.0.0.0:8815
```

### **Batch Processing Setup**

```bash
# Use batch processing resilience preset for ETL workloads
fastflight start-all \
  --resilience-preset batch_processing \
  --modules "fastflight.demo_services,mycompany.etl_services"
```

### **Minimal Commands (Using Defaults)**

```bash
# Simplest possible commands using all defaults
fastflight start-flight-server  # Uses grpc://0.0.0.0:8815
fastflight start-rest-server    # Uses host 0.0.0.0, port 8000, default resilience
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
- Check that your data service classes are properly registered with the `@register_data_service` decorator:
  ```python
  # In your custom module
  from fastflight import BaseParams, register_data_service
  import pandas as pd
  
  @register_data_service
  class YourParams(BaseParams):
      # Your parameters
      pass
  
  def get_data(params: YourParams) -> pd.DataFrame:
      # Your implementation
      return pd.DataFrame(...)
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

### **Resilience Issues**

- Check resilience configuration:
  ```bash
  fastflight show-resilience-config --preset default
  ```
- Test with resilience disabled for debugging:
  ```bash
  fastflight start-rest-server --resilience-preset disabled
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
curl http://localhost:8000/fastflight/registered_data_types

# Test specific request
curl -X POST http://localhost:8000/fastflight/stream \
  -H "Content-Type: application/json" \
  -d '{"type": "fastflight.demo_services.duckdb_demo.DuckDBParams", "database_path": ":memory:", "query": "SELECT 1"}'
```

### **Custom Resilience Settings**

```bash
# Start with custom retry settings
fastflight start-rest-server \
  --resilience-preset default \
  --retry-max-attempts 10 \
  --circuit-breaker-failure-threshold 3

# Disable circuit breaker but keep retries
fastflight start-all \
  --resilience-preset default \
  --enable-circuit-breaker false
```

---

## **📌 Command Reference**

| Command                     | Description                               | Key Options                    |
|-----------------------------|-------------------------------------------|--------------------------------|
| `start-flight-server`       | Start the FastFlight gRPC server         | `--flight-location`, `--modules` |
| `start-rest-server`         | Start the REST API server as a proxy     | `--rest-host`, `--rest-port`, `--flight-location`, `--modules`, `--resilience-preset` |
| `start-all`                 | Start both FastFlight and REST API servers| All options from above        |
| `list-resilience-presets`   | Show available resilience configurations  | N/A                            |
| `show-resilience-config`    | Display detailed resilience settings     | `--preset`                     |
| `--help`                    | Show help for any command                | N/A                            |

### **Consistent Options Across Commands**

- **`--flight-location`**: Flight server gRPC address (used by all commands)
- **`--rest-host`**: REST API server host 
- **`--rest-port`**: REST API server port
- **`--modules`**: Custom service modules to load
- **`--rest-prefix`**: API route prefix
- **`--resilience-preset`**: Resilience configuration (REST server only)

### **REST API Request Format**

The REST API expects the `type` field to contain the fully qualified name (FQN) of the parameter class:

```bash
# Correct format - uses fully qualified name
curl -X POST "http://localhost:8000/fastflight/stream" \
  -d '{"type": "fastflight.demo_services.duckdb_demo.DuckDBParams", "database_path": ":memory:", "query": "SELECT 1"}'

# For custom modules
curl -X POST "http://localhost:8000/fastflight/stream" \
  -d '{"type": "mycompany.services.MyCustomParams", "param1": "value1"}'
```

**Key Points:**
- The `type` field must be the complete module path + class name
- This ensures proper service routing and parameter deserialization
- The class must be loaded via the `--modules` option

FastFlight CLI now provides a **consistent, predictable interface** for managing high-performance data transfer servers with built-in resilience and improved naming conventions that clearly distinguish between gRPC Flight services and REST API services.

**🚀 Get started now and supercharge your data transfers!**
