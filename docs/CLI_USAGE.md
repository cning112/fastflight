# **FastFlight CLI Usage Guide**

## **📌 Overview**

FastFlight provides a command-line interface (CLI) to simplify starting and managing the **FastFlight Server** and *
*FastAPI Server**. This CLI allows users to **quickly launch servers, test connectivity, and manage debugging options**
without writing additional code.

## **🚀 Installation**

Ensure you have FastFlight installed:

```bash
pip install fastflight
```

Once installed, the `fastflight` command becomes available.

---

## **🎯 Available CLI Commands**

### **1️⃣ Start the FastFlight Server**

```bash
fastflight start-fast-flight-server --location grpc://0.0.0.0:8815
```

**Options:**

- `--location` (optional): Specify the gRPC server address (default: `grpc://0.0.0.0:8815`).

### **2️⃣ Start the FastAPI Server**

```bash
fastflight start-fastapi --host 0.0.0.0 --port 8000 --fast-flight-route-prefix /fastflight --flight-location grpc://0.0.0.0:8815
```

**Options:**

- `--host` (optional): Set FastAPI server host (default: `0.0.0.0`).
- `--port` (optional): Set FastAPI server port (default: `8000`).
- `--fast-flight-route-prefix` (optional): API route prefix (default: `/fastflight`).
- `--flight-location` (optional): Address of the Arrow Flight server (default: `grpc://0.0.0.0:8815`).

### **3️⃣ Start Both FastFlight and FastAPI Servers**

```bash
fastflight start-all --api-host 0.0.0.0 --api-port 8000 --fast-flight-route-prefix /fastflight --flight-location grpc://0.0.0.0:8815
```

**Options:**

- `--api-host` (optional): FastAPI server host (default: `0.0.0.0`).
- `--api-port` (optional): FastAPI server port (default: `8000`).
- `--fast-flight-route-prefix` (optional): API route prefix (default: `/fastflight`).
- `--flight-location` (optional): Address of the Arrow Flight server (default: `grpc://0.0.0.0:8815`).

This command launches **both FastFlight and FastAPI servers** as separate processes and supports `Ctrl+C` termination.

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

## **🛠 Troubleshooting**

**1. Command not found?**

- Ensure FastFlight is installed: `pip install fastflight`
- If installed globally, try: `python -m fastflight --help`

**2. Port already in use?**

- Stop any existing process using the port:
  ```bash
  lsof -i :8000  # Check processes on port 8000
  kill -9 <PID>  # Replace <PID> with the actual process ID
  ```
- Or use a different port:
  ```bash
  fastflight start-fastapi --port 8080
  ```

---

## **📌 Summary**

| Command                               | Description                               |
|---------------------------------------|-------------------------------------------|
| `fastflight start-fast-flight-server` | Start the FastFlight gRPC server          |
| `fastflight start-fastapi`            | Start the FastAPI server as a proxy       |
| `fastflight start-all`                | Start both FastFlight and FastAPI servers |
| `fastflight --help`                   | List all available CLI commands           |

FastFlight CLI simplifies the management of high-performance data transfer servers, making it easy to deploy and debug
Arrow Flight-based solutions.

**🚀 Get started now and supercharge your data transfers!**

