# Multi-Protocol Demo

This directory contains a comprehensive example demonstrating how to use the FastFlight library for high-performance
data transfer with multiple protocol interfaces. The example showcases how the same FastFlight server can be accessed
through different protocols, allowing for flexible integration approaches.

## Overview

This example demonstrates:

- **Unified FastFlight Server**: Single gRPC server serving multiple data services
- **Multi-Protocol Access**: Both direct gRPC and REST API access to the same server
- **Future Extensibility**: Architecture ready for additional protocols (WebSocket, GraphQL, etc.)
- **Multiple Data Services**: SQLite, DuckDB, and CSV file handling services
- **Performance Comparison**: Side-by-side comparison of different protocol performance
- **Synchronous and Asynchronous**: Support for both sync and async operations

## Prerequisites

- Python 3.10 or above
- FastFlight library installed with examples dependencies
- Required dependencies: `pandas`, `pyarrow`, `httpx`, `uvicorn`, `rich`, etc.

## Setup

1. Install the required packages:

   ```bash
   pip install "fastflight[examples]"
   ```

2. Or install everything:

   ```bash
   pip install "fastflight[all]"
   ```

## Quick Start

### 1. Start the FastFlight Server

First, start the main FastFlight gRPC server:

```bash
python start_flight_server.py
```

This will start the server at `grpc://localhost:8815` and load all demo services.

### 2. Start the REST Server (Optional)

In another terminal, start the REST API server:

```bash
python start_rest_server.py
```

This will start the REST API at `http://localhost:8000` with routes under `/fastflight`.

### 3. Run the Comprehensive Comparison

In a third terminal, run the multi-protocol comparison demo:

```bash
python run_demo.py
```

This will execute all demo services via both gRPC and REST, showing performance metrics and results.

## File Structure

```
examples/multi_protocol_demo/
├── README.md                      # This file
├── start_flight_server.py         # Starts the FastFlight gRPC server
├── start_rest_server.py           # Starts the REST API server
├── run_demo.py                    # Main multi-protocol comparison demo
└── demo_services/                 # Data service implementations
    ├── __init__.py
    ├── csv_demo.py                # CSV file handling service
    ├── duckdb_demo.py             # DuckDB integration service
    └── sqllite_demo.py            # SQLite database service
```

## Demo Services

### SQLite Service (`sqllite_demo.py`)

- **Purpose**: Demonstrates database queries via SQLAlchemy
- **Parameters**: Connection string and SQL query
- **Example**: Basic queries against the included `example.db`

### DuckDB Service (`duckdb_demo.py`)

- **Purpose**: Shows DuckDB integration for analytics workloads
- **Features**: In-memory databases, CSV processing, parameterized queries
- **Example**: Generate series data and CSV file analysis

### CSV Service (`csv_demo.py`)

- **Purpose**: Direct CSV file reading and processing
- **Features**: Both synchronous and asynchronous variants
- **Example**: Sensor data and time-series processing

## Usage Examples

### Direct gRPC Access

```python
from fastflight.client import FastFlightBouncer
from examples.multi_protocol_demo.demo_services.sqllite_demo import SQLParams

# Connect to FastFlight server
client = FastFlightBouncer("grpc://localhost:8815")

# Execute a query
params = SQLParams(
    conn_str="sqlite:///example.db",
    query="SELECT 1 as id, 'Hello' as message"
)

# Synchronous
df = client.get_pd_dataframe(params)

# Asynchronous
df = await client.aget_pd_dataframe(params)
```

### REST API Access

```python
import httpx
from fastflight.utils.stream_utils import read_dataframe_from_arrow_stream

# Prepare request
url = "http://127.0.0.1:8000/fastflight/stream"
json_data = {
    "type": "multi_protocol_demo.demo_services.sqllite_demo.SQLParams",
    "conn_str": "sqlite:///example.db",
    "query": "SELECT 1 as id, 'Hello' as message"
}

# Stream response
with httpx.stream("POST", url, json=json_data, timeout=30) as response:
    response.raise_for_status()
    df = read_dataframe_from_arrow_stream(response.iter_bytes())
```

## Server Configuration

### FastFlight Server (`start_flight_server.py`)

- **Address**: `grpc://localhost:8815`
- **Services**: Auto-loads all services from `demo_services` package
- **Protocol**: gRPC with Arrow Flight

### REST Server (`start_rest_server.py`)

- **Address**: `http://localhost:8000`
- **Route Prefix**: `/fastflight`
- **Backend**: Connects to FastFlight server at `grpc://localhost:8815`
- **Framework**: FastAPI with uvicorn

## Performance Insights

The comprehensive comparison typically shows:

- **gRPC Advantages**:
    - Lower latency for direct connections
    - More efficient binary protocol
    - Better for high-frequency data access

- **REST Advantages**:
    - Better web application compatibility
    - Easier debugging and monitoring
    - Standard HTTP tooling support

- **Common Benefits**:
    - Same underlying Arrow Flight performance
    - Identical data processing capabilities
    - Unified service implementation

## Protocol Extensibility

The architecture is designed to support additional protocols:

- **Future Protocols**: WebSocket, GraphQL, gRPC-Web
- **Unified Service Layer**: All protocols share the same data services
- **Consistent Interface**: Same parameter classes across all protocols
- **Performance Baseline**: Easy comparison of new protocols against existing ones

## Extending the Example

### Adding New Services

1. Create a new service in `demo_services/`:
   ```python
   # my_service.py
   from fastflight import BaseParams, register_data_service
   
   @register_data_service
   class MyParams(BaseParams):
       # Define your parameters
       
   def get_data(params: MyParams) -> pd.DataFrame:
       # Implement your data logic
   ```

2. The service will be automatically loaded by all protocol servers

### Adding New Protocols

1. Create a new protocol server (e.g., `start_websocket_server.py`)
2. Implement protocol-specific handlers that call FastFlight services
3. Update the comparison script to include the new protocol
4. Document the new protocol in this README

### Custom Configurations

Modify the server startup scripts to:

- Change ports and addresses
- Add authentication
- Configure logging
- Set custom route prefixes

## Troubleshooting

### Server Connection Issues

- Ensure both servers are running
- Check firewall settings for ports 8815 and 8000
- Verify no port conflicts with other applications

### Import Errors

- Confirm FastFlight installation with examples dependencies
- Check Python path configuration
- Ensure all required packages are installed

### Performance Issues

- Monitor server resource usage
- Consider data size and network conditions
- Check for concurrent access patterns

## Next Steps

- Explore the individual service implementations in `demo_services/`
- Modify queries and parameters to test with your data
- Implement custom data services for your specific use cases
- Consider adding new protocols (WebSocket, GraphQL) to the demo
- Evaluate the authentication and security requirements for production use

## Contributing

This example is part of the FastFlight project. Feel free to:

- Report issues or suggestions
- Submit improvements to the demo services
- Add new protocol implementations
- Add new example scenarios
- Enhance documentation

Happy coding with FastFlight multi-protocol support!
