# Enhanced Error Handling in FastFlight

FastFlight provides comprehensive error handling with structured exception hierarchies, retry mechanisms, and circuit breaker patterns to ensure robust operation in production environments.

## Exception Hierarchy

FastFlight defines a hierarchical exception system that allows for fine-grained error handling:

```
FastFlightError (base)
├── FastFlightConnectionError
├── FastFlightTimeoutError
├── FastFlightAuthenticationError
├── FastFlightServerError
│   └── FastFlightDataServiceError
├── FastFlightDataValidationError
├── FastFlightSerializationError
├── FastFlightResourceExhaustionError
├── FastFlightCircuitOpenError
└── FastFlightRetryExhaustedError
```

### Exception Types

- **`FastFlightError`**: Base exception for all FastFlight-related errors
- **`FastFlightConnectionError`**: Network connectivity issues, server unavailability
- **`FastFlightTimeoutError`**: Operations that exceed configured timeout limits
- **`FastFlightAuthenticationError`**: Authentication failures, invalid credentials
- **`FastFlightServerError`**: Server-side errors not related to client configuration
- **`FastFlightDataServiceError`**: Data service specific errors (query failures, etc.)
- **`FastFlightDataValidationError`**: Parameter validation failures
- **`FastFlightSerializationError`**: Data serialization/deserialization errors
- **`FastFlightResourceExhaustionError`**: Resource constraints (pool exhaustion, memory limits)
- **`FastFlightCircuitOpenError`**: Circuit breaker is in open state
- **`FastFlightRetryExhaustedError`**: All retry attempts have been exhausted

## Key Benefits

The enhanced error handling system provides:

1. **Fine-grained Error Classification**: Specific exception types for different failure modes
2. **Automatic Retry Logic**: Configurable retry strategies with backoff algorithms  
3. **Circuit Breaker Protection**: Prevents cascading failures in distributed systems
4. **Production-Ready Resilience**: Battle-tested patterns for robust applications
5. **Connection Pooling**: Intelligent connection management with resource exhaustion protection

## Resilience Configuration

### Basic Usage

```python
from fastflight import (
    FastFlightBouncer,
    FastFlightConnectionError,
    RetryConfig,
    RetryStrategy,
    CircuitBreakerConfig,
    ResilienceConfig
)

# Configure resilient client with connection pooling
bouncer = FastFlightBouncer(
    flight_server_location="grpc://localhost:8815",
    client_pool_size=10,
    resilience_config=ResilienceConfig(
        retry_config=RetryConfig(
            max_attempts=3,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
            base_delay=1.0
        ),
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=5,
            recovery_timeout=30.0
        ),
        enable_circuit_breaker=True
    )
)
```

### Retry Strategies

FastFlight supports multiple retry strategies:

```python
# Fixed delay between retries
retry_config = RetryConfig(
    max_attempts=5,
    strategy=RetryStrategy.FIXED_DELAY,
    base_delay=2.0
)

# Linear backoff (1s, 2s, 3s, 4s...)
retry_config = RetryConfig(
    max_attempts=5,
    strategy=RetryStrategy.LINEAR_BACKOFF,
    base_delay=1.0
)

# Exponential backoff (1s, 2s, 4s, 8s...)
retry_config = RetryConfig(
    max_attempts=5,
    strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    base_delay=1.0,
    exponential_base=2.0,
    max_delay=60.0
)

# Jittered exponential (adds randomization)
retry_config = RetryConfig(
    max_attempts=5,
    strategy=RetryStrategy.JITTERED_EXPONENTIAL,
    base_delay=1.0,
    jitter_factor=0.1
)
```

### Circuit Breaker Protection

```python
# Configure circuit breaker
circuit_config = CircuitBreakerConfig(
    failure_threshold=5,      # Open after 5 failures
    recovery_timeout=30.0,    # Try recovery after 30s
    success_threshold=2       # Close after 2 successes
)

# Use with bouncer
bouncer = FastFlightBouncer(
    "grpc://localhost:8815",
    resilience_config=ResilienceConfig(
        circuit_breaker_config=circuit_config,
        enable_circuit_breaker=True,
        circuit_breaker_name="my_service"
    )
)

# Check circuit breaker status
status = bouncer.get_circuit_breaker_status()
print(f"Circuit state: {status['state']}")
```

## Error Handling Examples

### Robust Data Fetching

```python
from fastflight.demo_services.duckdb_demo import DuckDBParams

async def fetch_data_with_error_handling():
    params = DuckDBParams(
        database_path=":memory:",
        query="SELECT 1 as test_column",
        parameters=[]
    )
    
    try:
        # Fetch data with automatic resilience
        table = await bouncer.aget_pa_table(params)
        print(f"Successfully received {len(table)} rows")
        return table
        
    except FastFlightConnectionError as e:
        print(f"Connection failed: {e.message}")
        print(f"Details: {e.details}")
        # Handle with fallback strategy
        return None
        
    except FastFlightResourceExhaustionError as e:
        print(f"Resource exhausted: {e.resource_type}")
        print(f"Pool status: {bouncer.get_connection_pool_status()}")
        # Wait and retry, or use alternative approach
        return None
        
    except FastFlightRetryExhaustedError as e:
        print(f"All {e.attempt_count} retries failed")
        # Handle persistent failures
        return None
```

### Connection Pool Management

```python
# Check pool status
pool_status = bouncer.get_connection_pool_status()
print(f"Available connections: {pool_status['available_connections']}")
print(f"Pool size: {pool_status['pool_size']}")

# Use context manager for automatic cleanup
async with FastFlightBouncer("grpc://localhost:8815") as bouncer:
    data = await bouncer.aget_pa_table(params)
    # Connections automatically closed when exiting context
```

### Per-Request Resilience Override

```python
# Override resilience settings for specific requests
high_priority_config = ResilienceConfig(
    retry_config=RetryConfig(max_attempts=10),
    enable_circuit_breaker=False  # Disable for critical requests
)

# Apply to specific request
critical_data = await bouncer.aget_pa_table(
    params, 
    resilience_config=high_priority_config
)
```

## Production Recommendations

1. **Use Connection Pooling**: Configure appropriate `client_pool_size` based on your concurrency needs
2. **Monitor Circuit Breakers**: Regularly check circuit breaker status for service health
3. **Configure Timeouts**: Set appropriate timeout values for your network environment
4. **Log Error Details**: Use the `details` field in exceptions for debugging
5. **Test Resilience**: Verify your error handling with network failures and service outages

This enhanced error handling makes FastFlight suitable for production environments where reliability and fault tolerance are critical requirements.
