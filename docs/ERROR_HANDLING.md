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
5. **Comprehensive Monitoring**: Built-in observability for error tracking and alerting

## Usage Example

```python
from fastflight import (
    FastFlightClient,
    FastFlightConnectionError,
    RetryConfig,
    RetryStrategy,
    CircuitBreakerConfig
)

# Configure resilient client
client = FastFlightClient(
    flight_server_location="grpc://localhost:8815",
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

# Robust data fetching with error handling
try:
    data = await client.aget_pa_table(params)
    print(f"Successfully received {len(data)} rows")
    
except FastFlightConnectionError as e:
    print(f"Connection failed: {e.message}")
    # Handle with fallback strategy
    
except FastFlightRetryExhaustedError as e:
    print(f"All {e.attempt_count} retries failed")
    # Handle persistent failures
```

This enhanced error handling makes FastFlight suitable for production environments where reliability and fault tolerance are critical requirements.
