# FastFlight Resilience

Dead-simple retry and circuit breaker patterns for reliable data operations.

## Quick Start

```python
from fastflight.resilience import ResilienceManager

# Create manager with defaults (3 retries, circuit breaker enabled)
manager = ResilienceManager()

# Wrap any async or sync function with automatic retry + circuit breaker
# Note: even if your_function is sync, you must await the call because execute_with_resilience is async
result = await manager.execute_with_resilience(your_function, *args, **kwargs)
```

## Usage Patterns

### Basic Retry Only

```python
from fastflight.resilience import ResilienceConfig, RetryConfig, ResilienceManager

manager = ResilienceManager()

config = ResilienceConfig(
    retry_config=RetryConfig(max_attempts=5, base_delay=1.0),
    enable_circuit_breaker=False
)

# your_function can be async or sync, but you must await the call
result = await manager.execute_with_resilience(risky_operation, config=config)
```

### Circuit Breaker Only

```python
from fastflight.resilience import ResilienceConfig, ResilienceManager

manager = ResilienceManager()

config = ResilienceConfig(
    circuit_breaker_name="api_calls",
    retry_config=None  # No retries
)

# your_function can be async or sync, but you must await the call
result = await manager.execute_with_resilience(api_call, config=config)
```

### Production Ready

```python
from fastflight.resilience import ResilienceConfig, ResilienceManager

manager = ResilienceManager()

# High availability: aggressive retries + fast circuit breaker to minimize downtime
config = ResilienceConfig.create_for_high_availability()

# Batch processing: conservative retries + tolerant circuit breaker for background jobs  
config = ResilienceConfig.create_for_batch_processing()
```

## Configuration Presets

| Preset                           | Max Retries | Retry Strategy       | Circuit Threshold | Use Case            |
|----------------------------------|-------------|----------------------|-------------------|---------------------|
| `create_default()`               | 3           | Exponential          | 5 failures        | General purpose     |
| `create_for_high_availability()` | 5           | Jittered exponential | 3 failures        | Critical operations |
| `create_for_batch_processing()`  | 2           | Fixed delay          | 10 failures       | Background jobs     |

## Architecture

Configuration and runtime logic are separated into distinct modules for clarity and maintainability:

```
resilience/
├── config/          # Pydantic models with validation
│   ├── retry.py           # RetryConfig
│   ├── circuit_breaker.py # CircuitBreakerConfig  
│   └── resilience.py      # ResilienceConfig (combines both)
├── core/            # Implementation
│   ├── circuit_breaker.py # CircuitBreaker class
│   └── manager.py         # ResilienceManager (main API)
└── types.py         # Enums and shared types
```

## Key Classes

- **`ResilienceManager`** - Main API, wrap functions with resilience (import from `fastflight.resilience.core.manager`)
- **`ResilienceConfig`** - Configure retry + circuit breaker behavior (import from
  `fastflight.resilience.config.resilience`)
- **`RetryConfig`** - Retry-specific settings with validation (import from `fastflight.resilience.config.retry`)
- **`CircuitBreakerConfig`** - Circuit breaker settings with validation (import from
  `fastflight.resilience.config.circuit_breaker`)

## Error Handling

The manager handles these FastFlight exceptions automatically:

- `FastFlightConnectionError`
- `FastFlightTimeoutError`
- `FastFlightServerError`

Customize retryable exceptions in `RetryConfig.retryable_exceptions`.
