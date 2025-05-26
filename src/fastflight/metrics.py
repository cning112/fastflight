from prometheus_client import Counter, Gauge, Histogram

# --- Flight Server Metrics ---
flight_server_requests_total = Counter(
    "flight_server_requests_total",
    "Total number of requests to the Flight server.",
    ["method", "status"]  # e.g., method="do_get", status="success" / "error"
)

flight_server_request_duration_seconds = Histogram(
    "flight_server_request_duration_seconds",
    "Histogram of Flight server request latencies.",
    ["method"]  # e.g., method="do_get"
)

flight_server_active_connections = Gauge(
    "flight_server_active_connections",
    "Number of currently active connections on the Flight server."
    # This might be hard to track accurately without deeper Flight integration.
    # For now, it can be incremented on request start and decremented on request end for do_get.
)

flight_server_bytes_transferred = Counter(
    "flight_server_bytes_transferred",
    "Total number of bytes transferred by the Flight server.",
    ["method", "direction"]  # e.g., method="do_get", direction="sent" / "received"
    # This is challenging to implement accurately without deep hooks.
    # We can count ticket bytes for "received" and RecordBatch bytes for "sent".
)

# --- FastAPI Application Metrics ---
# These might be largely provided by starlette-prometheus if used.
# If implementing manually or needing additional custom metrics:
fastapi_requests_total = Counter(
    "fastapi_requests_total",
    "Total number of requests to the FastAPI application.",
    ["method", "path", "status_code"]
)

fastapi_request_duration_seconds = Histogram(
    "fastapi_request_duration_seconds",
    "Histogram of FastAPI request latencies.",
    ["method", "path"]
)

# --- FastFlightBouncer Metrics ---
bouncer_connections_acquired_total = Counter(
    "bouncer_connections_acquired_total",
    "Total number of connections acquired from the bouncer pool."
)

bouncer_connections_released_total = Counter(
    "bouncer_connections_released_total",
    "Total number of connections released back to the bouncer pool."
)

bouncer_pool_size = Gauge(
    "bouncer_pool_size",
    "Configured size of the bouncer connection pool."
)

bouncer_pool_available_connections = Gauge(
    "bouncer_pool_available_connections",
    "Current number of available connections in the bouncer pool."
)

bouncer_circuit_breaker_state = Gauge(
    "bouncer_circuit_breaker_state",
    "State of the circuit breaker.",
    ["circuit_name", "state"],  # state: 0 for closed, 1 for open, 2 for half-open
)

bouncer_circuit_breaker_failures_total = Counter(
    "bouncer_circuit_breaker_failures_total",
    "Total number of failures tracked by the circuit breaker.",
    ["circuit_name"]
)

bouncer_circuit_breaker_successes_total = Counter(
    "bouncer_circuit_breaker_successes_total",
    "Total number of successes tracked by the circuit breaker (in half-open or closed state).",
    ["circuit_name"]
)

# Helper to map circuit breaker state string to a number for Prometheus
CIRCUIT_BREAKER_STATE_MAP = {
    "closed": 0,
    "open": 1,
    "half-open": 2,
}
