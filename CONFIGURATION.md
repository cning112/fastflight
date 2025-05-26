# Configuration Management in FastFlight

FastFlight utilizes a robust configuration system based on [Pydantic Settings](https://docs.pydantic.dev/latest/usage/settings/). This allows for type-validated settings loaded from various sources with clear precedence.

## Overview

Configuration for different components of FastFlight (Logging, Flight Server, FastAPI Application, Connection Bouncer) is managed through distinct Pydantic `BaseSettings` models. These models define the expected configuration parameters, their types, and default values.

## Loading Mechanism

Settings are loaded with the following order of precedence (highest to lowest):

1.  **CLI Arguments:** Command-line arguments provided to `fastflight` CLI commands (e.g., `--port` for `start-fastapi`) will override any other source for the specific parameters they control.
2.  **Environment Variables:** Settings can be provided as environment variables. Each setting group has a specific prefix.
3.  **`.env` File:** If a `.env` file is present in the working directory when the application starts and the `python-dotenv` package is installed, environment variables will be loaded from this file. These are then treated as regular environment variables.
4.  **Default Values:** If a setting is not found in any of the above sources, the default value defined in the Pydantic model is used.

## Setting Groups

### 1. Logging Settings

Controls the application-wide logging behavior.
**Environment Variable Prefix:** `FASTFLIGHT_LOGGING_`

| Variable Suffix | Description                       | Type   | Default Value | Example Value      |
| :-------------- | :-------------------------------- | :----- | :------------ | :----------------- |
| `LOG_LEVEL`     | Minimum logging level to output.  | `str`  | `"INFO"`      | `"DEBUG"`, `"WARN"` |
| `LOG_FORMAT`    | Log output format.                | `str`  | `"plain"`     | `"json"`           |

### 2. Flight Server Settings

Controls the behavior of the Arrow Flight server.
**Environment Variable Prefix:** `FASTFLIGHT_SERVER_`

| Variable Suffix        | Description                                  | Type   | Default Value | Example Value                  |
| :--------------------- | :------------------------------------------- | :----- | :------------ | :----------------------------- |
| `HOST`                 | Host address to bind the server to.          | `str`  | `"0.0.0.0"`   | `"127.0.0.1"`                  |
| `PORT`                 | Port to bind the server to.                  | `int`  | `8815`        | `9000`                         |
| `LOG_LEVEL`            | Logging level specific to the Flight server. | `str`  | `"INFO"`      | `"DEBUG"`                      |
| `AUTH_TOKEN`           | Enables token authentication if set.         | `str`  | `None`        | `"your-secret-token"`          |
| `TLS_CERT_PATH`        | Path to the server's TLS certificate file.   | `str`  | `None`        | `"/path/to/server.crt"`        |
| `TLS_KEY_PATH`         | Path to the server's TLS private key file.   | `str`  | `None`        | `"/path/to/server.key"`        |

### 3. FastAPI Application Settings

Controls the behavior of the FastAPI web application.
**Environment Variable Prefix:** `FASTFLIGHT_API_`

| Variable Suffix           | Description                                                        | Type        | Default Value           | Example Value                             |
| :------------------------ | :----------------------------------------------------------------- | :---------- | :---------------------- | :---------------------------------------- |
| `HOST`                    | Host address for Uvicorn to bind to.                               | `str`       | `"0.0.0.0"`             | `"127.0.0.1"`                             |
| `PORT`                    | Port for Uvicorn to bind to.                                       | `int`       | `8000`                  | `8080`                                    |
| `LOG_LEVEL`               | Logging level for Uvicorn and FastAPI app.                         | `str`       | `"INFO"`                | `"DEBUG"`                                 |
| `FLIGHT_SERVER_LOCATION`  | URL for the FastAPI app to connect to the Flight server.           | `str`       | `"grpc://localhost:8815"` | `"grpc+tls://flight.example.com:443"`   |
| `VALID_API_KEYS`          | Comma-separated list of valid API keys for client authentication.  | `list[str]` | `[]` (empty list)       | `"key1,key2,anotherkey"`                  |
| `SSL_KEYFILE`             | Path to the SSL private key file for Uvicorn (HTTPS).              | `str`       | `None`                  | `"/path/to/api.key"`                      |
| `SSL_CERTFILE`            | Path to the SSL certificate file for Uvicorn (HTTPS).              | `str`       | `None`                  | `"/path/to/api.crt"`                      |
| `METRICS_ENABLED`         | Enable (`True`) or disable (`False`) the `/metrics` endpoint.      | `bool`      | `True`                  | `False` (or `"false"`, `"0"`)            |

*Note on `VALID_API_KEYS`: An empty string for the environment variable `FASTFLIGHT_API_VALID_API_KEYS` will result in an empty list, effectively disabling API key checks if that's the desired policy (see `SECURITY.md`).*

### 4. Bouncer Settings

Controls the default behavior of the `FastFlightBouncer` (client-side Flight connection pool).
**Environment Variable Prefix:** `FASTFLIGHT_BOUNCER_`

| Variable Suffix | Description                               | Type  | Default Value | Example Value |
| :-------------- | :---------------------------------------- | :---- | :------------ | :------------ |
| `POOL_SIZE`     | Default number of connections in the pool. | `int` | `10`          | `20`          |
