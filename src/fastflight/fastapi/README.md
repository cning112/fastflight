# FastFlight FastAPI Integration

This module provides an HTTP API proxy for **FastFlight** combined with **FastAPI**, allowing clients to access an Arrow
Flight server over HTTP.

## 📌 Features

- **Configurable HTTP API Interface**: Provides HTTP endpoints to interact with Arrow Flight, with support for dynamic
  route prefixes.
- **Streaming Support**: Enables efficient data streaming from the Flight server.
- **Lifecycle Management**: `create_app()` supports multiple lifespan functions, making it easy to execute custom logic
  on startup and shutdown.
- **Custom Flight Server Location**: Allows specifying the `flight_location` to connect to different Arrow Flight
  servers dynamically.

---

## 🚀 Quick Start

### 1️⃣ **Start Arrow Flight Server**

Ensure that the Flight Server is running at your desired `flight_location`, e.g., `grpc://0.0.0.0:8815`.

### 2️⃣ **Run the FastAPI Server**

```bash
uvicorn fastflight.fastapi.app:create_app --factory --reload
```

By default, API endpoints will be mounted at `/fastflight`.

---

## ⚙️ **Customizing API Route Prefix**

To change the FastAPI API prefix, for example, to `/api/v1/fastflight`, modify the app initialization:

```python
from fastflight.fastapi.app import create_app

app = create_app(route_prefix="/api/v1/fastflight")
```

Now, API endpoints will be accessible at:

```
POST /api/v1/fastflight/
```

---

## 🌍 **Specifying the Flight Server Location**

`create_app()` allows specifying a `flight_location` to dynamically connect to different Flight servers:

```python
app = create_app(flight_location="grpc://my.flight.server:8815")
```

This makes it easy to deploy in environments where the Flight server location varies.

---

## 🔄 **Combining Multiple Lifespan Functions**

`create_app()` accepts multiple lifespan functions, enabling additional logic such as logging, database connections,
etc.

### **Example: Combining Custom Lifespan**

```python
from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastflight.fastapi.app import create_app


@asynccontextmanager
async def custom_lifespan(app: FastAPI):
    print("Custom lifespan start")
    yield
    print("Custom lifespan end")


app = create_app(route_prefix="/fastflight", flight_location="grpc://my.flight.server:8815", custom_lifespan)
```

On FastAPI startup and shutdown, the logs will display:

```
Custom lifespan start
Custom lifespan end
```

---

## 📂 **Project Structure**

```
fastflight/
│── fastapi/
│   │── __init__.py          # Exports core API components
│   │── app.py               # FastAPI application entry point
│   │── lifespan.py          # FastAPI lifespan management
│   │── router.py            # API endpoints
│   │── dependencies.py      # Dependency injection
│   └── README.md            # This file
```

---

## 🛠 **Future Improvements**

- ✅ Support dynamic route prefixes ✅
- ✅ Allow multiple lifespan combinations ✅
- ✅ Add configurable `flight_location` ✅
- ⏳ **Expand API endpoints & authentication support**

Contributions are welcome! Open a PR or issue to help improve FastFlight.
