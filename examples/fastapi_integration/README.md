# Demo of FastFlight: FastAPI + Arrow Flight

This is a demo of how to run a FastAPI server that uses FastFlight to serve Arrow Flight data.

# Steps

1. Start the FastAPI server and the FastFlight server.

```
python start_fastapi_server.py
```

and

```
python start_flight_server.py
```

2. Run the demo of getting data from the flight server via HTTP request

```
python demo_http_requests.py
```

3. Run the demo of getting data directly from flight server

```
python demo_grpc_requests.py
```
