import multiprocessing
import signal
import time
from functools import wraps
from typing import Annotated

import typer

cli = typer.Typer(help="FastFlight CLI - Manage FastFlight and FastAPI Servers")


def apply_paths(func):
    import os
    import sys

    # Add current working directory to sys.path
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    # Add paths from PYTHONPATH environment variable
    py_path = os.environ.get("PYTHONPATH")
    if py_path:
        for path in py_path.split(os.pathsep):
            if path and path not in sys.path:
                sys.path.insert(0, path)

    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@cli.command()
@apply_paths
def start_fast_flight_server(
    location: Annotated[str, typer.Argument(help="Flight server location")] = "grpc://0.0.0.0:8815",
):
    """
    Start the FastFlight server.

    Args:
        location (str): The gRPC location of the Flight server (default: "grpc://0.0.0.0:8815").
    """

    from fastflight.server import FastFlightServer

    typer.echo(f"Starting FastFlightServer at {location}")
    FastFlightServer.start_instance(location)


@cli.command()
@apply_paths
def start_fastapi(
    host: Annotated[str, typer.Option(help="Host for FastAPI server")] = "0.0.0.0",
    port: Annotated[int, typer.Option(help="Port for FastAPI server")] = 8000,
    fast_flight_route_prefix: Annotated[
        str, typer.Option(help="Route prefix for FastFlight API integration")
    ] = "/fastflight",
    flight_location: Annotated[
        str, typer.Option(help="Flight server location that FastAPI will connect to")
    ] = "grpc://0.0.0.0:8815",
    module_paths: Annotated[
        list[str], typer.Option(help="Module paths to scan for parameter classes", show_default=True)
    ] = ("fastflight.demo_services",),  # type: ignore
):
    """
    Start the FastAPI server.

    Args:
        host (str): Host address for the FastAPI server (default: "0.0.0.0").
        port (int): Port for the FastAPI server (default: 8000).
        fast_flight_route_prefix (str): API route prefix for FastFlight integration (default: "/fastflight").
        flight_location (str): The gRPC location of the Flight server that FastAPI will connect to (default: "grpc://0.0.0.0:8815").
        module_paths (list[str, ...]): Module paths to scan for parameter classes (default: ("fastflight.demo_services",)).

    """
    import uvicorn

    from fastflight.fastapi import create_app

    typer.echo(f"Starting FastAPI Server at {host}:{port}")
    app = create_app(list(module_paths), route_prefix=fast_flight_route_prefix, flight_location=flight_location)
    uvicorn.run(app, host=host, port=port)


@cli.command()
@apply_paths
def start_all(
    api_host: Annotated[str, typer.Option(help="Host for FastAPI server")] = "0.0.0.0",
    api_port: Annotated[int, typer.Option(help="Port for FastAPI server")] = 8000,
    fast_flight_route_prefix: Annotated[
        str, typer.Option(help="Route prefix for FastFlight API integration")
    ] = "/fastflight",
    flight_location: Annotated[
        str, typer.Option(help="Flight server location that FastAPI will connect to")
    ] = "grpc://0.0.0.0:8815",
    module_paths: Annotated[
        list[str], typer.Option(help="Module paths to scan for parameter classes", show_default=True)
    ] = ("fastflight.demo_services",),  # type: ignore
):
    """
    Start both FastFlight and FastAPI servers.

    Args:
        api_host (str): Host address for the FastAPI server (default: "0.0.0.0").
        api_port (int): Port for the FastAPI server (default: 8000).
        fast_flight_route_prefix (str): API route prefix for FastFlight integration (default: "/fastflight").
        flight_location (str): The gRPC location of the Flight server (default: "grpc://0.0.0.0:8815").
        module_paths (list[str]): Module paths to scan for parameter classes (default: ("fastflight.demo_services",)).
    """
    # Create processes
    flight_process = multiprocessing.Process(target=start_fast_flight_server, args=(flight_location,))
    api_process = multiprocessing.Process(
        target=start_fastapi, args=(api_host, api_port, fast_flight_route_prefix, flight_location, module_paths)
    )

    flight_process.start()
    api_process.start()

    def shutdown_handler(signum, frame):
        typer.echo("Received termination signal. Shutting down servers...")
        flight_process.terminate()
        api_process.terminate()
        flight_process.join(timeout=5)
        if flight_process.is_alive():
            flight_process.kill()
        api_process.join(timeout=5)
        if api_process.is_alive():
            api_process.kill()
        typer.echo("Servers shut down cleanly.")
        exit(0)

    # Handle SIGINT (Ctrl+C) and SIGTERM
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        while True:
            time.sleep(1)  # Keep main process running
    except KeyboardInterrupt:
        shutdown_handler(signal.SIGINT, None)


if __name__ == "__main__":
    cli()
