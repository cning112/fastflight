"""
FastFlight CLI module.

Provides command-line interface for managing FastFlight and REST API servers
with proper multiprocessing support and consistent parameter naming.
"""

import multiprocessing
import signal
import time
from functools import wraps
from typing import Annotated

import typer

from fastflight.utils.custom_logging import setup_logging

setup_logging(log_file=None)

cli = typer.Typer(help="FastFlight CLI - Manage FastFlight and REST API Servers")


def apply_paths(func):
    """Apply paths decorator to ensure proper module loading."""
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


# Module-level functions for multiprocessing compatibility
@apply_paths
def _start_flight_server(flight_location: str, modules: list[str]):
    """Start Flight server in a separate process."""
    from fastflight.server import FastFlightServer
    from fastflight.utils.registry_check import import_all_modules_in_package

    for module in modules:
        import_all_modules_in_package(module)

    print(f"Starting FastFlightServer at {flight_location}")
    FastFlightServer.start_instance(flight_location)


@apply_paths
def _start_rest_server(rest_host: str, rest_port: int, rest_prefix: str, flight_location: str, modules: list[str]):
    """Start REST server in a separate process."""
    import uvicorn

    from fastflight.fastapi_integration import create_app

    print(f"Starting REST API Server at {rest_host}:{rest_port}")
    app = create_app(modules, route_prefix=rest_prefix, flight_location=flight_location)
    uvicorn.run(app, host=rest_host, port=rest_port)


@cli.command()
def start_flight_server(
    flight_location: Annotated[str, typer.Option(help="Flight server location")] = "grpc://0.0.0.0:8815",
    modules: Annotated[
        list[str], typer.Option(help="Module paths to scan for parameter classes", show_default=True)
    ] = ("fastflight.demo_services",),  # type: ignore
):
    """
    Start the FastFlight server.

    Args:
        flight_location (str): The gRPC location of the Flight server (default: "grpc://0.0.0.0:8815").
        modules (list[str, ...]): Module paths to scan for parameter classes (default: ("fastflight.demo_services",)).
    """
    _start_flight_server(flight_location, list(modules))


@cli.command()
def start_rest_server(
    rest_host: Annotated[str, typer.Option(help="Host for REST API server")] = "0.0.0.0",
    rest_port: Annotated[int, typer.Option(help="Port for REST API server")] = 8000,
    rest_prefix: Annotated[str, typer.Option(help="Route prefix for REST API")] = "/fastflight",
    flight_location: Annotated[
        str, typer.Option(help="Flight server location that REST API will connect to")
    ] = "grpc://0.0.0.0:8815",
    modules: Annotated[
        list[str], typer.Option(help="Module paths to scan for parameter classes", show_default=True)
    ] = ("fastflight.demo_services",),  # type: ignore
):
    """
    Start the REST API server.

    Args:
        rest_host (str): Host address for the REST API server (default: "0.0.0.0").
        rest_port (int): Port for the REST API server (default: 8000).
        rest_prefix (str): Route prefix for REST API integration (default: "/fastflight").
        flight_location (str): The gRPC location of the Flight server that REST API will connect to (default: "grpc://0.0.0.0:8815").
        modules (list[str, ...]): Module paths to scan for parameter classes (default: ("fastflight.demo_services",)).
    """
    _start_rest_server(rest_host, rest_port, rest_prefix, flight_location, modules)


@cli.command()
def start_all(
    flight_location: Annotated[str, typer.Option(help="Flight server location")] = "grpc://0.0.0.0:8815",
    rest_host: Annotated[str, typer.Option(help="Host for REST API server")] = "0.0.0.0",
    rest_port: Annotated[int, typer.Option(help="Port for REST API server")] = 8000,
    rest_prefix: Annotated[str, typer.Option(help="Route prefix for REST API")] = "/fastflight",
    modules: Annotated[
        list[str], typer.Option(help="Module paths to scan for parameter classes", show_default=True)
    ] = ("fastflight.demo_services",),  # type: ignore
):
    """
    Start both FastFlight and REST API servers.

    Args:
        flight_location (str): The gRPC location of the Flight server (default: "grpc://0.0.0.0:8815").
        rest_host (str): Host address for the REST API server (default: "0.0.0.0").
        rest_port (int): Port for the REST API server (default: 8000).
        rest_prefix (str): Route prefix for REST API integration (default: "/fastflight").
        modules (list[str]): Module paths to scan for parameter classes (default: ("fastflight.demo_services",)).
    """
    # Create processes using module-level functions for multiprocessing compatibility
    flight_process = multiprocessing.Process(target=_start_flight_server, args=(flight_location, list(modules)))
    rest_process = multiprocessing.Process(
        target=_start_rest_server, args=(rest_host, rest_port, rest_prefix, flight_location, list(modules))
    )

    def shutdown_handler(signum, frame):  # noqa: ARG001
        """Handle shutdown signals gracefully."""
        typer.echo("Received termination signal. Shutting down servers...")
        flight_process.terminate()
        rest_process.terminate()
        flight_process.join(timeout=5)
        if flight_process.is_alive():
            flight_process.kill()
        rest_process.join(timeout=5)
        if rest_process.is_alive():
            rest_process.kill()
        typer.echo("Servers shut down cleanly.")
        exit(0)

    # Handle SIGINT (Ctrl+C) and SIGTERM
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        typer.echo(f"Starting FastFlight server at {flight_location}")
        typer.echo(f"Starting REST API server at {rest_host}:{rest_port}")
        typer.echo("Press Ctrl+C to stop both servers")

        flight_process.start()
        rest_process.start()

        while True:
            time.sleep(1)  # Keep main process running
    except KeyboardInterrupt:
        shutdown_handler(signal.SIGINT, None)


if __name__ == "__main__":
    cli()
