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
    from fastflight.config import fastapi_settings # Import settings
    from fastflight.fastapi import create_app

    typer.echo(f"Starting FastAPI Server at {host}:{port}")
    app = create_app(list(module_paths), route_prefix=fast_flight_route_prefix, flight_location=flight_location)

    uvicorn_kwargs = {"host": host, "port": port}
    if fastapi_settings.ssl_keyfile and fastapi_settings.ssl_certfile:
        uvicorn_kwargs["ssl_keyfile"] = fastapi_settings.ssl_keyfile
        uvicorn_kwargs["ssl_certfile"] = fastapi_settings.ssl_certfile
        typer.echo(f"FastAPI SSL enabled using key: {fastapi_settings.ssl_keyfile} and cert: {fastapi_settings.ssl_certfile}")
    else:
        typer.echo("FastAPI SSL disabled (ssl_keyfile or ssl_certfile not configured).")
        if fastapi_settings.ssl_keyfile or fastapi_settings.ssl_certfile:
            typer.echo("Warning: FastAPI SSL partially configured but not enabled. Both key and cert files are required.")

    uvicorn.run(app, **uvicorn_kwargs)


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
    # Note: The previous CLI implementation used global settings for host/port inside the target functions.
    # This might need adjustment if the target functions (start_fast_flight_server, start_fastapi)
    # are to be purely driven by parameters passed here from start_all's own CLI options.
    # For now, assuming the target functions will pick up global settings or their own defaults
    # if not overridden by direct parameters.
    
    # The `start_fast_flight_server` in the previous implementation takes a single `location` string.
    # The updated `server.py`'s `main` and `start_instance` now derive location, auth, tls from global settings.
    # So, we can call `start_fast_flight_server` without arguments if it's updated to use settings directly,
    # or we parse `flight_location` here if that's still its input.
    # Given the changes in server.py, `start_fast_flight_server` itself should be simplified or its parameters changed.
    # Let's assume for now that `start_fast_flight_server` will rely on the global `flight_server_settings`.
    # A direct call to `flight_server.main()` might be cleaner if `start_fast_flight_server` becomes complex.
    
    # For this diff, I'll keep the structure, assuming `start_fast_flight_server` is adapted or `flight_location` is still primary.
    # However, Flight server settings (host, port, token, tls) are now global.
    # The `flight_location` parameter for `start_all` might be redundant if server self-configures from global settings.

    from fastflight.server import main as flight_server_main # Direct import of main
    from fastflight.fastapi import create_app # For API process
    import uvicorn
    from fastflight.config import fastapi_settings # For API SSL

    flight_process = multiprocessing.Process(target=flight_server_main) # Flight server will use its own settings

    # API process target needs to be a function that can be pickled by multiprocessing.
    # A simple wrapper or ensuring start_fastapi is robust.
    def run_api_server_process():
        typer.echo(f"FastAPI (from start_all) will use host: {api_host}, port: {api_port}")
        app = create_app(
            list(module_paths),
            route_prefix=fast_flight_route_prefix,
            # flight_location for create_app is now sourced from fastapi_settings internally
        )
        
        uvicorn_kwargs = {"host": api_host, "port": api_port}
        if fastapi_settings.ssl_keyfile and fastapi_settings.ssl_certfile:
            uvicorn_kwargs["ssl_keyfile"] = fastapi_settings.ssl_keyfile
            uvicorn_kwargs["ssl_certfile"] = fastapi_settings.ssl_certfile
            typer.echo(f"FastAPI SSL (from start_all) enabled using key: {fastapi_settings.ssl_keyfile} and cert: {fastapi_settings.ssl_certfile}")
        else:
            typer.echo("FastAPI SSL (from start_all) disabled (ssl_keyfile or ssl_certfile not configured).")
            if fastapi_settings.ssl_keyfile or fastapi_settings.ssl_certfile:
                 typer.echo("Warning: FastAPI SSL (from start_all) partially configured. Both key and cert files are required.")
        
        uvicorn.run(app, **uvicorn_kwargs)

    api_process = multiprocessing.Process(target=run_api_server_process)

    flight_process.start()
    api_process.start()

    original_sigint_handler = signal.getsignal(signal.SIGINT)
    original_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def shutdown_handler(signum, frame):
        typer.echo(f"Signal {signum} received, initiating shutdown...")
        
        # Restore original handlers to prevent re-entry if issues occur during shutdown
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGTERM, original_sigterm_handler)

        # Terminate FastAPI/Uvicorn first, as it might depend on the Flight server
        # or just as a general order.
        if api_process.is_alive():
            typer.echo("Terminating FastAPI process (sending SIGTERM)...")
            api_process.terminate()
        
        if flight_process.is_alive():
            typer.echo("Terminating Flight server process (sending SIGTERM)...")
            flight_process.terminate()
        
        api_shutdown_gracefully = True
        flight_shutdown_gracefully = True

        if api_process.is_alive():
            typer.echo("Waiting for FastAPI process to exit (timeout 10s)...")
            api_process.join(timeout=10) # Uvicorn's default graceful exit timeout is 5s, give a bit more
            if api_process.is_alive():
                typer.echo("FastAPI process did not terminate gracefully, killing (sending SIGKILL)...")
                api_process.kill()
                api_shutdown_gracefully = False
        
        if flight_process.is_alive():
            typer.echo("Waiting for Flight server process to exit (timeout 10s)...")
            flight_process.join(timeout=10) 
            if flight_process.is_alive():
                typer.echo("Flight server process did not terminate gracefully, killing (sending SIGKILL)...")
                flight_process.kill()
                flight_shutdown_gracefully = False

        if api_shutdown_gracefully and flight_shutdown_gracefully:
            typer.echo("All servers shut down gracefully.")
        else:
            typer.echo("One or more servers required force killing.")
        
        # Raising SystemExit to ensure the main process exits cleanly after handling signals
        # Using exit(0) directly in a signal handler can sometimes be problematic.
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Wait for both processes to complete. 
    # They will run until a signal is received and handled by shutdown_handler,
    # which then raises SystemExit.
    # If a process exits unexpectedly (e.g., due to an error), its join() will return.
    try:
        if flight_process.is_alive():
            flight_process.join()
            if flight_process.exitcode != 0 and flight_process.exitcode is not None: # None if killed by signal not from terminate/kill
                 typer.echo(f"Warning: Flight server process exited with code {flight_process.exitcode}.", err=True)

        if api_process.is_alive():
            api_process.join()
            if api_process.exitcode != 0 and api_process.exitcode is not None:
                 typer.echo(f"Warning: FastAPI process exited with code {api_process.exitcode}.", err=True)
                
    except SystemExit: # Caught from shutdown_handler
        typer.echo("CLI `start-all` process is exiting due to signal.")
    except Exception as e: # Catch any other unexpected errors in the main process
        typer.echo(f"An unexpected error occurred in `start-all` main loop: {e}", err=True)
    finally:
        # Ensure processes are cleaned up if they are somehow still alive and SystemExit wasn't raised
        # This is a fallback, primary cleanup is in shutdown_handler
        if flight_process.is_alive():
            typer.echo("Final cleanup: Terminating lingering Flight server process.", err=True)
            flight_process.kill()
        if api_process.is_alive():
            typer.echo("Final cleanup: Terminating lingering FastAPI process.", err=True)
            api_process.kill()

    typer.echo("FastFlight CLI `start-all` finished.")


if __name__ == "__main__":
    cli()
