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

from fastflight.resilience.config.resilience import ResilienceConfig
from fastflight.resilience.config_builder.factory import ResilienceConfigFactory
from fastflight.resilience.config_builder.types import ResiliencePreset
from fastflight.resilience.types import RetryStrategy
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
def _start_rest_server(
    rest_host: str,
    rest_port: int,
    rest_prefix: str,
    flight_location: str,
    modules: list[str],
    resilience_config: ResilienceConfig | None = None,
):
    """Start REST server in a separate process."""
    import uvicorn

    from fastflight.fastapi_integration import create_app

    print(f"Starting REST API Server at {rest_host}:{rest_port}")
    if resilience_config:
        print(f"Using resilience configuration for FastFlightBouncer: {resilience_config}")
    app = create_app(
        modules, route_prefix=rest_prefix, flight_location=flight_location, resilience_config=resilience_config
    )
    uvicorn.run(app, host=rest_host, port=rest_port)


@cli.command()
def start_flight_server(
    flight_location: Annotated[
        str, typer.Option(help="Flight server location (grpc://host:port format)")
    ] = "grpc://0.0.0.0:8815",  # nosec B104
    modules: Annotated[
        list[str], typer.Option(help="Module paths to scan for parameter classes", show_default=True)
    ] = ("fastflight.demo_services",),  # type: ignore
):
    """
    Start the FastFlight server.

    The FastFlight server is a pure Arrow Flight server that handles data requests.
    It does not use resilience configuration as these are client-side concerns
    handled by FastFlightBouncer.

    Args:
        flight_location (str): The gRPC location of the Flight server (default: "grpc://0.0.0.0:8815").
        modules (list[str, ...]): Module paths to scan for parameter classes
            (default: ("fastflight.demo_services",)).

    Note:
        For resilience features (retry, circuit breaker), configure your FastFlightBouncer
        clients or use the REST API server which includes built-in resilience.
    """
    _start_flight_server(flight_location, list(modules))


@cli.command()
def start_rest_server(
    rest_host: Annotated[
        str,
        typer.Option(help="Host for REST API server (use 127.0.0.1 for localhost only, 0.0.0.0 for all interfaces)"),
    ] = "0.0.0.0",  # nosec B104
    rest_port: Annotated[int, typer.Option(help="Port for REST API server")] = 8000,
    rest_prefix: Annotated[str, typer.Option(help="Route prefix for REST API")] = "/fastflight",
    flight_location: Annotated[
        str, typer.Option(help="Flight server location that REST API will connect to")
    ] = "grpc://0.0.0.0:8815",  # nosec B104
    modules: Annotated[
        list[str], typer.Option(help="Module paths to scan for parameter classes", show_default=True)
    ] = ("fastflight.demo_services",),  # type: ignore
    # Resilience configuration options
    resilience_preset: Annotated[
        ResiliencePreset, typer.Option(help="Resilience configuration preset")
    ] = ResiliencePreset.DEFAULT,
    retry_max_attempts: Annotated[
        int | None, typer.Option(help="Maximum retry attempts (1-100)", min=1, max=100)
    ] = None,
    retry_strategy: Annotated[RetryStrategy | None, typer.Option(help="Retry strategy")] = None,
    retry_base_delay: Annotated[
        float | None, typer.Option(help="Base delay for retries in seconds", min=0.1, max=300.0)
    ] = None,
    retry_max_delay: Annotated[
        float | None, typer.Option(help="Maximum delay for retries in seconds", min=0.1, max=3600.0)
    ] = None,
    circuit_breaker_failure_threshold: Annotated[
        int | None, typer.Option(help="Circuit breaker failure threshold (1-1000)", min=1, max=1000)
    ] = None,
    circuit_breaker_recovery_timeout: Annotated[
        float | None, typer.Option(help="Circuit breaker recovery timeout in seconds", min=1.0, max=3600.0)
    ] = None,
    circuit_breaker_success_threshold: Annotated[
        int | None, typer.Option(help="Circuit breaker success threshold (1-100)", min=1, max=100)
    ] = None,
    operation_timeout: Annotated[
        float | None, typer.Option(help="Operation timeout in seconds", min=1.0, max=3600.0)
    ] = None,
    enable_circuit_breaker: Annotated[bool, typer.Option(help="Enable circuit breaker functionality")] = True,
    circuit_breaker_name: Annotated[
        str | None, typer.Option(help="Circuit breaker name (alphanumeric, underscore, dash only)")
    ] = None,
):
    """
    Start the REST API server.

    The REST API server uses FastFlightBouncer internally to connect to Flight servers.
    Resilience configuration is applied to this internal bouncer, providing retry and
    circuit breaker protection for REST API requests.

    Args:
        rest_host (str): Host address for the REST API server (default: "0.0.0.0").
        rest_port (int): Port for the REST API server (default: 8000).
        rest_prefix (str): Route prefix for REST API integration (default: "/fastflight").
        flight_location (str): The gRPC location of the Flight server that REST API will
            connect to (default: "grpc://0.0.0.0:8815").
        modules (list[str, ...]): Module paths to scan for parameter classes
            (default: ("fastflight.demo_services",)).
        resilience_preset (ResiliencePreset): Resilience configuration preset -
            controls the internal bouncer's behavior.
        retry_max_attempts (int, optional): Override maximum retry attempts.
        retry_strategy (RetryStrategy, optional): Override retry strategy.
        retry_base_delay (float, optional): Override base delay for retries.
        retry_max_delay (float, optional): Override maximum delay for retries.
        circuit_breaker_failure_threshold (int, optional): Override circuit breaker failure threshold.
        circuit_breaker_recovery_timeout (float, optional): Override circuit breaker recovery timeout.
        circuit_breaker_success_threshold (int, optional): Override circuit breaker success threshold.
        operation_timeout (float, optional): Override operation timeout.
        enable_circuit_breaker (bool): Enable or disable circuit breaker functionality.
        circuit_breaker_name (str, optional): Custom circuit breaker name.

    Resilience Presets:
        - disabled: No resilience features enabled in the internal bouncer
        - default: Balanced retry and circuit breaker settings for general use
        - high_availability: Aggressive retries with fast circuit breaker for HA scenarios
        - batch_processing: Conservative retries with tolerant circuit breaker for batch jobs
    """
    # Create resilience configuration using the modern factory
    resilience_config = ResilienceConfigFactory.create_for_cli(
        preset=resilience_preset,
        retry_max_attempts=retry_max_attempts,
        retry_strategy=retry_strategy,
        retry_base_delay=retry_base_delay,
        retry_max_delay=retry_max_delay,
        circuit_breaker_failure_threshold=circuit_breaker_failure_threshold,
        circuit_breaker_recovery_timeout=circuit_breaker_recovery_timeout,
        circuit_breaker_success_threshold=circuit_breaker_success_threshold,
        operation_timeout=operation_timeout,
        enable_circuit_breaker=enable_circuit_breaker,
        circuit_breaker_name=circuit_breaker_name,
    )

    _start_rest_server(rest_host, rest_port, rest_prefix, flight_location, list(modules), resilience_config)


@cli.command()
def start_all(
    flight_location: Annotated[
        str, typer.Option(help="Flight server location (grpc://host:port format)")
    ] = "grpc://0.0.0.0:8815",  # nosec B104
    rest_host: Annotated[
        str,
        typer.Option(help="Host for REST API server (use 127.0.0.1 for localhost only, 0.0.0.0 for all interfaces)"),
    ] = "0.0.0.0",  # nosec B104
    rest_port: Annotated[int, typer.Option(help="Port for REST API server")] = 8000,
    rest_prefix: Annotated[str, typer.Option(help="Route prefix for REST API")] = "/fastflight",
    modules: Annotated[
        list[str], typer.Option(help="Module paths to scan for parameter classes", show_default=True)
    ] = ("fastflight.demo_services",),  # type: ignore
    # Resilience configuration options (only affects REST server)
    resilience_preset: Annotated[
        ResiliencePreset, typer.Option(help="Resilience configuration preset for REST server's internal bouncer")
    ] = ResiliencePreset.DEFAULT,
    retry_max_attempts: Annotated[
        int | None, typer.Option(help="Maximum retry attempts (1-100)", min=1, max=100)
    ] = None,
    retry_strategy: Annotated[RetryStrategy | None, typer.Option(help="Retry strategy")] = None,
    retry_base_delay: Annotated[
        float | None, typer.Option(help="Base delay for retries in seconds", min=0.1, max=300.0)
    ] = None,
    retry_max_delay: Annotated[
        float | None, typer.Option(help="Maximum delay for retries in seconds", min=0.1, max=3600.0)
    ] = None,
    circuit_breaker_failure_threshold: Annotated[
        int | None, typer.Option(help="Circuit breaker failure threshold (1-1000)", min=1, max=1000)
    ] = None,
    circuit_breaker_recovery_timeout: Annotated[
        float | None, typer.Option(help="Circuit breaker recovery timeout in seconds", min=1.0, max=3600.0)
    ] = None,
    circuit_breaker_success_threshold: Annotated[
        int | None, typer.Option(help="Circuit breaker success threshold (1-100)", min=1, max=100)
    ] = None,
    operation_timeout: Annotated[
        float | None, typer.Option(help="Operation timeout in seconds", min=1.0, max=3600.0)
    ] = None,
    enable_circuit_breaker: Annotated[bool, typer.Option(help="Enable circuit breaker functionality")] = True,
    circuit_breaker_name: Annotated[
        str | None, typer.Option(help="Circuit breaker name (alphanumeric, underscore, dash only)")
    ] = None,
):
    """
    Start both FastFlight and REST API servers.

    This command starts two separate processes:
    1. FastFlightServer - Pure Arrow Flight server (no resilience configuration)
    2. REST API Server - FastAPI server with internal FastFlightBouncer (with resilience)

    Args:
        flight_location (str): The gRPC location of the Flight server (default: "grpc://0.0.0.0:8815").
        rest_host (str): Host address for the REST API server (default: "0.0.0.0").
        rest_port (int): Port for the REST API server (default: 8000).
        rest_prefix (str): Route prefix for REST API integration (default: "/fastflight").
        modules (list[str]): Module paths to scan for parameter classes (default: ("fastflight.demo_services",)).
        resilience_preset (ResiliencePreset): Resilience configuration preset -
            affects REST server's internal bouncer only.
        retry_max_attempts (int, optional): Override maximum retry attempts (REST server only).
        retry_strategy (RetryStrategy, optional): Override retry strategy (REST server only).
        retry_base_delay (float, optional): Override base delay for retries (REST server only).
        retry_max_delay (float, optional): Override maximum delay for retries (REST server only).
        circuit_breaker_failure_threshold (int, optional): Override circuit breaker
            failure threshold (REST server only).
        circuit_breaker_recovery_timeout (float, optional): Override circuit breaker
            recovery timeout (REST server only).
        circuit_breaker_success_threshold (int, optional): Override circuit breaker
            success threshold (REST server only).
        operation_timeout (float, optional): Override operation timeout (REST server only).
        enable_circuit_breaker (bool): Enable or disable circuit breaker functionality (REST server only).
        circuit_breaker_name (str, optional): Custom circuit breaker name (REST server only).

    Architecture:
        REST Client → REST API (with bouncer + resilience) → Flight Server (raw)

    Important:
        - Resilience configuration ONLY applies to the REST server's internal bouncer
        - The Flight server itself does not use any resilience configuration
        - Direct Flight clients should configure their own FastFlightBouncer instances

    Resilience Presets:
        - disabled: No resilience features enabled in the REST server's bouncer
        - default: Balanced retry and circuit breaker settings for general use
        - high_availability: Aggressive retries with fast circuit breaker for HA scenarios
        - batch_processing: Conservative retries with tolerant circuit breaker for batch jobs
    """
    # Create resilience configuration using the modern factory
    resilience_config = ResilienceConfigFactory.create_for_cli(
        preset=resilience_preset,
        retry_max_attempts=retry_max_attempts,
        retry_strategy=retry_strategy,
        retry_base_delay=retry_base_delay,
        retry_max_delay=retry_max_delay,
        circuit_breaker_failure_threshold=circuit_breaker_failure_threshold,
        circuit_breaker_recovery_timeout=circuit_breaker_recovery_timeout,
        circuit_breaker_success_threshold=circuit_breaker_success_threshold,
        operation_timeout=operation_timeout,
        enable_circuit_breaker=enable_circuit_breaker,
        circuit_breaker_name=circuit_breaker_name,
    )

    # Create processes using module-level functions for multiprocessing compatibility
    flight_process = multiprocessing.Process(target=_start_flight_server, args=(flight_location, list(modules)))
    rest_process = multiprocessing.Process(
        target=_start_rest_server,
        args=(rest_host, rest_port, rest_prefix, flight_location, list(modules), resilience_config),
    )

    def shutdown_handler(_signum, _frame):
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
        if resilience_config:
            typer.echo(f"Using resilience preset: {resilience_preset}")
            typer.echo(f"Estimated max operation time: {resilience_config.estimated_max_operation_time:.2f}s")
        typer.echo("Press Ctrl+C to stop both servers")

        flight_process.start()
        rest_process.start()

        while True:
            time.sleep(1)  # Keep main process running
    except KeyboardInterrupt:
        shutdown_handler(signal.SIGINT, None)


@cli.command()
def show_resilience_config(
    preset: Annotated[
        ResiliencePreset, typer.Option(help="Resilience configuration preset to display")
    ] = ResiliencePreset.DEFAULT,
):
    """
    Display detailed information about resilience configuration presets.

    This command shows the specific retry and circuit breaker settings for each preset,
    helping users understand the behavior and choose appropriate configurations.

    Args:
        preset (str): The resilience preset to display information for.
    """
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    console = Console()

    if preset == ResiliencePreset.DISABLED:
        console.print(Panel.fit("Resilience features are disabled", style="red"))
        return

    # Create the configuration using the modern factory
    config = ResilienceConfigFactory.create_preset(preset)

    if config is None:
        console.print(Panel.fit("No resilience configuration", style="red"))
        return

    # Create main table
    table = Table(title=f"Resilience Configuration: {preset.upper()}")
    table.add_column("Component", style="cyan", no_wrap=True)
    table.add_column("Setting", style="magenta")
    table.add_column("Value", style="green")
    table.add_column("Description", style="white")

    # Add general settings
    table.add_row(
        "General", "Circuit Breaker Enabled", str(config.enable_circuit_breaker), "Whether circuit breaker is active"
    )
    table.add_row(
        "General", "Circuit Breaker Name", config.circuit_breaker_name or "N/A", "Identifier for this circuit breaker"
    )
    table.add_row(
        "General",
        "Operation Timeout",
        f"{config.operation_timeout}s" if config.operation_timeout else "N/A",
        "Maximum time for a single operation",
    )
    table.add_row(
        "General",
        "Est. Max Operation Time",
        f"{config.estimated_max_operation_time:.2f}s",
        "Total time including retries and recovery",
    )

    # Add retry configuration if available
    if config.retry_config:
        retry = config.retry_config
        table.add_row("Retry", "Max Attempts", str(retry.max_attempts), "Maximum number of retry attempts")
        table.add_row("Retry", "Strategy", retry.strategy, "Backoff strategy for retries")
        table.add_row("Retry", "Base Delay", f"{retry.base_delay}s", "Initial delay between retries")
        table.add_row("Retry", "Max Delay", f"{retry.max_delay}s", "Maximum delay between retries")
        table.add_row("Retry", "Exponential Base", str(retry.exponential_base), "Multiplier for exponential backoff")
        table.add_row("Retry", "Jitter Factor", str(retry.jitter_factor), "Randomization factor for delays")
        table.add_row("Retry", "Total Max Delay", f"{retry.total_max_delay:.2f}s", "Maximum total time spent retrying")
    else:
        table.add_row("Retry", "Status", "DISABLED", "Retry functionality is disabled")

    # Add circuit breaker configuration if available
    if config.circuit_breaker_config and config.enable_circuit_breaker:
        cb = config.circuit_breaker_config
        table.add_row(
            "Circuit Breaker", "Failure Threshold", str(cb.failure_threshold), "Failures before opening circuit"
        )
        table.add_row(
            "Circuit Breaker", "Recovery Timeout", f"{cb.recovery_timeout}s", "Time before attempting recovery"
        )
        table.add_row(
            "Circuit Breaker", "Success Threshold", str(cb.success_threshold), "Successes needed to close circuit"
        )
        table.add_row("Circuit Breaker", "Operation Timeout", f"{cb.timeout}s", "Timeout for individual operations")
        table.add_row(
            "Circuit Breaker", "Max Recovery Time", f"{cb.max_recovery_time:.2f}s", "Maximum time for full recovery"
        )
    else:
        table.add_row("Circuit Breaker", "Status", "DISABLED", "Circuit breaker functionality is disabled")

    console.print(table)

    # Add usage examples
    usage_panel = Panel(
        f"""[bold]Usage Examples:[/bold]

[cyan]Start with this preset:[/cyan]
fastflight start-all --resilience-preset {preset.value}

[cyan]Customize retry attempts:[/cyan]
fastflight start-flight-server --resilience-preset {preset.value} --retry-max-attempts 10

[cyan]Override circuit breaker threshold:[/cyan]
fastflight start-rest-server --resilience-preset {preset.value} --circuit-breaker-failure-threshold 3

[cyan]Disable circuit breaker but keep retries:[/cyan]
fastflight start-all --resilience-preset {preset.value} --enable-circuit-breaker false
        """,
        title="Usage Examples",
    )
    console.print(usage_panel)

    # Add preset descriptions
    descriptions = {
        ResiliencePreset.DEFAULT: "Balanced settings suitable for most production "
        "environments. Provides reasonable fault tolerance without being overly aggressive.",
        ResiliencePreset.HIGH_AVAILABILITY: "Optimized for high-availability "
        "scenarios where quick recovery and aggressive retries are prioritized over resource conservation.",
        ResiliencePreset.BATCH_PROCESSING: "Conservative settings designed for "
        "batch processing where tolerance for failures is higher and resource efficiency is important.",
    }

    if preset in descriptions:
        desc_panel = Panel(descriptions[preset], title=f"{preset.value.upper()} Preset Description")
        console.print(desc_panel)


@cli.command()
def list_resilience_presets():
    """
    List all available resilience configuration presets with brief descriptions.

    Note:
        Resilience configuration only applies to the client-side FastFlightBouncer,
        such as within the REST API server. The pure Flight server does not apply
        any resilience logic.

    This command provides an overview of all resilience presets available in FastFlight,
    helping users choose the most appropriate configuration for their use case.
    """
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    console = Console()

    table = Table(title="Available Resilience Presets")
    table.add_column("Preset", style="cyan", no_wrap=True)
    table.add_column("Use Case", style="magenta")
    table.add_column("Retry Behavior", style="green")
    table.add_column("Circuit Breaker", style="yellow")

    table.add_row("disabled", "Testing, Development", "None", "None")
    table.add_row("default", "General Production", "3 attempts, exponential backoff", "5 failures, 30s recovery")
    table.add_row(
        "high_availability", "Critical Services", "5 attempts, jittered exponential", "3 failures, 15s recovery"
    )
    table.add_row("batch_processing", "Batch Jobs, ETL", "2 attempts, fixed delay", "10 failures, 60s recovery")

    console.print(table)

    # Add explanatory panel about scope of resilience configuration
    console.print(
        Panel.fit(
            "[bold yellow]Note:[/bold yellow] Resilience configuration applies only to the [cyan]client-side[/cyan] "
            "FastFlightBouncer,\n"
            "such as within the REST API server. The [red]Flight server[/red] does not use resilience logic.",
            title="Resilience Scope",
            style="bold white",
        )
    )

    console.print("\n[bold]Next Steps:[/bold]")
    console.print(
        "• Use [cyan]fastflight show-resilience-config --preset PRESET_NAME[/cyan] for detailed configuration"
    )
    console.print("• Use [cyan]fastflight start-all --resilience-preset PRESET_NAME[/cyan] to apply a preset")
    console.print(
        "• Override individual settings with specific options "
        "(e.g., --retry-max-attempts, --circuit-breaker-failure-threshold)"
    )


if __name__ == "__main__":
    cli()
