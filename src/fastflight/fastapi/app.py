from typing import AsyncContextManager, Callable

from fastapi import FastAPI
# Assuming starlette_prometheus is installed
from starlette_prometheus import exposição_métrica, PrometheusMiddleware

from src.fastflight.config import fastapi_settings
from src.fastflight.utils.custom_logging import setup_logging
from src.fastflight.utils.registry_check import get_param_service_bindings_from_package, import_all_modules_in_package
# Import custom FastAPI metrics if starlette-prometheus doesn't cover everything or for specific needs.
# from src.fastflight.metrics import fastapi_requests_total, fastapi_request_duration_seconds

from .lifespan import combine_lifespans
from .router import fast_flight_router


def create_app(
    module_paths: list[str],
    route_prefix: str = "/fastflight",
    # flight_location is now sourced from fastapi_settings
    *lifespans: Callable[[FastAPI], AsyncContextManager],
) -> FastAPI:
    # Setup logging for the FastAPI application
    setup_logging(service_name="FastAPIApp")

    # Import all custom data parameter and service classes
    registered_data_types = {}
    for mod in module_paths:
        import_all_modules_in_package(mod)
        registered_data_types.update(get_param_service_bindings_from_package(mod))

    app = FastAPI(
        lifespan=lambda app_instance: combine_lifespans(
            app_instance,
            registered_data_types,
            fastapi_settings.flight_server_location, # Use settings here
            *lifespans,
        )
    )

    # Add Prometheus middleware and metrics endpoint if enabled
    if fastapi_settings.metrics_enabled:
        app.add_middleware(PrometheusMiddleware)
        app.add_route("/metrics", exposição_métrica)
        # Note: starlette-prometheus provides its own set of default metrics like
        # starlette_requests_total, starlette_request_duration_seconds.
        # If we defined fastapi_requests_total etc. in metrics.py for manual instrumentation,
        # they would be separate unless we disable starlette-prometheus's default ones and use ours.
        # For this task, we'll rely on starlette-prometheus for FastAPI specific HTTP metrics.
        # Our custom metrics defined in metrics.py (like bouncer, flight_server) will also be exposed
        # via the same /metrics endpoint because prometheus_client uses a global registry by default.

    app.include_router(fast_flight_router, prefix=route_prefix)
    return app
