from typing import AsyncContextManager, Callable

from fastapi import FastAPI

from ..utils.registry_check import get_param_service_bindings_from_package, import_all_modules_in_package
from .lifespan import combine_lifespans
from .router import fast_flight_router


def create_app(
    module_paths: list[str],
    route_prefix: str = "/fastflight",
    flight_location: str = "grpc://0.0.0.0:8815",
    *lifespans: Callable[[FastAPI], AsyncContextManager],
) -> FastAPI:
    # Import all custom data parameter and service classes, and check if they are registered
    registered_data_types = {}
    for mod in module_paths:
        import_all_modules_in_package(mod)
        registered_data_types.update(get_param_service_bindings_from_package(mod))

    app = FastAPI(lifespan=lambda a: combine_lifespans(a, registered_data_types, flight_location, *lifespans))
    app.include_router(fast_flight_router, prefix=route_prefix)
    return app
