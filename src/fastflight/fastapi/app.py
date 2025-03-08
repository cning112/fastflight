from typing import AsyncContextManager, Callable

from fastapi import FastAPI

from ..data_services.discovery import discover_param_classes
from .lifespan import combine_lifespans
from .router import fast_flight_router


def create_app(
    module_paths: list[str],
    route_prefix: str = "/fastflight",
    flight_location: str = "grpc://0.0.0.0:8815",
    *lifespans: Callable[[FastAPI], AsyncContextManager],
) -> FastAPI:
    registry = {}
    for mod in module_paths:
        registry.update(discover_param_classes(mod))

    app = FastAPI(lifespan=lambda a: combine_lifespans(a, registry, flight_location, *lifespans))
    app.include_router(fast_flight_router, prefix=route_prefix)
    return app
