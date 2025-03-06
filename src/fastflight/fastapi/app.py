from typing import AsyncContextManager, Callable

from fastapi import FastAPI

from .lifespan import combine_lifespans
from .router import fast_flight_router


def create_app(
    route_prefix: str = "/fastflight",
    flight_location: str = "grpc://0.0.0.0:8815",
    *lifespans: Callable[[FastAPI], AsyncContextManager],
) -> FastAPI:
    app = FastAPI(lifespan=lambda a: combine_lifespans(a, flight_location, *lifespans))
    app.include_router(fast_flight_router, prefix=route_prefix)
    return app
