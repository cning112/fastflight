from fastapi import FastAPI
from mock_data_service import MockDataParams

from fastflight.fastapi.lifespan import combine_lifespans
from fastflight.fastapi.router import fast_flight_router as ff_router
from fastflight.utils.custom_logging import setup_logging

setup_logging()

__services__ = [MockDataParams]
app = FastAPI(lifespan=combine_lifespans)

app.include_router(ff_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
