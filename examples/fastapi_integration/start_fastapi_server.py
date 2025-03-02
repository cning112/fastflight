from fastapi import FastAPI
from mock_data_service import MockDataParams

from fastflight.fastapi.api_router import router as ff_router
from fastflight.fastapi.lifespan import combined_lifespan
from fastflight.utils.custom_logging import setup_logging

setup_logging()

__services__ = [MockDataParams]
app = FastAPI(lifespan=combined_lifespan)

app.include_router(ff_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
