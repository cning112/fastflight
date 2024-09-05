from fastapi import FastAPI

from demo.flight_service import load_all
from fastflight.utils.fastapi_utils.api_router import router as ff_router
from fastflight.utils.fastapi_utils.lifespan import combined_lifespan

load_all()
app = FastAPI(lifespan=combined_lifespan)

app.include_router(ff_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)