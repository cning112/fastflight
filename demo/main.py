from fastapi import FastAPI

from demo.flight_service import load_all
from fastflight.utils.fastapi.api_router import router as ff_router
from fastflight.utils.fastapi.lifespan import combined_lifespan

# faulthandler.enable()
load_all()
app = FastAPI(lifespan=combined_lifespan)

app.include_router(ff_router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
