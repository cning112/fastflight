from fastapi import APIRouter, FastAPI

from fastflight.utils.fastapi_utils.api_route import router as ff_router
from fastflight.utils.fastapi_utils.lifespan import combined_lifespan

from .routers import pd_data, root, ui_form
from .utils.error_handling import setup_error_handling

app = FastAPI(lifespan=combined_lifespan)

api_router = APIRouter(prefix="/api")
api_router.include_router(root.router)
api_router.include_router(pd_data.router)
api_router.include_router(ui_form.router)
api_router.include_router(ff_router)

app.include_router(api_router)

setup_error_handling(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
