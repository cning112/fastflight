from fastapi import APIRouter, FastAPI

from .routers import arrow_flight, pd_data, root, ui_form
from .utils.error_handling import setup_error_handling

app = FastAPI()

api_router = APIRouter(prefix="/api")
api_router.include_router(root.router)
api_router.include_router(pd_data.router)
api_router.include_router(ui_form.router)
api_router.include_router(arrow_flight.router)

app.include_router(api_router)

setup_error_handling(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
