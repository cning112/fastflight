from fastapi import APIRouter, FastAPI
from my_fastapi.utils.error_handling import setup_error_handling
from .routers import pd_data, root, ui_form

app = FastAPI()

api_router = APIRouter(prefix="/api")
api_router.include_router(root.router)
api_router.include_router(pd_data.router)
api_router.include_router(ui_form.router)

app.include_router(api_router)

setup_error_handling(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
