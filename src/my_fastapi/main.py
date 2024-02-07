from fastapi import FastAPI

from my_fastapi.utils.error_handling import setup_error_handling

from .routers import root

app = FastAPI()

app.include_router(root.router)

setup_error_handling(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, port=8000)
