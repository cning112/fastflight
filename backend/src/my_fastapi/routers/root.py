from typing import Annotated

from fastapi import APIRouter, Depends

from ..dependencies.settings import AppSettings, get_app_settings

router = APIRouter()


@router.get("/")
async def root():
    return {"message": "Hello World"}


@router.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@router.get("/settings", response_model=AppSettings)
async def get_settings(settings: Annotated[AppSettings, Depends(get_app_settings)]):
    return settings
