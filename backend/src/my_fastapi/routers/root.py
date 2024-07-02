import logging
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends

from ..dependencies.settings import AppSettings, get_app_settings

router = APIRouter()

logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)
log = logging.getLogger(__name__)


@router.get("/")
async def root():
    return {"message": "Hello World"}


@router.get("/hello/{name}")
async def say_hello(name: str):
    with structlog.contextvars.bound_contextvars(hello_name=name):
        logger.info("structlog says hello to %s", name)
        log.info("logging says hello  to " + name)
    return {"message": f"Hello {name}"}


@router.get("/settings", response_model=AppSettings)
async def get_settings(settings: Annotated[AppSettings, Depends(get_app_settings)]):
    return settings
