import os
from functools import cache
from pathlib import Path

from my_fastapi.internal.settings import AppSettings


@cache
def get_app_settings(env: str | None = None) -> AppSettings:
    env = env or os.getenv("ENV", "local")
    env_file = Path("config") / f"{env}.env"
    return AppSettings(_env_file=env_file)
