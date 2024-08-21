from os import PathLike
from pathlib import Path

from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    env: str = "local"
    log_file: PathLike = Path("./logs/app.log")
    console_log_level: str = "DEBUG"
    file_log_level: str = "DEBUG"
