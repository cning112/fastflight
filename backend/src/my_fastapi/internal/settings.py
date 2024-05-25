from os import PathLike

from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    env: str = "local"
    log_config_file: PathLike | None = None
    log_file: PathLike | None = None
    console_log_level: str = "INFO"
    file_log_level: str = "DEBUG"
