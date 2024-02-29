from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    env: str = "local"
