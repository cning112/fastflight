from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class LoggingSettings(BaseSettings):
    log_level: str = "INFO"
    log_format: str = "plain" # or "json"
    model_config = SettingsConfigDict(env_prefix='FASTFLIGHT_LOGGING_')

class FlightServerSettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8815
    log_level: str = "INFO"
    auth_token: Optional[str] = None # For simple single-token auth
    # For multiple valid tokens, consider `valid_auth_tokens: list[str] = []`
    tls_server_cert_path: Optional[str] = None
    tls_server_key_path: Optional[str] = None
    model_config = SettingsConfigDict(env_prefix='FASTFLIGHT_SERVER_')

class FastAPISettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    flight_server_location: str = "grpc://localhost:8815" # Default if Flight server is local
    valid_api_keys: list[str] = [] # List of valid API keys for X-API-Key header
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    metrics_enabled: bool = True # For Prometheus metrics endpoint
    model_config = SettingsConfigDict(env_prefix='FASTFLIGHT_API_')

class BouncerSettings(BaseSettings):
    # TODO: Define resilience settings, e.g., max_retries, timeout
    pool_size: int = 10
    model_config = SettingsConfigDict(env_prefix='FASTFLIGHT_BOUNCER_')

# Global settings instances that can be imported and used by other modules.
# These will be loaded from environment variables or .env files.
logging_settings = LoggingSettings()
flight_server_settings = FlightServerSettings()
fastapi_settings = FastAPISettings()
bouncer_settings = BouncerSettings()
