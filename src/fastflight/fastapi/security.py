from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN

from src.fastflight.config import fastapi_settings

API_KEY_NAME = "X-API-Key"
api_key_header_auth = APIKeyHeader(name=API_KEY_NAME, auto_error=False) # auto_error=False to allow custom error

async def get_api_key(api_key_header: str = Security(api_key_header_auth)):
    """
    Dependency to validate the API key from the X-API-Key header.
    Raises HTTPException if the key is missing or invalid.
    """
    if not fastapi_settings.valid_api_keys:
        # If no API keys are configured on the server, authentication is effectively disabled.
        # Depending on policy, could also deny all requests if keys are expected but list is empty.
        # For now, assume it means auth is optional/disabled.
        return None # Or some indicator that auth is not enforced

    if not api_key_header:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Not authenticated: X-API-Key header is missing."
        )
    
    if api_key_header not in fastapi_settings.valid_api_keys:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="Could not validate credentials: Invalid API Key."
        )
    return api_key_header # Return the key or a success indicator
