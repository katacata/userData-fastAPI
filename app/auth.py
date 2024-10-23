from fastapi import HTTPException, Security
from fastapi.security import APIKeyQuery
from config import settings

API_KEY_QUERY = APIKeyQuery(name="key")

async def get_api_key(api_key: str = Security(API_KEY_QUERY)):
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key
