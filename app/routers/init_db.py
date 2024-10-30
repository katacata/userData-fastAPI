from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from internal.init_db import init_db
from auth import get_api_key  # Import the get_api_key function


router = APIRouter()

@router.get("/init-db")
async def init_db_endpoint():
    members =  init_db()
    return JSONResponse(content=jsonable_encoder(members))

