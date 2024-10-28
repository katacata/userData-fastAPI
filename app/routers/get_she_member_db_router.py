from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from internal.get_she_member_db import get_she_member
from auth import get_api_key  # Import the get_api_key function


router = APIRouter()

@router.get("/get-she-member-db")
async def get_she_member_db_endpoint():
    members = get_she_member()
    return JSONResponse(content=jsonable_encoder(members))

