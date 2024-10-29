from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from internal.get_campaigns import combine_directories
from auth import get_api_key  # Import the get_api_key function


router = APIRouter()

@router.get("/get-campaigns")
async def get_campaigns_endpoint():
    members =  combine_directories()
    return JSONResponse(content=jsonable_encoder(members))

