from fastapi import APIRouter, Depends
import pandas as pd
from datetime import datetime, date
import os
import re
from internal.process_directory import combine_directories
from config import settings
from auth import get_api_key  # Import the get_api_key function

router = APIRouter()

@router.get("/process-directory")
async def process_directory_endpoint(api_key: str = Depends(get_api_key)):
    return combine_directories()

# Include router in main app