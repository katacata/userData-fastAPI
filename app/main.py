from fastapi import FastAPI
from routers.process_directory_router import router as process_directory_router
from routers.get_she_member_router import router as get_she_member_router
from routers.get_she_member_db_router import router as get_she_member_db_endpoint
from routers.get_campaigns_router import router as get_campaigns_endpoint
# from routers.init_db import router as init_db_endpoint

# from database import engine, SessionLocal, get_db

app = FastAPI()

app.include_router(get_she_member_db_endpoint)
app.include_router(process_directory_router)
app.include_router(get_she_member_router)
app.include_router(get_campaigns_endpoint)
# app.include_router(init_db_endpoint)


@app.get("/", tags=["root"])
async def root():
    return {"message": "Welcome to the API!"}

# @app.on_event("startup")
# async def startup():
#     startup_db()
#
# @app.on_event("shutdown")
# async def shutdown():
#     shutdown_db()