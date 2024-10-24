from fastapi import FastAPI
from routers.process_directory_router import router as process_directory_router
from routers.get_she_member_router import router as get_she_member_router
from database import engine, SessionLocal, get_db

app = FastAPI()

app.include_router(process_directory_router)
app.include_router(get_she_member_router)

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