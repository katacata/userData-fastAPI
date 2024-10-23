from sqlalchemy import create_engine
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from fastapi import Depends

# Database configuration
DB_DRIVER = "mysql+pymysql"
DB_HOST = "sheauth-firebase_mysql_1"
DB_PORT = 3306
DB_NAME = "shecampaign"
DB_USER = "root"
DB_PASSWORD = "secret"

# Create URL object
# database_url = URL(
#     drivername=DB_DRIVER,
#     username=DB_USER,
#     password=DB_PASSWORD,
#     host=DB_HOST,
#     port=DB_PORT,
#     database=DB_NAME
# )

database_url = sa.engine.URL(DB_DRIVER, DB_USER, DB_PASSWORD, DB_HOST , DB_PORT, DB_NAME, {})

# Create engine and session
engine = create_engine(database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def initialize_database_schema():
    metadata = MetaData()

    # Define your models here
    from app.models import User

    # Create tables if they don't exist
    metadata.create_all(engine)

# @app.on_event("startup")
# async def startup_db():
#     # Initialize the database connection here
#     pass
#
# @app.on_event("shutdown")
# async def shutdown_db():
#     # Close the database connection here
#     pass
