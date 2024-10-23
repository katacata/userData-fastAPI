from fastapi import Depends
from sqlalchemy.orm import Session
from .database import engine, initialize_database_schema

def init_db():
    initialize_database_schema()
    return SessionLocal()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
