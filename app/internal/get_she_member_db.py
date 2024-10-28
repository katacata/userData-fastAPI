import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import re
from config import settings
from itertools import islice
from pydantic import BaseModel, ValidationError
from typing import List, Optional
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, MetaData, Table, Boolean, insert, select, delete
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import Depends
from typing import List
from config import Settings

Base = declarative_base()

# Create URL object
database_url = sa.engine.URL(Settings.DB_DRIVER, Settings.DB_USER, Settings.DB_PASSWORD,
    Settings.DB_HOST, Settings.DB_PORT, Settings.DB_SHEMEMBER_NAME, {})

# Create engine and session
engine = create_engine(database_url)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

from sqlalchemy import MetaData, Table, select, text
from sqlalchemy.engine import Engine

def get_she_member():
    metadata = MetaData()

    try:
        conn = engine.connect()

        # Reflect the table schema
        metadata.reflect(bind=engine)

        # Get the reflected table
        users_table = metadata.tables['users']

        # Select all columns from the users table
        stmt = select(users_table)

        # Execute the query
        result = conn.execute(stmt)
        data = pd.DataFrame(result)
        print(data)

        # Fetch all rows
        rows = result.fetchall()

        # Print the results
        print("Table contents:")
#         for row in rows:
#             print(row)
        print(rows)

        # Close the connection
        conn.close()
        print("Connection closed")

        # Return the table representation
        return {"table_info": str(users_table)}

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {"error": str(e)}


__all__ = ["get_she_member"]