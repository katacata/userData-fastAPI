import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import re
from collections import defaultdict
from config import settings
from itertools import islice
from pydantic import BaseModel, ValidationError
from typing import List, Optional
from sqlalchemy import create_engine, Column, inspect, Integer, String, ForeignKey, MetaData, Table, Boolean, insert, select, delete, text
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
    Settings.DB_HOST, Settings.DB_PORT, Settings.DB_CAMPAIGN_NAME, {})

# Create engine and session
engine = create_engine(database_url)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def process_directory(directory_path):
    csv_files = sorted([file for file in os.listdir(directory_path) if file.endswith('.csv')])

    df1 = pd.read_csv(os.path.join(directory_path, csv_files[0]), on_bad_lines='skip')
    df2 = pd.read_csv(os.path.join(directory_path, csv_files[1]), on_bad_lines='skip')
    event_name = directory_path.split('/')[-1]

    df1.to_sql(f"{event_name}_user", con=engine, if_exists='replace', index=False)
    df2.to_sql(f"{event_name}_meta", con=engine, if_exists='replace', index=False)

def init_db():
    result = []
    parent_directory = settings.DATA_DIR
    subdirectories = [os.path.join(parent_directory, d)
        for d in os.listdir(parent_directory)
        if os.path.isdir(os.path.join(parent_directory, d))]
    for subdir in subdirectories:
        process_directory(subdir)
#     print(result)  # Print the result after appending
#     combined = pd.concat(result, ignore_index=True)
    return {"status": "success"}

# Export functions
__all__ = ["init_db"]
