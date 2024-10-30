from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, MetaData, Table, Boolean, insert, select, delete
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import Depends
from typing import List
from config import Settings
import pandas as pd

Base = declarative_base()

# Create URL object
database_url = sa.engine.URL(Settings.DB_DRIVER, Settings.DB_USER, Settings.DB_PASSWORD,
    Settings.DB_HOST, Settings.DB_PORT, Settings.DB_CAMPAIGN_NAME, {})

# Create engine and session
engine = create_engine(database_url)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

from sqlalchemy import MetaData, Table, Column, Integer, String, Boolean, insert
from sqlalchemy.engine import Engine

def initialize_database_schema():
    metadata = MetaData()

    conn = engine.connect()

    # Create table
    Student_meta = Table('Student_meta', metadata,
                    Column('Id', Integer(), primary_key=True),
                    Column('Name', String(255), nullable=False),
                    Column('Major', String(255), default="Math"),
                    Column('Pass', Boolean(), default=True)
                    )

    # Create table
    Student_record = Table('Student_record', metadata,
                    Column('Id', Integer(), primary_key=True),
                    Column('Name', String(255), nullable=False),
                    Column('Major', String(255), default="Math"),
                    Column('Pass', Boolean(), default=True)
                    )

#     delete_stmt = delete(Student)
#     result = conn.execute(delete_stmt)
#     print(f"Cleared {result.rowcount} rows from the Student table")

    metadata.create_all(engine)
    print("Table created")

#     # Insert rows
#     query = insert(Student)
#     values_list = [{'Id': 1, 'Name': 'Nisha', 'Major': "Science", 'Pass': False},
#                    {'Id': 2, 'Name': 'Natasha', 'Major': "Math", 'Pass': True},
#                    {'Id': 3, 'Name': 'Ben', 'Major': "English", 'Pass': False}]
#
#     result = conn.execute(query, values_list)
#     print(f"Inserted {result.rowcount} rows")
#     conn.commit()


#     # Query the table
#     output = conn.execute(Student.select()).fetchall()
#     print("Query result:")
#     for row in output:
#         print(row)

    conn.close()
    print("Connection closed")

# Initialize database schema
initialize_database_schema()

# # Create new table
# create_new_table()

# # Add new column to existing table
# add_new_column_to_existing_table()