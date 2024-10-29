from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, MetaData, Table, Boolean, insert, select, delete
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
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

def add_table(table_name: str, columns: List[tuple]):
    """
    Dynamically add a new table with given columns.

    :param table_name: Name of the table to create
    :param columns: List of column definitions (name, type)
    """
    Base.metadata.tables[table_name] = Table(
        table_name,
        Base.metadata,
        *[
            Column(col[0], eval(f"sa.{col[1]}")) for col in columns
        ]
    )

def add_column(table_name: str, column_name: str, column_type):
    """
    Dynamically add a new column to an existing table.

    :param table_name: Name of the table to modify
    :param column_name: Name of the new column
    :param column_type: SQLAlchemy type for the new column
    """
    Base.metadata.tables[table_name].append_column(
        Column(column_name, eval(f"sa.{column_type}"))
    )

# Example usage
def create_new_table():
    # Define columns for the new table
    new_table_columns = [
        ("id", Integer),
        ("name", String),
        ("age", Integer)
    ]

    # Create the new table
    add_table("new_users", new_table_columns)

def add_new_column_to_existing_table():
    # Add a new column to the existing 'users' table
    add_column("users", "created_at", "DateTime")

# Initialize database schema
initialize_database_schema()

# # Create new table
# create_new_table()

# # Add new column to existing table
# add_new_column_to_existing_table()