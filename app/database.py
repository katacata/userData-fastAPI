from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, MetaData, Table
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import Depends
from typing import List
from config import Settings

Base = declarative_base()

# Create URL object
database_url = sa.engine.URL(Settings.DB_DRIVER, Settings.DB_USER, Settings.DB_PASSWORD,
    Settings.DB_HOST, Settings.DB_PORT, Settings.DB_NAME, {})

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

    # Define models
    class User(Base):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True, index=True)
        username = Column(String, unique=True, index=True)
        email = Column(String, unique=True, index=True)
        hashed_password = Column(String)

    class Post(Base):
        __tablename__ = 'posts'
        id = Column(Integer, primary_key=True, index=True)
        title = Column(String, index=True)
        content = Column(String)
        author_id = Column(Integer, ForeignKey('users.id'))

    # Create tables if they don't exist
    metadata.create_all(engine)
    print("initialize_database_schema finished")

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
create_new_table()

# # Add new column to existing table
# add_new_column_to_existing_table()
