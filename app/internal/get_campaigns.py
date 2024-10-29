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

class CamMember(BaseModel):
    name: Optional[str]
    mobile: Optional[str]
    email: Optional[str]
    birth: Optional[str]
    age: Optional[str]
    byear: Optional[int]
    bmonth: Optional[int]
    gender: Optional[str]
    work: Optional[str]
    gender: Optional[str]
    marriage: Optional[str]
    title: Optional[str]


def get_db_by_table_name(table_name):
    metadata = MetaData()
    conn = engine.connect()

    # Reflect the table schema
    metadata.reflect(bind=engine)

    # Get the reflected table
    record_table = metadata.tables[table_name]

    # Select all columns from the users table
    stmt = select(record_table)

    # Execute the query
    result = conn.execute(stmt)
    conn.close()
    data = pd.DataFrame(result)

    return data


def process_directory(table_names):
    df1 = get_db_by_table_name(table_names[0])
    print("df1: \n" + df1.to_string(index=False))
    if(len(table_names) != 2): return None
    df2 = get_db_by_table_name(table_names[1])

    return None

#
#     df2 = df2.drop_duplicates(subset=['record_id', 'meta_key'], keep='first')
#     df2 = df2.pivot(index='record_id', columns='meta_key', values='meta_value')
#     df2.reset_index(inplace=True)
#     df2 = df2.rename(columns={'record_id': 'id'})
#
#     df1['id'] = df1['id'].astype(str)
#     df2['id'] = df2['id'].astype(str)
#
#     merged_df = pd.merge(df1, df2, on='id', how='outer')
#
#     required_columns = ['name', 'mobile', 'email', 'birth', 'age', 'byear', 'bmonth', 'work', 'gender', 'marriage', 'title']
#     target_columns = []
#     for column in required_columns:
#         if column in merged_df:
#             target_columns.append(column)
#     final_df = merged_df[target_columns]
#     final_df['event'] = event_name
#
#     return final_df

def getSheMember():
    she_member = pd.read_csv(settings.DATA_DIR+'SheMember.csv',  on_bad_lines='skip')
    she_member.rename(columns={'birth_month': 'bmonth', 'birth_year': 'byear', 'phone': 'mobile'}, inplace=True)
    she_member['name'] = she_member['first_name'] + ' ' + she_member['last_name']
    return she_member[['name', 'mobile', 'email', 'byear', 'bmonth', 'gender']]

def get_table_prefixes(engine):
    # Query to get all table names
#     query = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
#     result = db_connection.execute(query).fetchall()
    inspector = inspect(engine)
    result = inspector.get_table_names()

    print(result)

    # Extract prefixes and group tables
    prefix_groups = defaultdict(list)
    for table_name in result:
#         prefix = os.path.splitext(table_name)[0] if '_' in table_name else table_name
        prefix = table_name.split('_')[0] if '_' in table_name else table_name
        prefix_groups[prefix].append(table_name)
    return prefix_groups


def combine_directories():
    # Establish database connection
    metadata = MetaData()

    # Get table prefixes and groups
    prefix_groups = get_table_prefixes(engine)

    # Process each group of tables
    for prefix, table_names in prefix_groups.items():
        result = []
        print("Prefix: " +  prefix)
        print("Table: " + str(table_names))
        result.append(process_directory(sorted(table_names)))
        # Do something with the processed results for this group
        print(f"Processed {len(result)} tables starting with '{prefix}'")
    return None

#     combined = pd.concat(result, ignore_index=True)
#
#
#     combined['age'] = combined['age'].str.replace(r'[^\d\-,]', '', regex=True)
#     def calculate_midpoint(value, offset = 2):
#         match = re.match(r'^(\d+)-(\d+)$', str(value))
#         if match:
#             start, end = map(int, match.groups())
#             return  datetime.now().year - ((start + end) // 2 + offset)
#         match = re.match(r'^(\d+)$', str(value))
#         if match:
#             return  datetime.now().year - int(value)
#         return value
#     combined['age'] = combined['age'].apply(calculate_midpoint)
#
#     def map_title(title):
#         title_map = {
#             'MS': 'F',
#             '小姐': 'F',
#             '太太': 'F',
#             'MR': 'M',
#             '先生': 'M',
#             'MRS': 'F'
#         }
#         return title_map.get(title, title)
#     combined['title'] = combined['title'].map(map_title)
#
#
# #     combined.to_csv('campaign_data.csv', index=False)
# #     she_member = getSheMember()
# #     combined = pd.concat([combined, she_member], ignore_index=True)
#
#     # Print column names for debugging
#     print("Column names:")
#     print(combined.columns.tolist())
#
#     members = []
#     for _, row in combined.head(100)[['name', 'mobile', 'email', 'birth', 'age', 'byear', 'bmonth', 'work', 'gender', 'marriage', 'title']].iterrows():
#         # Print name value for debugging
#         print(f"Name value: {row['name']}")
#
#         # Handle NaN values
#         name = str(row['name']) if pd.notna(row['name']) else None
#         gender = str(row['gender']) if pd.notna(row['gender']) else None
#         mobile = str(row['mobile']) if pd.notna(row['mobile']) else None
#         email = str(row['email']) if pd.notna(row['email']) else None
#         work = str(row['work']) if pd.notna(row['work']) else None
#         gender = str(row['gender']) if pd.notna(row['gender']) else None
#         marriage = str(row['marriage']) if pd.notna(row['marriage']) else None
#         title = str(row['title']) if pd.notna(row['title']) else None
#         birth = str(row['birth']) if pd.notna(row['birth']) else None
#         age = str(row['age']) if pd.notna(row['age']) else None
#
#         # Convert byear and bmonth to integers, handling NaN values
#         byear = int(row['byear']) if pd.notna(row['byear']) else None
#         bmonth = int(row['bmonth']) if pd.notna(row['bmonth']) else None
#
#
#         try:
#             member = CamMember(
#                 name = name,
#                 gender = gender,
#                 mobile = mobile,
#                 email = email,
#                 work = work,
#                 marriage = marriage,
#                 title = title,
#                 byear = byear,
#                 bmonth = bmonth,
#                 birth = birth,
#                 age = age,
#             )
#             members.append(member)
#         except ValidationError as e:
#             print(f"Validation error for row: {row}")
#             print(f"Error details: {e}")
#     return members

# Export functions
__all__ = ["process_directory", "getSheMember","combine_directories"]
