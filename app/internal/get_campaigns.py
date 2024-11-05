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

# Create URL object
result_url = sa.engine.URL(Settings.DB_DRIVER, Settings.DB_USER, Settings.DB_PASSWORD,
    Settings.DB_HOST, Settings.DB_PORT, Settings.DB_RESULT_NAME, {})

# Create engine and session
result_engine = create_engine(result_url)

result_SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=result_engine)

class CamMember(BaseModel):
    name: Optional[str]
    mobile: Optional[str]
    email: Optional[str]
#     birth: Optional[str]
#     age: Optional[str]
    birth_year: Optional[str]
    bmonth: Optional[str]
    work: Optional[str]
    event: Optional[str]
    gender_combined: Optional[str]
    marriage: Optional[str]
#     title: Optional[str]


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


def process_directory(table_names, event_name):
    if(len(table_names) != 2): return None
    df1 = get_db_by_table_name(table_names[0])
#     print("df1: \n" + df1.to_string(index=False))
    df2 = get_db_by_table_name(table_names[1])

    df2 = df2.drop_duplicates(subset=['record_id', 'meta_key'], keep='first')
    df2 = df2.pivot(index='record_id', columns='meta_key', values='meta_value')
    df2.reset_index(inplace=True)
    df2 = df2.rename(columns={'record_id': 'id'})

    df1['id'] = df1['id'].astype(str)
    df2['id'] = df2['id'].astype(str)

    merged_df = pd.merge(df1, df2, on='id', how='outer')

    required_columns = ['name', 'mobile', 'email', 'birth', 'age', 'byear', 'bmonth', 'work', 'gender', 'marriage', 'title']
    target_columns = []
    for column in required_columns:
        if column in merged_df:
            target_columns.append(column)
    final_df = merged_df[target_columns]
    final_df['event'] = event_name

    return final_df

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

def transform_data(df):

    # Group by email and aggregate values
    transformed_df = df.groupby('email').agg({
        'name': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique()),
        'mobile': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique()),
        'birth': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique()),
        'age': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique()),
        'byear': lambda x: ', '.join(transform_byear(str(val)) for val in x[x.notnull()].unique()),
        'bmonth': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique()),
        'work': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique()),
        'gender': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique()),
        'marriage': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique()),
        'title': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique()),
        'event': lambda x: ', '.join(str(val) for val in x[x.notnull()].unique())
    }).reset_index()

    target_cols = ['birth', 'age', 'byear']
    def concatenate_strings(series):
        s = ', '.join(map(str, series))
        parts = s.split(', ')
        parts = [part.strip() for part in parts if part]
        return ', '.join(parts)

    # Concatenate the target columns
    transformed_df['birth_year'] = transformed_df[target_cols].apply(concatenate_strings, axis=1)


    target_cols = ['gender', 'title']
    transformed_df['gender_combined'] = transformed_df[target_cols].apply(concatenate_strings, axis=1)

    # Drop the original columns
    transformed_df = transformed_df.drop(target_cols, axis=1)

    def extract_year(value):
        if isinstance(value, str):
            if '-' in value:
                return value.split('-')[0]
            elif ',' in value:
                return ','.join(part.split('-')[0] for part in value.split(',') if part)
        elif isinstance(value, datetime):
            return str(value.year)
        return value

    transformed_df['birth_year'] = transformed_df['birth_year'].apply(extract_year)

    # Remove trailing commas
    for col in transformed_df.columns:
        if col != 'email':
            transformed_df[col] = transformed_df[col].str.rstrip(',')

    def clean_field(value):
        if pd.isna(value):
            return None

        # Split the value into a list
        values = str(value).split(',')

        # Clean each value and join them back
        cleaned_values = [re.sub(r'\.0+$', '', v.strip()) for v in values if v.strip()]

        # Return the joined cleaned values or None if empty
        return ', '.join(cleaned_values) if cleaned_values else None


    def mobile_trim(value):
        if(value):
            return value[-8:] if len(value) > 8 else value
        return value

    transformed_df['mobile'] = transformed_df['mobile'].apply(clean_field).apply(mobile_trim)
    transformed_df['birth_year'] = transformed_df['birth_year'].apply(clean_field)

    print(transformed_df.to_string)
    return transformed_df

def transform_byear(year):
    bYearList = [
        "1970", "1971", "1972", "1973", "1974", "1975",
        "1976", "1977", "1978", "1979", "1980", "1981",
        "1982", "1983", "1984", "1985", "1986", "1987",
        "1988", "1989", "1990", "1991", "1992", "1993",
        "1994", "1995", "1996", "1997", "1998", "1999",
        "2000", "2001", "2002", "2003", "2004", "2005",
        "2006", "2007", "2008", "2009", "2010", "2011",
        "2012", "2013", "2014", "2015"
    ]

    try:
        year_int = int(year)
        if year_int < len(bYearList):
            return bYearList[year_int]
        else:
            return year
    except ValueError:
        return year

def create_result_table(members):
    metadata = MetaData()

    conn = result_engine.connect()

    inspector = sa.inspect(result_engine)
    if inspector.has_table('result'):
        print("Table 'result' already exists. Dropping...")
        conn.execute(sa.text(f"DROP TABLE IF EXISTS result"))
        print("Old table dropped.")

    # Create table
    result = Table('result', metadata,
                    Column('email',  String(255),  default=''),
                    Column('name', String(255), default=''),
                    Column('mobile', String(255), default=''),
                    Column('birth_year', String(255), default=''),
                    Column('bmonth', String(255), default=''),
                    Column('work', String(255), default=''),
                    Column('gender_combined', String(255), default=''),
                    Column('marriage', String(255), default=''),
                    Column('event', String(255), default=''),
                    )

#     print(str(member_dicts))
    metadata.create_all(result_engine)
    print("Table created")

    member_dicts = []
    for member in members:
        member_dict = {
            'email': member.email,
            'name': member.name,
            'mobile': member.mobile,
            'birth_year': member.birth_year,
            'bmonth': member.bmonth,
            'work': member.work,
            'gender_combined': member.gender_combined,
            'marriage': member.marriage,
            'event': member.event,
        }
        member_dicts.append(member_dict)

    # Insert rows
    query = insert(result)
    execution = conn.execute(query, member_dicts)
    conn.commit()

    conn.close()


def combine_directories():
    # Establish database connection
    metadata = MetaData()

    # Get table prefixes and groups
    prefix_groups = get_table_prefixes(engine)

    result = []
    # Process each group of tables
    for prefix, table_names in prefix_groups.items():
        print("Prefix: " +  prefix)
        print("Table: " + str(table_names))
        result.append(process_directory(sorted(table_names, reverse=True), prefix))
        # Do something with the processed results for this group
        print(f"Processed tables starting with '{prefix}'")

    print("Length: " + str(len(result)))
    combined = pd.concat(result, ignore_index=True)
    print(combined.head(100).to_string(index=False))

#     return None


    combined['age'] = combined['age'].str.replace(r'[^\d\-,]', '', regex=True)
    def calculate_midpoint(value, offset = 2):
        match = re.match(r'^(\d+)-(\d+)$', str(value))
        if match:
            start, end = map(int, match.groups())
            return  datetime.now().year - ((start + end) // 2 + offset)
        match = re.match(r'^(\d+)$', str(value))
        if match:
            return  datetime.now().year - int(value)
        return value
    combined['age'] = combined['age'].apply(calculate_midpoint)

    def map_title(title):
        title_map = {
            'MS': 'F',
            '小姐': 'F',
            '太太': 'F',
            'MR': 'M',
            '先生': 'M',
            'MRS': 'F'
        }
        return title_map.get(title, title)
    combined['title'] = combined['title'].map(map_title)


#     combined.to_csv('campaign_data.csv', index=False)
#     she_member = getSheMember()
#     combined = pd.concat([combined, she_member], ignore_index=True)

    # Print column names for debugging
    print("Column names:")
    print(combined.columns.tolist())

    combined = transform_data(combined)
    combined.to_csv("output.csv", index=False)

    members = []
    for _, row in combined[['name', 'mobile', 'email', 'birth_year', 'bmonth', 'work', 'gender_combined', 'marriage', 'event']].iterrows():
        # Print name value for debugging
#         print(f"Name value: {row['name']}")

        # Handle NaN values
        name = str(row['name']) if pd.notna(row['name']) else None
        mobile = str(row['mobile']) if pd.notna(row['mobile']) else None
        email = str(row['email']) if pd.notna(row['email']) else None
        work = str(row['work']) if pd.notna(row['work']) else None
        gender_combined = str(row['gender_combined']) if pd.notna(row['gender_combined']) else None
        marriage = str(row['marriage']) if pd.notna(row['marriage']) else None
#         title = str(row['title']) if pd.notna(row['title']) else None
#         birth = str(row['birth']) if pd.notna(row['birth']) else None
#         age = str(row['age']) if pd.notna(row['age']) else None
        birth_year = str(row['birth_year']) if pd.notna(row['birth_year']) else None
        bmonth = str(row['bmonth']) if pd.notna(row['bmonth']) else None
        event = str(row['event']) if pd.notna(row['event']) else None


        try:
            member = CamMember(
                name = name,
                gender_combined = gender_combined,
                mobile = mobile,
                email = email,
                work = work,
                marriage = marriage,
#                 title = title,
                birth_year = birth_year,
                bmonth = bmonth,
                event = event,
            )
            members.append(member)
        except ValidationError as e:
            print(f"Validation error for row: {row}")
            print(f"Error details: {e}")
    create_result_table(members)
    return members

# Export functions
__all__ = ["process_directory", "getSheMember","combine_directories"]
